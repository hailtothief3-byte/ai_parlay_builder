from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from random import Random

from sqlalchemy import delete

from db.models import Event, MarketLine, PropProjection
from db.session import SessionLocal
from sports_config import get_sport_config, get_sport_labels


SPORTPLAYERS = {
    "NBA": ["Jalen Hart", "Marcus Vale", "Tyrese North", "Devin Stone", "Luka Frost", "Jaylen Cole"],
    "MLB": ["Aaron Pike", "Mason Reed", "Diego Lane", "Nolan Fox", "Corey Banks", "Evan West"],
    "NFL": ["C.J. Rivers", "Jayden Knox", "Malik Ford", "Aiden Price", "Trevor Hale", "Roman Bell"],
    "CS2": ["NiKo Prime", "Frozen Ace", "Ropz Echo", "M0nesy Beam", "Xantares Bolt", "Twistzz Nova"],
    "LoL": ["Faker Pulse", "Knight Drift", "Chovy Flux", "Ruler Spark", "Caps Orbit", "Guma Wave"],
    "DOTA2": ["Miracle Arc", "Yatoro Rift", "Nisha Core", "Topson Rune", "Ame Surge", "Quinn Ember"],
}

MARKET_BASELINES = {
    "player_points": (23.5, 3.2),
    "player_rebounds": (7.5, 1.6),
    "player_assists": (6.5, 1.5),
    "player_points_rebounds_assists": (34.5, 4.2),
    "player_threes": (2.5, 0.8),
    "player_first_basket": (0.5, 0.2),
    "player_home_runs": (0.5, 0.2),
    "player_hits": (1.5, 0.4),
    "player_total_bases": (1.5, 0.5),
    "player_strikeouts": (6.5, 1.5),
    "player_pass_yds": (252.5, 12.0),
    "player_pass_tds": (1.5, 0.4),
    "player_rush_yds": (68.5, 6.0),
    "player_reception_yds": (62.5, 5.0),
    "player_receptions": (5.5, 0.8),
    "player_kills": (17.5, 2.5),
    "player_headshots": (9.5, 1.8),
    "player_kills_assists": (24.5, 3.2),
    "player_fantasy_score": (41.5, 5.0),
}

SPORTSBOOKS = [
    ("demo_draftkings", "DraftKings Demo", False),
    ("demo_fanduel", "FanDuel Demo", False),
    ("demo_caesars", "Caesars Demo", False),
]

DFS_BOOKS = [
    ("prizepicks", "PrizePicks Demo", True),
    ("underdog", "Underdog Demo", True),
]


def _demo_prefix(sport_label: str) -> str:
    return f"demo_{sport_label.lower().replace(' ', '_')}"


def _build_event_names(sport_label: str) -> list[tuple[str, str]]:
    return [
        (f"{sport_label} Alpha", f"{sport_label} Omega"),
        (f"{sport_label} Titan", f"{sport_label} Nova"),
    ]


def _insert_event(db, sport_key: str, event_id: str, home_team: str, away_team: str, commence_time: datetime) -> None:
    existing = db.query(Event).filter(Event.external_event_id == event_id).one_or_none()
    if existing:
        existing.sport_key = sport_key
        existing.commence_time = commence_time
        existing.home_team = home_team
        existing.away_team = away_team
    else:
        db.add(
            Event(
                external_event_id=event_id,
                sport_key=sport_key,
                commence_time=commence_time,
                home_team=home_team,
                away_team=away_team,
            )
        )


def _demo_player_team(player_idx: int, home_team: str, away_team: str) -> str:
    return away_team if player_idx % 2 else home_team


def clear_demo_live_data(sport_label: str) -> int:
    sport_key = get_sport_config(sport_label)["live_keys"][0]
    prefix = f"{_demo_prefix(sport_label)}_%"

    with SessionLocal() as db:
        deleted_lines = db.execute(
            delete(MarketLine).where(MarketLine.external_event_id.like(prefix))
        ).rowcount or 0
        deleted_projections = db.execute(
            delete(PropProjection).where(PropProjection.external_event_id.like(prefix))
        ).rowcount or 0
        deleted_events = db.execute(
            delete(Event).where(Event.external_event_id.like(prefix))
        ).rowcount or 0
        db.commit()

    return deleted_lines + deleted_projections + deleted_events


def seed_demo_live_data(sport_label: str) -> dict[str, int]:
    sport_config = get_sport_config(sport_label)
    sport_key = sport_config["live_keys"][0]
    prop_markets = [
        market
        for market in sport_config["prop_markets"]
        if market in MARKET_BASELINES
    ]
    dfs_markets = [
        market
        for market in sport_config["dfs_markets"]
        if market in MARKET_BASELINES
    ]

    seed = sum(ord(char) for char in f"{sport_label}:{sport_key}")
    rng = Random(seed)
    now = datetime.now(timezone.utc)
    prefix = _demo_prefix(sport_label)

    clear_demo_live_data(sport_label)

    event_count = 0
    line_count = 0
    projection_count = 0

    with SessionLocal() as db:
        players = SPORTPLAYERS[sport_label]
        team_pairs = _build_event_names(sport_label)

        for event_idx, (away_team, home_team) in enumerate(team_pairs, start=1):
            event_id = f"{prefix}_event_{event_idx}"
            commence_time = now + timedelta(hours=event_idx * 3)
            _insert_event(db, sport_key, event_id, home_team, away_team, commence_time)
            event_count += 1

            event_players = players[(event_idx - 1) * 3 : event_idx * 3]
            for player_idx, player_name in enumerate(event_players, start=1):
                player_team = _demo_player_team(player_idx, home_team, away_team)
                for market_idx, market_key in enumerate(prop_markets):
                    base_line, std_dev = MARKET_BASELINES[market_key]
                    line_anchor = round(base_line + rng.uniform(-1.8, 1.8), 1)
                    projection = round(line_anchor + rng.uniform(-1.4, 1.4), 2)
                    over_prob = round(min(max(0.5 + ((projection - line_anchor) * 0.06), 0.34), 0.72), 3)
                    under_prob = round(1.0 - over_prob, 3)
                    confidence = round(60 + rng.uniform(6, 24), 1)

                    for snap_idx in range(3):
                        pulled_at = now - timedelta(hours=6 - snap_idx * 2) + timedelta(minutes=event_idx * 7 + player_idx)
                        last_update = pulled_at + timedelta(minutes=5)

                        for book_idx, (book_key, book_title, is_dfs) in enumerate(SPORTSBOOKS):
                            line_value = round(line_anchor + ((book_idx - 1) * 0.5) + (snap_idx * 0.2), 1)
                            over_price = int(-115 + (book_idx * 6) + (snap_idx * 4))
                            under_price = int(-105 - (book_idx * 5) - (snap_idx * 3))

                            db.add(
                                MarketLine(
                                    external_event_id=event_id,
                                    sport_key=sport_key,
                                    bookmaker_key=book_key,
                                    bookmaker_title=book_title,
                                    market_key=market_key,
                                    player_name=player_name,
                                    outcome_name="Over",
                                    line=line_value,
                                    price=over_price,
                                    side="over",
                                    is_dfs=is_dfs,
                                    event_commence_time=commence_time,
                                    last_update=last_update,
                                    pulled_at=pulled_at,
                                    raw_json=json.dumps(
                                        {
                                            "source": "demo_seed",
                                            "sport": sport_label,
                                            "market": market_key,
                                            "side": "over",
                                            "player_team": player_team,
                                        }
                                    ),
                                )
                            )
                            db.add(
                                MarketLine(
                                    external_event_id=event_id,
                                    sport_key=sport_key,
                                    bookmaker_key=book_key,
                                    bookmaker_title=book_title,
                                    market_key=market_key,
                                    player_name=player_name,
                                    outcome_name="Under",
                                    line=line_value,
                                    price=under_price,
                                    side="under",
                                    is_dfs=is_dfs,
                                    event_commence_time=commence_time,
                                    last_update=last_update,
                                    pulled_at=pulled_at,
                                    raw_json=json.dumps(
                                        {
                                            "source": "demo_seed",
                                            "sport": sport_label,
                                            "market": market_key,
                                            "side": "under",
                                            "player_team": player_team,
                                        }
                                    ),
                                )
                            )
                            line_count += 2

                    db.add(
                        PropProjection(
                            sport_key=sport_key,
                            external_event_id=event_id,
                            player_name=player_name,
                            market_key=market_key,
                            projection=projection,
                            std_dev=std_dev,
                            over_prob=over_prob,
                            under_prob=under_prob,
                            confidence=confidence,
                            model_name="demo_live_seed_v1",
                            created_at=now + timedelta(minutes=market_idx),
                        )
                    )
                    projection_count += 1

                for market_idx, market_key in enumerate(dfs_markets):
                    base_line, _ = MARKET_BASELINES[market_key]
                    line_anchor = round(base_line + rng.uniform(-1.2, 1.2), 1)
                    pulled_at = now - timedelta(minutes=30 - market_idx * 3)
                    last_update = pulled_at + timedelta(minutes=2)

                    for book_key, book_title, is_dfs in DFS_BOOKS:
                        db.add(
                            MarketLine(
                                external_event_id=event_id,
                                sport_key=sport_key,
                                bookmaker_key=book_key,
                                bookmaker_title=book_title,
                                market_key=market_key,
                                player_name=player_name,
                                outcome_name="Over",
                                line=line_anchor,
                                price=None,
                                side="over",
                                is_dfs=is_dfs,
                                event_commence_time=commence_time,
                                last_update=last_update,
                                pulled_at=pulled_at,
                                raw_json=json.dumps(
                                    {
                                        "source": "demo_seed",
                                        "sport": sport_label,
                                        "market": market_key,
                                        "side": "over",
                                        "player_team": player_team,
                                    }
                                ),
                            )
                        )
                        line_count += 1

        db.commit()

    return {
        "events": event_count,
        "lines": line_count,
        "projections": projection_count,
    }


def seed_all_demo_live_data() -> dict[str, dict[str, int]]:
    return {
        label: seed_demo_live_data(label)
        for label in get_sport_labels()
    }
