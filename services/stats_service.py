from __future__ import annotations

from datetime import datetime, timedelta, timezone
from io import BytesIO

import pandas as pd
from sqlalchemy import delete, select

from config import CONFIG
from db.models import PlayerStatSnapshot
from db.session import SessionLocal
from ingestion.balldontlie_api import BallDontLieClient, format_balldontlie_error, recent_date_strings
from ingestion.sportsgameodds_api import SportsGameOddsClient, format_sgo_error


REQUIRED_COLUMNS = ["sport_key", "player_name", "market_key"]
MARKET_STAT_FIELDS = {
    "basketball_nba": {
        "player_points": ["pts"],
        "player_rebounds": ["reb"],
        "player_assists": ["ast"],
        "player_threes": ["fg3m"],
    },
    "baseball_mlb": {
        "player_hits": ["hits", "h"],
        "player_home_runs": ["home_runs", "hr"],
        "player_total_bases": ["total_bases", "tb"],
        "player_strikeouts": ["strikeouts", "so", "pitching_strikeouts"],
    },
}

SPORTSGAMEODDS_LEAGUE_IDS = {
    "NBA": "NBA",
    "MLB": "MLB",
    "NFL": "NFL",
}

SPORTSGAMEODDS_STAT_ODD_IDS = {
    "NBA": [
        "points-PLAYER_ID-game-ou-over",
        "points-PLAYER_ID-game-ou-under",
        "rebounds-PLAYER_ID-game-ou-over",
        "rebounds-PLAYER_ID-game-ou-under",
        "assists-PLAYER_ID-game-ou-over",
        "assists-PLAYER_ID-game-ou-under",
        "threes_made-PLAYER_ID-game-ou-over",
        "threes_made-PLAYER_ID-game-ou-under",
    ],
    "MLB": [
        "batting_hits-PLAYER_ID-game-ou-over",
        "batting_hits-PLAYER_ID-game-ou-under",
        "total_bases-PLAYER_ID-game-ou-over",
        "total_bases-PLAYER_ID-game-ou-under",
        "home_runs-PLAYER_ID-game-ou-over",
        "home_runs-PLAYER_ID-game-ou-under",
        "pitcher_strikeouts-PLAYER_ID-game-ou-over",
        "pitcher_strikeouts-PLAYER_ID-game-ou-under",
    ],
    "NFL": [
        "passing_yards-PLAYER_ID-game-ou-over",
        "passing_yards-PLAYER_ID-game-ou-under",
        "rushing_yards-PLAYER_ID-game-ou-over",
        "rushing_yards-PLAYER_ID-game-ou-under",
        "receiving_yards-PLAYER_ID-game-ou-over",
        "receiving_yards-PLAYER_ID-game-ou-under",
        "receptions-PLAYER_ID-game-ou-over",
        "receptions-PLAYER_ID-game-ou-under",
    ],
}


def import_stats_csv(file_bytes: bytes, default_source: str = "csv_upload") -> dict[str, int]:
    df = pd.read_csv(BytesIO(file_bytes))
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(
            "Stats CSV is missing required columns: " + ", ".join(missing)
        )

    imported = 0
    created_at = datetime.now(timezone.utc)

    with SessionLocal() as db:
        for _, row in df.iterrows():
            sport_key = str(row.get("sport_key") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            market_key = str(row.get("market_key") or "").strip()
            source = str(row.get("source") or default_source).strip()

            if not sport_key or not player_name or not market_key:
                continue

            db.execute(
                delete(PlayerStatSnapshot).where(
                    PlayerStatSnapshot.sport_key == sport_key,
                    PlayerStatSnapshot.player_name == player_name,
                    PlayerStatSnapshot.market_key == market_key,
                    PlayerStatSnapshot.source == source,
                )
            )

            db.add(
                PlayerStatSnapshot(
                    sport_key=sport_key,
                    player_name=player_name,
                    market_key=market_key,
                    season_average=float(row["season_average"]) if pd.notnull(row.get("season_average")) else None,
                    recent_average=float(row["recent_average"]) if pd.notnull(row.get("recent_average")) else None,
                    last_5_average=float(row["last_5_average"]) if pd.notnull(row.get("last_5_average")) else None,
                    trend=float(row["trend"]) if pd.notnull(row.get("trend")) else None,
                    sample_size=int(row["sample_size"]) if pd.notnull(row.get("sample_size")) else None,
                    source=source,
                    created_at=created_at,
                )
            )
            imported += 1

        db.commit()

    return {"rows_imported": imported}


def get_latest_stats_snapshots(sport_key: str | list[str]) -> pd.DataFrame:
    sport_keys = [sport_key] if isinstance(sport_key, str) else [key for key in sport_key if key]
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        rows = db.execute(
            select(PlayerStatSnapshot).where(PlayerStatSnapshot.sport_key.in_(sport_keys))
        ).scalars().all()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "sport_key": row.sport_key,
                "player": row.player_name,
                "market": row.market_key,
                "season_average": row.season_average,
                "recent_average": row.recent_average,
                "last_5_average": row.last_5_average,
                "trend": row.trend,
                "sample_size": row.sample_size,
                "source": row.source,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    )

    return df.sort_values("created_at").drop_duplicates(
        subset=["sport_key", "player", "market", "source"],
        keep="last",
    )


def build_stats_template() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "sport_key": "basketball_nba",
                "player_name": "Donovan Mitchell",
                "market_key": "player_points",
                "season_average": 26.8,
                "recent_average": 28.4,
                "last_5_average": 29.1,
                "trend": 1.2,
                "sample_size": 10,
                "source": "manual_csv",
            },
            {
                "sport_key": "baseball_mlb",
                "player_name": "Aaron Judge",
                "market_key": "player_home_runs",
                "season_average": 0.42,
                "recent_average": 0.55,
                "last_5_average": 0.60,
                "trend": 0.08,
                "sample_size": 10,
                "source": "manual_csv",
            },
        ]
    )


def upsert_stats_rows(rows: list[dict], default_source: str = "api_sync") -> dict[str, int]:
    imported = 0
    created_at = datetime.now(timezone.utc)

    with SessionLocal() as db:
        for row in rows:
            sport_key = str(row.get("sport_key") or "").strip()
            player_name = str(row.get("player_name") or "").strip()
            market_key = str(row.get("market_key") or "").strip()
            source = str(row.get("source") or default_source).strip()

            if not sport_key or not player_name or not market_key:
                continue

            db.execute(
                delete(PlayerStatSnapshot).where(
                    PlayerStatSnapshot.sport_key == sport_key,
                    PlayerStatSnapshot.player_name == player_name,
                    PlayerStatSnapshot.market_key == market_key,
                    PlayerStatSnapshot.source == source,
                )
            )

            db.add(
                PlayerStatSnapshot(
                    sport_key=sport_key,
                    player_name=player_name,
                    market_key=market_key,
                    season_average=row.get("season_average"),
                    recent_average=row.get("recent_average"),
                    last_5_average=row.get("last_5_average"),
                    trend=row.get("trend"),
                    sample_size=row.get("sample_size"),
                    source=source,
                    created_at=created_at,
                )
            )
            imported += 1

        db.commit()

    return {"rows_imported": imported}


def _extract_player_name(stat: dict) -> str:
    player = stat.get("player", {}) if isinstance(stat.get("player"), dict) else {}
    full_name = " ".join(
        part for part in [player.get("first_name"), player.get("last_name")] if part
    ).strip()
    return full_name or str(player.get("name") or "").strip()


def _extract_stat_value(stat: dict, candidates: list[str]) -> float | None:
    for field in candidates:
        value = stat.get(field)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _safe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sportsgameodds_player_name(event_payload: dict, odd: dict) -> str:
    player_id = odd.get("playerID") or odd.get("statEntityID")
    players = event_payload.get("players", {}) if isinstance(event_payload.get("players"), dict) else {}
    player = players.get(player_id, {}) if isinstance(players.get(player_id), dict) else {}
    if player.get("name"):
        return str(player["name"]).strip()
    return str(
        odd.get("playerName")
        or odd.get("name")
        or odd.get("statEntityName")
        or odd.get("participantName")
        or ""
    ).strip()


def _sportsgameodds_market_key(odd: dict) -> str | None:
    odd_id = str(odd.get("oddID") or "").strip()
    stat_id = odd_id.split("-")[0] if odd_id else str(odd.get("statID") or "").strip()
    stat_id = stat_id.lower()
    market_map = {
        "points": "player_points",
        "rebounds": "player_rebounds",
        "assists": "player_assists",
        "threes_made": "player_threes",
        "batting_hits": "player_hits",
        "home_runs": "player_home_runs",
        "total_bases": "player_total_bases",
        "pitcher_strikeouts": "player_strikeouts",
        "passing_yards": "player_pass_yds",
        "rushing_yards": "player_rush_yds",
        "receiving_yards": "player_reception_yds",
        "receptions": "player_receptions",
    }
    return market_map.get(stat_id)


def sync_stats_from_sportsgameodds(
    sport_label: str,
    sport_key: str,
    player_names: list[str],
    days: int = 14,
) -> dict[str, int]:
    league_id = SPORTSGAMEODDS_LEAGUE_IDS.get(sport_label)
    odd_ids = SPORTSGAMEODDS_STAT_ODD_IDS.get(sport_label, [])
    if not league_id or not odd_ids:
        raise ValueError(f"SportsGameOdds stats sync is not configured for {sport_label}.")

    normalized_targets = {name.strip().lower() for name in player_names if name and name.strip()}
    if not normalized_targets:
        return {"rows_imported": 0, "events_fetched": 0, "stat_records_built": 0}

    client = SportsGameOddsClient()

    try:
        events: list[dict] = []
        seen_event_ids: set[str] = set()
        today = datetime.now(timezone.utc).date()
        max_pages_per_day = 3

        for offset in range(days):
            day = today - timedelta(days=offset)
            day_start = day.strftime("%Y-%m-%d")
            day_end = (day + timedelta(days=1)).strftime("%Y-%m-%d")
            cursor: str | None = None
            pages_fetched = 0

            while True:
                params = {
                    "leagueID": league_id,
                    "finalized": "true",
                    "startsAfter": day_start,
                    "startsBefore": day_end,
                    "limit": 25,
                    "oddIDs": ",".join(odd_ids),
                }
                if cursor:
                    params["cursor"] = cursor

                payload = client._get("/events", params=params)
                batch = payload.get("data", []) if isinstance(payload, dict) else []
                if not isinstance(batch, list) or not batch:
                    break

                for event in batch:
                    if not isinstance(event, dict):
                        continue
                    event_id = str(event.get("eventID") or event.get("id") or "").strip()
                    if not event_id or event_id in seen_event_ids:
                        continue
                    seen_event_ids.add(event_id)
                    events.append(event)

                cursor = payload.get("nextCursor") if isinstance(payload, dict) else None
                pages_fetched += 1
                if not cursor or pages_fetched >= max_pages_per_day:
                    break
    except Exception as exc:
        raise ValueError(f"SportsGameOdds stats sync failed: {format_sgo_error(exc)}") from exc

    def event_sort_key(event_payload: dict) -> str:
        status = event_payload.get("status", {}) if isinstance(event_payload.get("status"), dict) else {}
        return str(status.get("startsAt") or event_payload.get("startsAt") or event_payload.get("startTime") or "")

    events = sorted(events, key=event_sort_key, reverse=True)
    player_market_values: dict[tuple[str, str], list[float]] = {}

    for event_payload in events:
        odds = event_payload.get("odds", {}) if isinstance(event_payload.get("odds"), dict) else {}
        seen_pairs: set[tuple[str, str, str]] = set()
        for odd in odds.values():
            if not isinstance(odd, dict):
                continue

            player_name = _sportsgameodds_player_name(event_payload, odd)
            market_key = _sportsgameodds_market_key(odd)
            actual_score = _safe_float(odd.get("score"))
            odd_id = str(odd.get("oddID") or "")
            pair_key = (str(event_payload.get("eventID") or ""), player_name, market_key or "")

            if not player_name or player_name.lower() not in normalized_targets:
                continue
            if not market_key or actual_score is None:
                continue
            if pair_key in seen_pairs:
                continue

            seen_pairs.add(pair_key)
            player_market_values.setdefault((player_name, market_key), []).append(actual_score)

    rows = []
    for (player_name, market_key), values in player_market_values.items():
        if not values:
            continue
        recent_values = values[:10]
        last_5_values = values[:5]
        season_average = float(sum(values) / len(values))
        recent_average = float(sum(recent_values) / len(recent_values))
        last_5_average = float(sum(last_5_values) / len(last_5_values))
        trend = recent_average - season_average

        rows.append(
            {
                "sport_key": sport_key,
                "player_name": player_name,
                "market_key": market_key,
                "season_average": round(season_average, 4),
                "recent_average": round(recent_average, 4),
                "last_5_average": round(last_5_average, 4),
                "trend": round(trend, 4),
                "sample_size": len(values),
                "source": "sportsgameodds_history",
            }
        )

    result = upsert_stats_rows(rows, default_source="sportsgameodds_history")
    result["events_fetched"] = len(events)
    result["stat_records_built"] = sum(len(values) for values in player_market_values.values())
    return result


def sync_stats_from_balldontlie(
    sport_label: str,
    sport_key: str,
    player_names: list[str],
    days: int = 14,
) -> dict[str, int]:
    if not CONFIG.balldontlie_api_key.strip():
        raise ValueError("BALLDONTLIE_API_KEY is not configured in .env.")

    stat_fields = MARKET_STAT_FIELDS.get(sport_key, {})
    if not stat_fields:
        raise ValueError(f"No BALLDONTLIE stat mapping is configured for {sport_key}.")

    normalized_targets = {name.strip().lower() for name in player_names if name and name.strip()}
    if not normalized_targets:
        return {"rows_imported": 0, "games_fetched": 0, "stats_fetched": 0}

    client = BallDontLieClient()
    try:
        dates = recent_date_strings(days)
        games = client.get_games_for_dates(sport_label, dates)
        game_ids = [
            game.get("id")
            for game in games
            if isinstance(game, dict) and game.get("id") is not None
        ]
        stats = client.get_stats_for_game_ids(sport_label, game_ids)
    except Exception as exc:
        raise ValueError(format_balldontlie_error(exc)) from exc

    player_market_values: dict[tuple[str, str], list[float]] = {}
    for stat in stats:
        if not isinstance(stat, dict):
            continue
        player_name = _extract_player_name(stat)
        if not player_name or player_name.strip().lower() not in normalized_targets:
            continue

        for market_key, candidate_fields in stat_fields.items():
            stat_value = _extract_stat_value(stat, candidate_fields)
            if stat_value is None:
                continue
            player_market_values.setdefault((player_name, market_key), []).append(stat_value)

    rows = []
    for (player_name, market_key), values in player_market_values.items():
        if not values:
            continue
        recent_values = values[:10]
        last_5_values = values[:5]
        season_average = float(sum(values) / len(values))
        recent_average = float(sum(recent_values) / len(recent_values))
        last_5_average = float(sum(last_5_values) / len(last_5_values))
        trend = recent_average - season_average

        rows.append(
            {
                "sport_key": sport_key,
                "player_name": player_name,
                "market_key": market_key,
                "season_average": round(season_average, 4),
                "recent_average": round(recent_average, 4),
                "last_5_average": round(last_5_average, 4),
                "trend": round(trend, 4),
                "sample_size": len(values),
                "source": "balldontlie_api",
            }
        )

    result = upsert_stats_rows(rows, default_source="balldontlie_api")
    result["games_fetched"] = len(game_ids)
    result["stats_fetched"] = len(stats)
    return result
