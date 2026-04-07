from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

from sqlalchemy.exc import IntegrityError

from config import CONFIG
from db.models import Event, MarketLine
from db.session import SessionLocal
from ingestion.providers.base import BaseProvider, SyncResult
from ingestion.sportsgameodds_api import SportsGameOddsClient, format_sgo_error
from sports_config import get_provider_labels, get_sport_config
from services.sync_policy import record_sync, sync_allowed


LEAGUE_IDS = {
    "NBA": "NBA",
    "MLB": "MLB",
    "NFL": "NFL",
}

# Inferred from SportsGameOdds' public guides:
# NBA docs show points/assists/rebounds/threes_made with oddID patterns.
# NFL docs show rushing_yards and similar oddID patterns.
# MLB docs show batting_hits and related player props.
PROP_ODD_IDS = {
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

NBA_EXOTIC_ODD_IDS = [
    "first_basket-PLAYER_ID-game-yn-yes",
    "first_basket-PLAYER_ID-game-yn-no",
    "first_to_score-PLAYER_ID-game-yn-yes",
    "first_to_score-PLAYER_ID-game-yn-no",
]

MARKET_KEY_MAP = {
    "points": "player_points",
    "first_basket": "player_first_basket",
    "first_to_score": "player_first_basket",
    "first_basket_scorer": "player_first_basket",
    "rebounds": "player_rebounds",
    "assists": "player_assists",
    "threes_made": "player_threes",
    "batting_hits": "player_hits",
    "total_bases": "player_total_bases",
    "home_runs": "player_home_runs",
    "pitcher_strikeouts": "player_strikeouts",
    "passing_yards": "player_pass_yds",
    "rushing_yards": "player_rush_yds",
    "receiving_yards": "player_reception_yds",
    "receptions": "player_receptions",
}

BOOKMAKERS = "fanduel,draftkings,betmgm,caesars"
DEBUG_SAMPLE_PATH = "data/sportsgameodds_event_sample.json"
NBA_EXOTIC_DEBUG_PATH = "data/sportsgameodds_nba_exotics_debug.json"
NBA_EXOTIC_KEYWORDS = ("first", "basket", "score", "scorer")


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _save_market_lines(db, rows: list[dict]) -> int:
    inserted = 0
    for row in rows:
        try:
            db.add(MarketLine(**row))
            db.flush()
            inserted += 1
        except IntegrityError:
            db.rollback()
    db.commit()
    return inserted


def _upsert_event(db, event_payload: dict, sport_key: str) -> str | None:
    event_id = str(
        event_payload.get("eventID")
        or event_payload.get("id")
        or event_payload.get("gameID")
        or ""
    ).strip()
    if not event_id:
        return None

    commence_time = _parse_dt(
        event_payload.get("startTime")
        or event_payload.get("startsAt")
        or event_payload.get("commenceTime")
        or (event_payload.get("status", {}) or {}).get("startsAt")
    )
    teams = event_payload.get("teams", {}) if isinstance(event_payload.get("teams"), dict) else {}
    home_team = teams.get("home", {}) if isinstance(teams.get("home"), dict) else {}
    away_team = teams.get("away", {}) if isinstance(teams.get("away"), dict) else {}
    home_names = home_team.get("names", {}) if isinstance(home_team.get("names"), dict) else {}
    away_names = away_team.get("names", {}) if isinstance(away_team.get("names"), dict) else {}
    home_name = (
        home_names.get("long")
        or home_team.get("name")
        or event_payload.get("homeTeamName")
        or event_payload.get("homeTeamID")
    )
    away_name = (
        away_names.get("long")
        or away_team.get("name")
        or event_payload.get("awayTeamName")
        or event_payload.get("awayTeamID")
    )

    existing = db.query(Event).filter(Event.external_event_id == event_id).one_or_none()
    if existing:
        existing.sport_key = sport_key
        existing.commence_time = commence_time
        existing.home_team = home_name
        existing.away_team = away_name
    else:
        db.add(
            Event(
                external_event_id=event_id,
                sport_key=sport_key,
                commence_time=commence_time,
                home_team=home_name,
                away_team=away_name,
            )
        )

    db.commit()
    return event_id


def _extract_odds_iterable(event_payload: dict) -> list[dict]:
    candidates = [
        event_payload.get("odds"),
        event_payload.get("markets"),
        event_payload.get("offers"),
        event_payload.get("sportsbooks"),
        event_payload.get("lines"),
    ]

    for odds in candidates:
        if isinstance(odds, dict):
            out: list[dict] = []
            for value in odds.values():
                if isinstance(value, dict):
                    out.append(value)
                elif isinstance(value, list):
                    out.extend(item for item in value if isinstance(item, dict))
            if out:
                return out
        if isinstance(odds, list):
            out = [value for value in odds if isinstance(value, dict)]
            if out:
                return out
    return []


def _player_name_from_odd(odd: dict) -> str | None:
    return (
        odd.get("playerName")
        or odd.get("name")
        or odd.get("statEntityName")
        or odd.get("participantName")
    )


def _player_name_from_event(event_payload: dict, odd: dict) -> str | None:
    direct_name = _player_name_from_odd(odd)
    if direct_name:
        return direct_name

    player_id = odd.get("playerID") or odd.get("statEntityID")
    players = event_payload.get("players", {}) if isinstance(event_payload.get("players"), dict) else {}
    player = players.get(player_id, {}) if isinstance(players.get(player_id), dict) else {}
    return player.get("name")


def _player_team_from_event(event_payload: dict, odd: dict) -> str | None:
    player_id = odd.get("playerID") or odd.get("statEntityID")
    players = event_payload.get("players", {}) if isinstance(event_payload.get("players"), dict) else {}
    teams = event_payload.get("teams", {}) if isinstance(event_payload.get("teams"), dict) else {}
    player = players.get(player_id, {}) if isinstance(players.get(player_id), dict) else {}
    player_team_id = player.get("teamID")
    if not player_team_id:
        return None

    for side in ["home", "away"]:
        team = teams.get(side, {}) if isinstance(teams.get(side), dict) else {}
        if str(team.get("teamID") or "") != str(player_team_id):
            continue
        names = team.get("names", {}) if isinstance(team.get("names"), dict) else {}
        return (
            names.get("short")
            or names.get("medium")
            or names.get("long")
            or names.get("location")
            or team.get("name")
        )
    return None


def _market_key_from_odd(odd: dict) -> str | None:
    odd_id = str(odd.get("oddID") or odd.get("marketID") or odd.get("marketKey") or "")
    stat_id = odd_id.split("-")[0] if odd_id else ""
    if not stat_id:
        stat_id = str(
            odd.get("statID")
            or odd.get("stat")
            or odd.get("marketType")
            or odd.get("marketName")
            or ""
        ).lower().replace(" ", "_")
    if not stat_id:
        market_name = str(odd.get("marketName") or "").strip().lower()
        if any(phrase in market_name for phrase in ["first basket", "first to score", "first scorer"]):
            return "player_first_basket"
        return None

    mapped = MARKET_KEY_MAP.get(stat_id)
    if mapped:
        return mapped

    market_name = str(odd.get("marketName") or "").strip().lower()
    if any(phrase in market_name for phrase in ["first basket", "first to score", "first scorer"]):
        return "player_first_basket"
    return None


def _side_from_odd(odd: dict, market_key: str | None = None) -> str | None:
    side = str(
        odd.get("sideID")
        or odd.get("side")
        or odd.get("outcome")
        or odd.get("betType")
        or ""
    ).lower()
    if side in {"over", "under", "yes", "no"}:
        return side

    if market_key == "player_first_basket":
        return "yes"
    return None


def _pick_name_from_side(side: str | None) -> str:
    if side == "over":
        return "Over"
    if side == "under":
        return "Under"
    if side == "yes":
        return "Yes"
    if side == "no":
        return "No"
    return "Pick"


def _line_from_odd(odd: dict) -> float | None:
    for field in ["bookOverUnder", "fairOverUnder", "line", "closeOverUnder", "points", "value"]:
        value = odd.get(field)
        if value in (None, ""):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _price_from_odd(odd: dict) -> float | None:
    for field in ["bookOdds", "fairOdds", "odds", "americanOdds", "price", "sportsbookOdds"]:
        value = odd.get(field)
        if value in (None, ""):
            continue
        if isinstance(value, str):
            value = value.replace("+", "")
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _write_debug_sample(label: str, event_payload: dict) -> None:
    try:
        payload = {
            "label": label,
            "top_level_keys": sorted(event_payload.keys()),
            "event_sample": event_payload,
        }
        with open(DEBUG_SAMPLE_PATH, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
    except Exception:
        pass


def _write_json_debug(path: str, payload: dict) -> None:
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
    except Exception:
        pass


def _event_starts_at(event_payload: dict) -> datetime | None:
    return _parse_dt(
        event_payload.get("startTime")
        or event_payload.get("startsAt")
        or event_payload.get("commenceTime")
        or (event_payload.get("status", {}) or {}).get("startsAt")
    )


def _should_keep_event(event_payload: dict) -> bool:
    if not CONFIG.sportsgameodds_only_future_events:
        return True

    starts_at = _event_starts_at(event_payload)
    if starts_at is None:
        return False

    now = datetime.now(timezone.utc)
    latest_allowed = now + timedelta(hours=CONFIG.sportsgameodds_future_window_hours)
    return now <= starts_at <= latest_allowed


def _normalize_market_lines(event_payload: dict, sport_key: str, pulled_at: datetime) -> list[dict]:
    event_id = str(event_payload.get("eventID") or event_payload.get("id") or event_payload.get("gameID") or "").strip()
    if not event_id:
        return []

    event_commence_time = _parse_dt(
        event_payload.get("startTime")
        or event_payload.get("startsAt")
        or event_payload.get("commenceTime")
        or (event_payload.get("status", {}) or {}).get("startsAt")
    )

    rows: list[dict] = []
    for odd in _extract_odds_iterable(event_payload):
        market_key = _market_key_from_odd(odd)
        player_name = _player_name_from_event(event_payload, odd)
        side = _side_from_odd(odd, market_key=market_key)
        line = _line_from_odd(odd)
        price = _price_from_odd(odd)

        if not market_key or not player_name or side is None:
            continue

        by_bookmaker = odd.get("byBookmaker", {}) if isinstance(odd.get("byBookmaker"), dict) else {}
        player_team = _player_team_from_event(event_payload, odd)
        if by_bookmaker:
            for bookmaker_key, bookmaker_data in by_bookmaker.items():
                if not isinstance(bookmaker_data, dict):
                    continue
                if bookmaker_data.get("available") is False:
                    continue

                book_line = line
                book_price = price

                if bookmaker_data.get("overUnder") not in (None, ""):
                    try:
                        book_line = float(bookmaker_data["overUnder"])
                    except (TypeError, ValueError):
                        pass

                if bookmaker_data.get("odds") not in (None, ""):
                    try:
                        odds_value = bookmaker_data["odds"]
                        if isinstance(odds_value, str):
                            odds_value = odds_value.replace("+", "")
                        book_price = float(odds_value)
                    except (TypeError, ValueError):
                        pass

                last_update = _parse_dt(bookmaker_data.get("lastUpdatedAt")) or pulled_at
                rows.append(
                    {
                        "external_event_id": event_id,
                        "sport_key": sport_key,
                        "bookmaker_key": str(bookmaker_key),
                        "bookmaker_title": str(bookmaker_key).replace("_", " ").title(),
                        "market_key": market_key,
                        "player_name": player_name,
                        "outcome_name": _pick_name_from_side(side),
                        "line": book_line,
                        "price": book_price,
                        "side": side,
                        "is_dfs": str(bookmaker_key) in {"prizepicks", "underdog"},
                        "event_commence_time": event_commence_time,
                        "last_update": last_update,
                        "pulled_at": pulled_at,
                        "raw_json": json.dumps(
                            {
                                "odd": odd,
                                "bookmaker": bookmaker_key,
                                "data": bookmaker_data,
                                "player_team": player_team,
                            },
                            default=str,
                        ),
                    }
                )
        else:
            bookmaker_key = str(odd.get("bookmakerID") or "sportsgameodds_consensus")
            bookmaker_title = bookmaker_key.replace("_", " ").title()
            last_update = _parse_dt(
                odd.get("updatedAt")
                or odd.get("lastUpdated")
                or event_payload.get("updatedAt")
            ) or pulled_at

            rows.append(
                {
                    "external_event_id": event_id,
                    "sport_key": sport_key,
                    "bookmaker_key": bookmaker_key,
                    "bookmaker_title": bookmaker_title,
                    "market_key": market_key,
                    "player_name": player_name,
                    "outcome_name": _pick_name_from_side(side),
                    "line": line,
                    "price": price,
                    "side": side,
                    "is_dfs": bookmaker_key in {"prizepicks", "underdog"},
                    "event_commence_time": event_commence_time,
                    "last_update": last_update,
                    "pulled_at": pulled_at,
                    "raw_json": json.dumps(
                        {
                            "odd": odd,
                            "player_team": player_team,
                        },
                        default=str,
                    ),
                }
            )

    return rows


def _build_exotic_debug_payload(label: str, events: list[dict], normalized_rows: list[dict]) -> dict:
    candidate_markets: list[dict] = []
    distinct_stat_ids: set[str] = set()
    distinct_market_names: set[str] = set()
    distinct_odd_ids: set[str] = set()

    for event_payload in events:
        for odd in _extract_odds_iterable(event_payload):
            odd_id = str(odd.get("oddID") or "").strip()
            stat_id = str(odd.get("statID") or "").strip()
            market_name = str(odd.get("marketName") or "").strip()

            if odd_id:
                distinct_odd_ids.add(odd_id)
            if stat_id:
                distinct_stat_ids.add(stat_id)
            if market_name:
                distinct_market_names.add(market_name)

            haystack = " ".join(
                [
                    odd_id.lower(),
                    stat_id.lower(),
                    market_name.lower(),
                    str(odd.get("betTypeID") or "").lower(),
                    str(odd.get("sideID") or "").lower(),
                ]
            )
            if any(keyword in haystack for keyword in NBA_EXOTIC_KEYWORDS):
                candidate_markets.append(
                    {
                        "oddID": odd_id,
                        "statID": stat_id,
                        "marketName": market_name,
                        "betTypeID": odd.get("betTypeID"),
                        "sideID": odd.get("sideID"),
                        "playerID": odd.get("playerID") or odd.get("statEntityID"),
                    }
                )

    market_counts: dict[str, int] = {}
    for row in normalized_rows:
        market_key = str(row.get("market_key") or "")
        market_counts[market_key] = market_counts.get(market_key, 0) + 1

    return {
        "label": label,
        "event_count": len(events),
        "normalized_market_counts": market_counts,
        "candidate_market_count": len(candidate_markets),
        "candidate_markets": candidate_markets[:250],
        "distinct_stat_ids": sorted(distinct_stat_ids),
        "distinct_market_names": sorted(distinct_market_names)[:250],
        "distinct_odd_ids_sample": sorted(distinct_odd_ids)[:250],
        "top_level_event_keys": sorted(events[0].keys()) if events else [],
    }


class SportsGameOddsProvider(BaseProvider):
    name = "sportsgameodds"

    def __init__(self) -> None:
        self.client = SportsGameOddsClient()

    def _supported_labels(self) -> list[str]:
        return get_provider_labels(self.name)

    def _fetch_events_for_label(self, label: str) -> list[dict]:
        league_id = LEAGUE_IDS.get(label)
        odd_ids = list(PROP_ODD_IDS.get(label, []))
        if label == "NBA" and CONFIG.sportsgameodds_include_nba_exotics:
            odd_ids.extend(NBA_EXOTIC_ODD_IDS)
        if not league_id or not odd_ids:
            return []

        results: list[dict] = []
        cursor: str | None = None
        while True:
            params = {
                "leagueID": league_id,
                "oddsAvailable": "true",
                "limit": 100,
                "bookmakerID": BOOKMAKERS,
                "oddIDs": ",".join(odd_ids),
            }
            if cursor:
                params["cursor"] = cursor

            payload = self.client._get("/events", params=params)
            batch = payload.get("data", []) if isinstance(payload, dict) else []
            if not isinstance(batch, list):
                break

            filtered_batch = [event for event in batch if isinstance(event, dict) and _should_keep_event(event)]
            results.extend(filtered_batch)
            cursor = payload.get("nextCursor") if isinstance(payload, dict) else None
            if not cursor or len(results) >= CONFIG.sportsgameodds_max_events_per_league_sync:
                break

        return results[: CONFIG.sportsgameodds_max_events_per_league_sync]

    def sync_events(self) -> SyncResult:
        return self.sync_events_for_labels(self._supported_labels())

    def sync_events_for_labels(self, labels: list[str]) -> SyncResult:
        result = SyncResult(provider=self.name)
        pulled_at = datetime.now(timezone.utc)

        with SessionLocal() as db:
            for label in labels:
                sport_key = get_sport_config(label)["live_keys"][0]
                allowed, message = sync_allowed(self.name, label)
                if not allowed:
                    print(message)
                    result.messages.append(message)
                    result.events_ok = False
                    continue
                try:
                    events = self._fetch_events_for_label(label)
                    print(f"{label}: {len(events)} events")
                    result.events_count += len(events)
                    if events:
                        _write_debug_sample(label, events[0])

                    all_rows: list[dict] = []
                    for event_payload in events:
                        event_id = _upsert_event(db, event_payload, sport_key)
                        if not event_id:
                            continue
                        all_rows.extend(_normalize_market_lines(event_payload, sport_key, pulled_at))

                    inserted = _save_market_lines(db, all_rows)
                    result.props_count += inserted
                    print(f"{label}: inserted {inserted} market rows")
                    if label == "NBA" and CONFIG.sportsgameodds_include_nba_exotics:
                        first_basket_rows = [
                            row for row in all_rows
                            if row.get("market_key") == "player_first_basket"
                        ]
                        if first_basket_rows:
                            message = (
                                f"NBA exotic discovery found {len(first_basket_rows)} "
                                "player_first_basket rows."
                            )
                            print(message)
                            result.messages.append(message)
                        elif events:
                            _write_json_debug(
                                NBA_EXOTIC_DEBUG_PATH,
                                _build_exotic_debug_payload(label, events, all_rows),
                            )
                            message = (
                                "NBA exotic discovery is enabled, but no player_first_basket rows "
                                f"were returned. Debug report saved to {NBA_EXOTIC_DEBUG_PATH}."
                            )
                            print(message)
                            result.messages.append(message)
                    record_sync(self.name, label)
                    if events and inserted == 0:
                        sample_keys = ", ".join(sorted(events[0].keys()))
                        message = (
                            f"SportsGameOdds returned events for {label}, but no odds were normalized. "
                            f"Sample event keys: {sample_keys}. Debug sample saved to {DEBUG_SAMPLE_PATH}."
                        )
                        print(message)
                        result.messages.append(message)
                except Exception as exc:
                    message = f"SportsGameOdds sync failed for {label}: {format_sgo_error(exc)}"
                    print(message)
                    result.events_ok = False
                    result.props_ok = False
                    result.messages.append(message)

        return result

    def sync_props(self) -> SyncResult:
        # Player props are already fetched through the /events endpoint above.
        return SyncResult(provider=self.name)

    def sync_dfs(self) -> SyncResult:
        return SyncResult(provider=self.name)
