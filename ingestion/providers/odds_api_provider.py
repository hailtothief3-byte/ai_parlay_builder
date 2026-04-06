from __future__ import annotations

from sqlalchemy.exc import IntegrityError

from db.models import Event, MarketLine
from db.session import SessionLocal
from ingestion.normalize import normalize_event, normalize_market_lines
from ingestion.odds_api import OddsApiClient, format_api_error, is_fatal_api_error
from ingestion.providers.base import BaseProvider, SyncResult
from sports_config import discover_live_sport_keys, find_sport_label_for_key, get_sport_config, get_sport_provider_name, get_syncable_labels


DFS_BOOKMAKERS = "prizepicks,underdog,pick6,betr_us_dfs"
CORE_SPORT_KEYS = [
    "basketball_nba",
    "baseball_mlb",
    "icehockey_nhl",
    "americanfootball_nfl",
    "basketball_ncaab",
    "baseball_mlb_preseason",
]


def _upsert_event(db, payload: dict) -> None:
    existing = db.query(Event).filter(
        Event.external_event_id == payload["external_event_id"]
    ).one_or_none()

    if existing:
        existing.sport_key = payload["sport_key"]
        existing.commence_time = payload["commence_time"]
        existing.home_team = payload["home_team"]
        existing.away_team = payload["away_team"]
    else:
        db.add(Event(**payload))


def _save_rows(db, rows: list[dict]) -> int:
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


class OddsApiProvider(BaseProvider):
    name = "the_odds_api"

    def __init__(self) -> None:
        self.client = OddsApiClient()

    def _supported_labels(self) -> list[str]:
        return [
            label
            for label in get_syncable_labels()
            if get_sport_provider_name(label) == self.name
        ]

    def _resolve_sync_sports(self) -> list[str]:
        supported_labels = self._supported_labels()
        if not supported_labels:
            return []

        sports = self.client.get_sports()
        discovered = discover_live_sport_keys(sports)

        resolved = list(CORE_SPORT_KEYS)
        for label in supported_labels:
            resolved.extend(discovered.get(label, []))

        return list(dict.fromkeys(resolved))

    def sync_events(self) -> SyncResult:
        result = SyncResult(provider=self.name)

        try:
            supported_sports = self._resolve_sync_sports()
            if not supported_sports:
                result.messages.append("No sports are currently assigned to The Odds API provider.")
                return result
        except Exception as exc:
            message = f"Sport discovery failed: {format_api_error(exc)}"
            print(message)
            result.events_ok = False
            result.messages.append(message)
            if is_fatal_api_error(exc):
                result.messages.append("Stopping The Odds API sync because credentials or credits are unavailable.")
                return result
            supported_sports = CORE_SPORT_KEYS.copy()

        with SessionLocal() as db:
            for sport_key in supported_sports:
                try:
                    events, _ = self.client.get_events(sport_key=sport_key, markets="h2h")
                    print(f"{sport_key}: {len(events)} events")
                    result.events_count += len(events)
                    for event in events:
                        _upsert_event(db, normalize_event(event))
                    db.commit()
                except Exception as exc:
                    message = f"Failed {sport_key}: {format_api_error(exc)}"
                    print(message)
                    result.messages.append(message)
                    result.events_ok = False
                    if is_fatal_api_error(exc):
                        result.messages.append("Stopping The Odds API event sync because credentials or credits are unavailable.")
                        return result

        return result

    def sync_props(self) -> SyncResult:
        result = SyncResult(provider=self.name)

        with SessionLocal() as db:
            events = db.query(Event).all()

            for event in events:
                label = find_sport_label_for_key(event.sport_key)
                if not label or get_sport_provider_name(label) != self.name:
                    continue

                markets = list(get_sport_config(label)["prop_markets"])
                if not markets:
                    continue

                try:
                    payload, _ = self.client.get_event_props(
                        sport_key=event.sport_key,
                        event_id=event.external_event_id,
                        markets=markets,
                        regions="us,us_dfs",
                    )
                    rows = normalize_market_lines(payload, self.client.utcnow())
                    inserted = _save_rows(db, rows)
                    result.props_count += inserted
                    print(f"{event.sport_key} {event.away_team} @ {event.home_team}: {inserted} rows")
                except Exception as exc:
                    message = (
                        f"Prop sync failed for {event.external_event_id} "
                        f"({event.away_team} @ {event.home_team}): {format_api_error(exc)}"
                    )
                    print(message)
                    result.messages.append(message)
                    result.props_ok = False
                    if is_fatal_api_error(exc):
                        result.messages.append("Stopping The Odds API prop sync because credentials or credits are unavailable.")
                        return result

        return result

    def sync_dfs(self) -> SyncResult:
        result = SyncResult(provider=self.name)

        with SessionLocal() as db:
            events = db.query(Event).all()

            for event in events:
                label = find_sport_label_for_key(event.sport_key)
                if not label or get_sport_provider_name(label) != self.name:
                    continue

                markets = list(get_sport_config(label)["dfs_markets"])
                if not markets:
                    continue

                try:
                    payload, _ = self.client.get_event_props(
                        sport_key=event.sport_key,
                        event_id=event.external_event_id,
                        markets=markets,
                        regions="us_dfs",
                        bookmakers=DFS_BOOKMAKERS,
                    )
                    rows = normalize_market_lines(payload, self.client.utcnow())
                    rows = [row for row in rows if row["is_dfs"]]
                    inserted = _save_rows(db, rows)
                    result.dfs_count += inserted
                    print(f"DFS synced {inserted} rows for {event.external_event_id}")
                except Exception as exc:
                    message = f"DFS sync failed for {event.external_event_id}: {format_api_error(exc)}"
                    print(message)
                    result.messages.append(message)
                    result.dfs_ok = False
                    if is_fatal_api_error(exc):
                        result.messages.append("Stopping The Odds API DFS sync because credentials or credits are unavailable.")
                        return result

        return result
