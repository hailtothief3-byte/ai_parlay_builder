import ast
import json

import pandas as pd
from sqlalchemy import select

from db.session import SessionLocal
from db.models import Event, MarketLine


def _coerce_sport_keys(sport_key: str | list[str]) -> list[str]:
    if isinstance(sport_key, str):
        return [sport_key]
    return [key for key in sport_key if key]


def _load_raw_payload(raw_json: str | None) -> dict:
    if not raw_json:
        return {}
    try:
        payload = json.loads(raw_json)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        try:
            payload = ast.literal_eval(raw_json)
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}


def _extract_player_team(raw_json: str | None) -> str | None:
    payload = _load_raw_payload(raw_json)
    if not payload:
        return None

    direct_team = payload.get("player_team")
    if isinstance(direct_team, str) and direct_team.strip():
        return direct_team.strip()

    odd_payload = payload.get("odd") if isinstance(payload.get("odd"), dict) else payload
    for field in ["playerTeam", "playerTeamName", "teamName", "team"]:
        value = odd_payload.get(field) if isinstance(odd_payload, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()

    return None


def get_latest_board(sport_key: str | list[str], is_dfs: bool | None = None):
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        stmt = select(MarketLine).where(MarketLine.sport_key.in_(sport_keys))

        if is_dfs is not None:
            stmt = stmt.where(MarketLine.is_dfs == is_dfs)

        rows = db.execute(stmt).scalars().all()

        if not rows:
            return pd.DataFrame()

        event_ids = sorted(
            {
                row.external_event_id
                for row in rows
                if row.external_event_id
            }
        )
        event_map: dict[str, Event] = {}
        if event_ids:
            event_rows = db.execute(
                select(Event).where(Event.external_event_id.in_(event_ids))
            ).scalars().all()
            event_map = {row.external_event_id: row for row in event_rows}

        df = pd.DataFrame(
            [
                {
                    "event_id": r.external_event_id,
                    "book": r.bookmaker_title,
                    "book_key": r.bookmaker_key,
                    "market": r.market_key,
                    "player": r.player_name,
                    "pick": r.outcome_name,
                    "line": r.line,
                    "price": r.price,
                    "side": r.side,
                    "is_dfs": r.is_dfs,
                    "commence_time": r.event_commence_time,
                    "last_update": r.last_update,
                    "pulled_at": r.pulled_at,
                    "player_team": _extract_player_team(r.raw_json),
                    "home_team": getattr(event_map.get(r.external_event_id), "home_team", None),
                    "away_team": getattr(event_map.get(r.external_event_id), "away_team", None),
                }
                for r in rows
            ]
        )

        df = df.sort_values("pulled_at")
        df = df.drop_duplicates(
            subset=["event_id", "book_key", "market", "player", "pick", "line"],
            keep="last",
        )
        return df
