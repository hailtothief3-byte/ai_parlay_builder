from datetime import datetime
from typing import Any


def parse_dt(value: str | None):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def infer_side(outcome_name: str) -> str | None:
    name = outcome_name.strip().lower()
    if name == "over":
        return "over"
    if name == "under":
        return "under"
    if name == "yes":
        return "yes"
    if name == "no":
        return "no"
    return None


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "external_event_id": event["id"],
        "sport_key": event["sport_key"],
        "commence_time": parse_dt(event.get("commence_time")),
        "home_team": event.get("home_team"),
        "away_team": event.get("away_team"),
    }


def normalize_market_lines(event_payload: dict[str, Any], pulled_at):
    rows = []

    external_event_id = event_payload["id"]
    sport_key = event_payload["sport_key"]
    event_commence_time = parse_dt(event_payload.get("commence_time"))

    for bookmaker in event_payload.get("bookmakers", []):
        bookmaker_key = bookmaker.get("key")
        bookmaker_title = bookmaker.get("title")
        bookmaker_last_update = parse_dt(bookmaker.get("last_update"))
        is_dfs = bookmaker_key in {"prizepicks", "underdog", "pick6", "betr_us_dfs"}

        for market in bookmaker.get("markets", []):
            market_key = market.get("key")

            for outcome in market.get("outcomes", []):
                rows.append(
                    {
                        "external_event_id": external_event_id,
                        "sport_key": sport_key,
                        "bookmaker_key": bookmaker_key,
                        "bookmaker_title": bookmaker_title,
                        "market_key": market_key,
                        "player_name": outcome.get("description"),
                        "outcome_name": outcome.get("name"),
                        "line": outcome.get("point"),
                        "price": outcome.get("price"),
                        "side": infer_side(outcome.get("name", "")),
                        "is_dfs": is_dfs,
                        "event_commence_time": event_commence_time,
                        "last_update": bookmaker_last_update,
                        "pulled_at": pulled_at,
                        "raw_json": str(outcome),
                    }
                )
    return rows
