from __future__ import annotations

from typing import Any

from config import CONFIG
from ingestion.sportsgameodds_api import SportsGameOddsClient, format_sgo_error


def get_sportsgameodds_usage_summary() -> dict[str, Any]:
    if not CONFIG.sportsgameodds_api_key.strip():
        return {
            "enabled": False,
            "ok_to_sync": False,
            "message": "No SportsGameOdds API key configured.",
        }

    client = SportsGameOddsClient()
    payload = client.get_usage()
    data = payload.get("data", {}) if isinstance(payload, dict) else {}
    rate_limits = data.get("rateLimits", {}) if isinstance(data, dict) else {}

    per_minute = rate_limits.get("per-minute", {}) or {}
    per_day = rate_limits.get("per-day", {}) or {}
    per_month = rate_limits.get("per-month", {}) or {}

    max_minute_requests = per_minute.get("max-requests")
    current_minute_requests = per_minute.get("current-requests", 0)
    max_day_entities = per_day.get("max-entities")
    current_day_entities = per_day.get("current-entities", 0)
    max_month_entities = per_month.get("max-entities")
    current_month_entities = per_month.get("current-entities", 0)

    minute_requests_remaining = (
        max_minute_requests - current_minute_requests
        if isinstance(max_minute_requests, int)
        else None
    )
    day_entities_remaining = (
        max_day_entities - current_day_entities
        if isinstance(max_day_entities, int)
        else None
    )
    month_entities_remaining = (
        max_month_entities - current_month_entities
        if isinstance(max_month_entities, int)
        else None
    )

    monthly_ok = (
        month_entities_remaining is None
        or month_entities_remaining >= CONFIG.sportsgameodds_min_monthly_entities_remaining
    )
    daily_ok = (
        day_entities_remaining is None
        or day_entities_remaining >= CONFIG.sportsgameodds_min_daily_entities_remaining
    )
    minute_ok = (
        minute_requests_remaining is None
        or minute_requests_remaining >= CONFIG.sportsgameodds_minute_request_buffer
    )

    ok_to_sync = monthly_ok and daily_ok and minute_ok

    return {
        "enabled": True,
        "ok_to_sync": ok_to_sync,
        "tier": data.get("tier"),
        "minute_requests_remaining": minute_requests_remaining,
        "day_entities_remaining": day_entities_remaining,
        "month_entities_remaining": month_entities_remaining,
        "message": (
            "Sync allowed."
            if ok_to_sync
            else "Sync blocked by usage guard to avoid exhausting SportsGameOdds allocation."
        ),
    }


def safe_get_sportsgameodds_usage_summary() -> dict[str, Any]:
    try:
        return get_sportsgameodds_usage_summary()
    except Exception as exc:
        formatted_error = format_sgo_error(exc)
        normalized_error = str(formatted_error or "").lower()
        if "401" in normalized_error or "invalid api key" in normalized_error or "unauthorized" in normalized_error:
            return {
                "enabled": False,
                "ok_to_sync": False,
                "auth_error": True,
                "message": "SportsGameOdds is not available locally because the configured API key is invalid or expired.",
                "detail": "Update SPORTSGAMEODDS_API_KEY in your local .env to restore live sync and usage checks.",
            }
        return {
            "enabled": True,
            "ok_to_sync": False,
            "message": "Unable to fetch SportsGameOdds usage right now.",
            "detail": formatted_error,
        }


def estimate_sportsgameodds_sync_cost(sport_label: str) -> dict[str, Any]:
    # Conservative upper-bound estimate for one manual sport sync.
    # One event object contains the event plus a bundle of requested odds objects.
    average_market_objects_per_event = {
        "NBA": 220,
        "MLB": 90,
        "NFL": 180,
    }

    events = CONFIG.sportsgameodds_max_events_per_league_sync
    per_event = average_market_objects_per_event.get(sport_label, 120)
    estimated_entities = events * per_event

    return {
        "sport": sport_label,
        "max_events": events,
        "estimated_entities": estimated_entities,
        "note": "Estimated upper bound based on current event cap and typical market density.",
    }
