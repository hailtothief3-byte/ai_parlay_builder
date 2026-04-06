from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import requests

from config import BALLDONTLIE_API_BASE, BALLDONTLIE_API_KEY


SPORT_PATHS = {
    "NBA": "nba",
    "MLB": "mlb",
}


class BallDontLieClient:
    def __init__(self, api_key: str = BALLDONTLIE_API_KEY):
        self.api_key = api_key

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        headers = {"Authorization": self.api_key}
        response = requests.get(
            f"{BALLDONTLIE_API_BASE}{path}",
            headers=headers,
            params=params or {},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_games_for_dates(self, sport_label: str, dates: list[str]) -> list[dict[str, Any]]:
        sport_path = SPORT_PATHS[sport_label]
        payload = self._get(
            f"/{sport_path}/v1/games",
            params={"dates[]": dates},
        )
        return payload.get("data", []) if isinstance(payload, dict) else []

    def get_stats_for_game_ids(self, sport_label: str, game_ids: list[int | str]) -> list[dict[str, Any]]:
        if not game_ids:
            return []

        sport_path = SPORT_PATHS[sport_label]
        payload = self._get(
            f"/{sport_path}/v1/stats",
            params={"game_ids[]": [str(game_id) for game_id in game_ids]},
        )
        return payload.get("data", []) if isinstance(payload, dict) else []


def recent_date_strings(days: int) -> list[str]:
    today = date.today()
    return [
        (today - timedelta(days=offset)).isoformat()
        for offset in range(max(days, 1))
    ]


def format_balldontlie_error(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status_code = exc.response.status_code
        reason = exc.response.reason or "HTTP error"
        body = (exc.response.text or "").strip().replace("\n", " ")
        if len(body) > 180:
            body = body[:177] + "..."

        if status_code == 401:
            return (
                "BALLDONTLIE rejected the API key or your current plan does not have access "
                f"to this endpoint. HTTP 401 {reason}."
            )
        if status_code == 403:
            return (
                "BALLDONTLIE denied access to this endpoint for the current account or plan. "
                f"HTTP 403 {reason}."
            )
        if status_code == 404:
            return f"BALLDONTLIE endpoint not found. HTTP 404 {reason}."

        return f"BALLDONTLIE request failed. HTTP {status_code} {reason}. {body}"

    return f"{type(exc).__name__}: {exc}"
