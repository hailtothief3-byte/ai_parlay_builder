from __future__ import annotations

from typing import Any

import requests

from config import SPORTSGAMEODDS_API_BASE, SPORTSGAMEODDS_API_KEY


class SportsGameOddsClient:
    def __init__(self, api_key: str = SPORTSGAMEODDS_API_KEY):
        self.api_key = api_key

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        params = params or {}
        headers = {"X-API-Key": self.api_key}

        # The quickstart shows header auth, while the reference docs display query auth.
        # Sending both keeps this client compatible with either gateway behavior.
        request_params = {"apiKey": self.api_key, **params}
        response = requests.get(
            f"{SPORTSGAMEODDS_API_BASE}{path}",
            params=request_params,
            headers=headers,
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def get_usage(self) -> dict[str, Any]:
        return self._get("/account/usage")


def format_sgo_error(exc: Exception) -> str:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        body = (exc.response.text or "").strip().replace("\n", " ")
        if len(body) > 240:
            body = f"{body[:237]}..."
        return f"HTTP {exc.response.status_code} | {exc.response.reason} | {body}"

    return f"{type(exc).__name__}: {exc}"
