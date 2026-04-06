import json
from datetime import datetime, timezone
from typing import Any
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from config import ODDS_API_BASE, ODDS_API_KEY

class OddsApiClient:
    def __init__(self, api_key: str = ODDS_API_KEY):
        self.api_key = api_key

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def _get(self, path: str, params: dict[str, Any] | None = None):
        params = params or {}
        params["apiKey"] = self.api_key
        url = f"{ODDS_API_BASE}{path}"

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json(), response.headers

    def get_sports(self):
        return self._get("/sports")[0]

    def get_events(self, sport_key: str, regions: str = "us", markets: str = "h2h"):
        data, headers = self._get(
            f"/sports/{sport_key}/odds",
            {
                "regions": regions,
                "markets": markets,
                "oddsFormat": "american",
                "dateFormat": "iso",
            },
        )
        return data, headers

    def get_event_props(
        self,
        sport_key: str,
        event_id: str,
        markets: list[str],
        regions: str = "us",
        bookmakers: str | None = None,
    ):
        params = {
            "regions": regions,
            "markets": ",".join(markets),
            "oddsFormat": "american",
            "dateFormat": "iso",
        }
        if bookmakers:
            params["bookmakers"] = bookmakers

        data, headers = self._get(
            f"/sports/{sport_key}/events/{event_id}/odds",
            params,
        )
        return data, headers

    @staticmethod
    def utcnow():
        return datetime.now(timezone.utc)

    @staticmethod
    def dumps_raw(obj: Any) -> str:
        return json.dumps(obj, ensure_ascii=False)
