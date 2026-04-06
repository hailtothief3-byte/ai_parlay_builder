import json
from datetime import datetime, timezone
from typing import Any

import requests
from tenacity import RetryError, retry, retry_if_exception, stop_after_attempt, wait_fixed

from config import ODDS_API_BASE, ODDS_API_KEY


class OddsApiFatalError(Exception):
    pass


def _should_retry_request(exc: Exception) -> bool:
    root = unwrap_api_exception(exc)

    if isinstance(root, requests.HTTPError):
        response = root.response
        if response is None:
            return True
        return response.status_code in {408, 409, 425, 429, 500, 502, 503, 504}

    return isinstance(root, requests.RequestException)


class OddsApiClient:
    def __init__(self, api_key: str = ODDS_API_KEY):
        self.api_key = api_key

    @retry(
        retry=retry_if_exception(_should_retry_request),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        reraise=True,
    )
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


def unwrap_api_exception(exc: Exception) -> Exception:
    root: Exception = exc
    if isinstance(exc, RetryError) and exc.last_attempt:
        try:
            root = exc.last_attempt.exception() or exc
        except Exception:
            root = exc
    return root


def extract_api_error_code(exc: Exception) -> str | None:
    root = unwrap_api_exception(exc)

    if not isinstance(root, requests.HTTPError) or root.response is None:
        return None

    try:
        payload = root.response.json()
    except ValueError:
        return None

    error_code = payload.get("error_code")
    return str(error_code) if error_code else None


def is_fatal_api_error(exc: Exception) -> bool:
    error_code = extract_api_error_code(exc)
    if error_code in {"OUT_OF_USAGE_CREDITS", "INVALID_API_KEY"}:
        return True

    root = unwrap_api_exception(exc)
    if isinstance(root, requests.HTTPError) and root.response is not None:
        return root.response.status_code in {401, 403}

    return False


def format_api_error(exc: Exception) -> str:
    root = unwrap_api_exception(exc)

    if isinstance(root, requests.HTTPError):
        response = root.response
        if response is None:
            return f"HTTPError: {root}"

        body = (response.text or "").strip().replace("\n", " ")
        if len(body) > 240:
            body = f"{body[:237]}..."

        parts = [
            f"HTTP {response.status_code}",
            response.reason or "HTTPError",
        ]
        if body:
            parts.append(body)
        return " | ".join(parts)

    if isinstance(root, requests.RequestException):
        return f"{type(root).__name__}: {root}"

    return f"{type(root).__name__}: {root}"
