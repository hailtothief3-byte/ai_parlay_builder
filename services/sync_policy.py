from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from typing import Any

from config import CONFIG


STATE_PATH = Path("data/sync_state.json")


def _read_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return {}
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def get_last_sync(provider: str, scope: str) -> datetime | None:
    state = _read_state()
    raw = state.get(provider, {}).get(scope)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def record_sync(provider: str, scope: str) -> None:
    state = _read_state()
    provider_state = state.setdefault(provider, {})
    provider_state[scope] = datetime.now(timezone.utc).isoformat()
    _write_state(state)


def record_sync_payload(provider: str, scope: str, payload: dict[str, Any]) -> None:
    state = _read_state()
    provider_state = state.setdefault(provider, {})
    provider_state[scope] = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    _write_state(state)


def get_sync_payload(provider: str, scope: str) -> dict[str, Any]:
    state = _read_state()
    raw = state.get(provider, {}).get(scope)
    if isinstance(raw, dict):
        return raw
    return {}


def sync_allowed(provider: str, scope: str) -> tuple[bool, str]:
    last_sync = get_last_sync(provider, scope)
    if last_sync is None:
        return True, "Sync allowed."

    cooldown = timedelta(minutes=CONFIG.sportsgameodds_sync_cooldown_minutes)
    next_allowed = last_sync + cooldown
    now = datetime.now(timezone.utc)
    if now >= next_allowed:
        return True, "Sync allowed."

    remaining = next_allowed - now
    minutes = max(1, int(remaining.total_seconds() // 60))
    return False, f"Sync cooldown active for `{scope}`. Try again in about {minutes} minute(s)."
