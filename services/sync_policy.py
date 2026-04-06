from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
from typing import Any

from config import CONFIG
from db.models import AppState
from db.session import SessionLocal


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


def _read_db_state(namespace: str, state_key: str) -> Any:
    try:
        with SessionLocal() as db:
            row = (
                db.query(AppState)
                .filter(AppState.namespace == namespace, AppState.state_key == state_key)
                .one_or_none()
            )
            if row is None:
                return None
            return json.loads(row.value_json)
    except Exception:
        return None


def _write_db_state(namespace: str, state_key: str, payload: Any) -> None:
    try:
        encoded = json.dumps(payload, default=str)
        with SessionLocal() as db:
            row = (
                db.query(AppState)
                .filter(AppState.namespace == namespace, AppState.state_key == state_key)
                .one_or_none()
            )
            if row is None:
                db.add(
                    AppState(
                        namespace=namespace,
                        state_key=state_key,
                        value_json=encoded,
                        updated_at=datetime.now(timezone.utc),
                    )
                )
            else:
                row.value_json = encoded
                row.updated_at = datetime.now(timezone.utc)
            db.commit()
    except Exception:
        pass


def get_last_sync(provider: str, scope: str) -> datetime | None:
    raw = _read_db_state("sync_last_run", f"{provider}:{scope}")
    if raw is None:
        state = _read_state()
        raw = state.get(provider, {}).get(scope)
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def record_sync(provider: str, scope: str) -> None:
    payload = datetime.now(timezone.utc).isoformat()
    _write_db_state("sync_last_run", f"{provider}:{scope}", payload)
    state = _read_state()
    provider_state = state.setdefault(provider, {})
    provider_state[scope] = payload
    _write_state(state)


def record_sync_payload(provider: str, scope: str, payload: dict[str, Any]) -> None:
    db_payload = {
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    _write_db_state("sync_payload", f"{provider}:{scope}", db_payload)
    state = _read_state()
    provider_state = state.setdefault(provider, {})
    provider_state[scope] = db_payload
    _write_state(state)


def get_sync_payload(provider: str, scope: str) -> dict[str, Any]:
    raw = _read_db_state("sync_payload", f"{provider}:{scope}")
    if raw is None:
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
