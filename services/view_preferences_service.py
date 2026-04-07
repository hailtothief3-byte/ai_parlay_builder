from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from db.models import AppState
from db.session import SessionLocal


VIEW_PREFERENCES_NAMESPACE = "view_preferences"


def _scope_key(sport_label: str) -> str:
    return (sport_label or "GLOBAL").strip().upper()


def _load_state(sport_label: str) -> dict[str, Any]:
    scope_key = _scope_key(sport_label)
    try:
        with SessionLocal() as db:
            row = (
                db.query(AppState)
                .filter(
                    AppState.namespace == VIEW_PREFERENCES_NAMESPACE,
                    AppState.state_key == scope_key,
                )
                .one_or_none()
            )
            if row is None:
                return {}
            payload = json.loads(row.value_json)
            return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_state(sport_label: str, payload: dict[str, Any]) -> None:
    scope_key = _scope_key(sport_label)
    encoded = json.dumps(payload, default=str)
    with SessionLocal() as db:
        row = (
            db.query(AppState)
            .filter(
                AppState.namespace == VIEW_PREFERENCES_NAMESPACE,
                AppState.state_key == scope_key,
            )
            .one_or_none()
        )
        if row is None:
            db.add(
                AppState(
                    namespace=VIEW_PREFERENCES_NAMESPACE,
                    state_key=scope_key,
                    value_json=encoded,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        else:
            row.value_json = encoded
            row.updated_at = datetime.now(timezone.utc)
        db.commit()


def get_view_preference(sport_label: str, preference_key: str, default: str) -> str:
    payload = _load_state(sport_label)
    value = payload.get(preference_key, default)
    return str(value) if value is not None else default


def save_view_preference(sport_label: str, preference_key: str, value: str) -> None:
    payload = _load_state(sport_label)
    payload[preference_key] = value
    _save_state(sport_label, payload)
