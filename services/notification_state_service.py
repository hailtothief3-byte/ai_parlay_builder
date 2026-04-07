from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from typing import Any

from db.models import AppState
from db.session import SessionLocal


NOTIFICATION_STATE_NAMESPACE = "notification_state"


def _scope_key(sport_label: str) -> str:
    return (sport_label or "GLOBAL").strip().upper()


def _load_state(sport_label: str) -> dict[str, Any]:
    scope_key = _scope_key(sport_label)
    try:
        with SessionLocal() as db:
            row = (
                db.query(AppState)
                .filter(
                    AppState.namespace == NOTIFICATION_STATE_NAMESPACE,
                    AppState.state_key == scope_key,
                )
                .one_or_none()
            )
            if row is None:
                return {}
            payload = json.loads(row.value_json)
            if isinstance(payload, dict):
                return payload
    except Exception:
        return {}
    return {}


def _save_state(sport_label: str, payload: dict[str, Any]) -> None:
    scope_key = _scope_key(sport_label)
    encoded = json.dumps(payload, default=str)
    with SessionLocal() as db:
        row = (
            db.query(AppState)
            .filter(
                AppState.namespace == NOTIFICATION_STATE_NAMESPACE,
                AppState.state_key == scope_key,
            )
            .one_or_none()
        )
        if row is None:
            db.add(
                AppState(
                    namespace=NOTIFICATION_STATE_NAMESPACE,
                    state_key=scope_key,
                    value_json=encoded,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        else:
            row.value_json = encoded
            row.updated_at = datetime.now(timezone.utc)
        db.commit()


def get_notification_state(sport_label: str) -> dict[str, Any]:
    payload = _load_state(sport_label)
    return {
        "dismissed": payload.get("dismissed", {}),
        "snoozed_until": payload.get("snoozed_until", {}),
    }


def dismiss_notification(sport_label: str, notice_id: str) -> None:
    payload = get_notification_state(sport_label)
    dismissed = dict(payload.get("dismissed", {}))
    dismissed[notice_id] = datetime.now(timezone.utc).isoformat()
    payload["dismissed"] = dismissed
    _save_state(sport_label, payload)


def snooze_notification(sport_label: str, notice_id: str, hours: int = 24) -> None:
    payload = get_notification_state(sport_label)
    snoozed = dict(payload.get("snoozed_until", {}))
    snoozed[notice_id] = (datetime.now(timezone.utc) + timedelta(hours=hours)).isoformat()
    payload["snoozed_until"] = snoozed
    _save_state(sport_label, payload)


def reset_notification(sport_label: str, notice_id: str) -> None:
    payload = get_notification_state(sport_label)
    dismissed = dict(payload.get("dismissed", {}))
    snoozed = dict(payload.get("snoozed_until", {}))
    dismissed.pop(notice_id, None)
    snoozed.pop(notice_id, None)
    payload["dismissed"] = dismissed
    payload["snoozed_until"] = snoozed
    _save_state(sport_label, payload)


def is_notification_visible(sport_label: str, notice_id: str) -> bool:
    payload = get_notification_state(sport_label)
    if notice_id in payload.get("dismissed", {}):
        return False

    snoozed_until = payload.get("snoozed_until", {}).get(notice_id)
    if not snoozed_until:
        return True
    try:
        until = datetime.fromisoformat(str(snoozed_until).replace("Z", "+00:00"))
    except ValueError:
        return True
    return datetime.now(timezone.utc) >= until


def get_notification_history_rows(sport_label: str) -> list[dict[str, str]]:
    payload = get_notification_state(sport_label)
    rows: list[dict[str, str]] = []
    for notice_id, ts in payload.get("dismissed", {}).items():
        rows.append(
            {
                "notice_id": notice_id,
                "state": "dismissed",
                "timestamp": str(ts),
            }
        )
    for notice_id, ts in payload.get("snoozed_until", {}).items():
        rows.append(
            {
                "notice_id": notice_id,
                "state": "snoozed",
                "timestamp": str(ts),
            }
        )
    return sorted(rows, key=lambda item: item["timestamp"], reverse=True)
