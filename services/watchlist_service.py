from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from typing import Any

import pandas as pd

from db.models import AppState
from db.session import SessionLocal


WATCHLIST_NAMESPACE = "watchlist"
WATCHLIST_ALERT_NAMESPACE = "watchlist_alerts"


def _scope_key(sport_label: str) -> str:
    return (sport_label or "GLOBAL").strip().upper()


def _normalize_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, float):
        return round(float(value), 4)
    return value


def _entry_identity(entry: dict[str, Any]) -> str:
    identity_payload = {
        "sport_label": entry.get("sport_label"),
        "external_event_id": entry.get("external_event_id"),
        "player": entry.get("player"),
        "market": entry.get("market"),
        "pick": entry.get("pick"),
        "sportsbook": entry.get("sportsbook"),
        "line": _normalize_value(entry.get("line")),
        "side": entry.get("side"),
    }
    encoded = json.dumps(identity_payload, sort_keys=True, default=str)
    return hashlib.sha1(encoded.encode("utf-8")).hexdigest()


def _serialize_row(row: pd.Series, sport_label: str) -> dict[str, Any]:
    entry = {
        "sport_label": sport_label,
        "external_event_id": _normalize_value(row.get("external_event_id")),
        "player": _normalize_value(row.get("player")),
        "market": _normalize_value(row.get("market")),
        "pick": _normalize_value(row.get("pick")),
        "sportsbook": _normalize_value(row.get("sportsbook")),
        "bookmaker_key": _normalize_value(row.get("bookmaker_key")),
        "line": _normalize_value(row.get("line")),
        "side": _normalize_value(row.get("side")),
        "price": _normalize_value(row.get("price")),
        "coverage_status": _normalize_value(row.get("coverage_status")),
        "coverage_note": _normalize_value(row.get("coverage_note")),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    entry["watchlist_key"] = _entry_identity(entry)
    return entry


def _load_entries(sport_label: str) -> list[dict[str, Any]]:
    scope_key = _scope_key(sport_label)
    try:
        with SessionLocal() as db:
            row = (
                db.query(AppState)
                .filter(
                    AppState.namespace == WATCHLIST_NAMESPACE,
                    AppState.state_key == scope_key,
                )
                .one_or_none()
            )
            if row is None:
                return []
            payload = json.loads(row.value_json)
            if isinstance(payload, list):
                return [entry for entry in payload if isinstance(entry, dict)]
    except Exception:
        return []
    return []


def _save_entries(sport_label: str, entries: list[dict[str, Any]]) -> None:
    scope_key = _scope_key(sport_label)
    encoded = json.dumps(entries, default=str)
    with SessionLocal() as db:
        row = (
            db.query(AppState)
            .filter(
                AppState.namespace == WATCHLIST_NAMESPACE,
                AppState.state_key == scope_key,
            )
            .one_or_none()
        )
        if row is None:
            db.add(
                AppState(
                    namespace=WATCHLIST_NAMESPACE,
                    state_key=scope_key,
                    value_json=encoded,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        else:
            row.value_json = encoded
            row.updated_at = datetime.now(timezone.utc)
        db.commit()


def _load_alert_settings(sport_label: str) -> dict[str, Any]:
    scope_key = _scope_key(sport_label)
    try:
        with SessionLocal() as db:
            row = (
                db.query(AppState)
                .filter(
                    AppState.namespace == WATCHLIST_ALERT_NAMESPACE,
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


def _save_alert_settings(sport_label: str, payload: dict[str, Any]) -> None:
    scope_key = _scope_key(sport_label)
    encoded = json.dumps(payload, default=str)
    with SessionLocal() as db:
        row = (
            db.query(AppState)
            .filter(
                AppState.namespace == WATCHLIST_ALERT_NAMESPACE,
                AppState.state_key == scope_key,
            )
            .one_or_none()
        )
        if row is None:
            db.add(
                AppState(
                    namespace=WATCHLIST_ALERT_NAMESPACE,
                    state_key=scope_key,
                    value_json=encoded,
                    updated_at=datetime.now(timezone.utc),
                )
            )
        else:
            row.value_json = encoded
            row.updated_at = datetime.now(timezone.utc)
        db.commit()


def get_watchlist_entries(sport_label: str) -> list[dict[str, Any]]:
    return _load_entries(sport_label)


def get_watchlist_alert_settings(sport_label: str) -> dict[str, Any]:
    settings = _load_alert_settings(sport_label)
    return {
        "min_edge_pct": float(settings.get("min_edge_pct", 5.0)),
        "min_confidence": float(settings.get("min_confidence", 60.0)),
    }


def save_watchlist_alert_settings(sport_label: str, min_edge_pct: float, min_confidence: float) -> None:
    _save_alert_settings(
        sport_label,
        {
            "min_edge_pct": round(float(min_edge_pct), 2),
            "min_confidence": round(float(min_confidence), 2),
        },
    )


def get_watchlist_df(sport_label: str) -> pd.DataFrame:
    entries = _load_entries(sport_label)
    if not entries:
        return pd.DataFrame()
    watchlist_df = pd.DataFrame(entries)
    sort_columns = [col for col in ["created_at", "player", "market"] if col in watchlist_df.columns]
    if "created_at" in watchlist_df.columns:
        watchlist_df = watchlist_df.sort_values("created_at", ascending=False)
    elif sort_columns:
        watchlist_df = watchlist_df.sort_values(sort_columns)
    return watchlist_df


def add_watchlist_rows(df: pd.DataFrame, row_indices: list[int], sport_label: str) -> int:
    if df.empty or not row_indices:
        return 0

    existing = _load_entries(sport_label)
    existing_map = {entry["watchlist_key"]: entry for entry in existing if entry.get("watchlist_key")}
    added = 0

    for row_index in row_indices:
        if row_index not in df.index:
            continue
        entry = _serialize_row(df.loc[row_index], sport_label=sport_label)
        if entry["watchlist_key"] in existing_map:
            continue
        existing_map[entry["watchlist_key"]] = entry
        added += 1

    _save_entries(sport_label, list(existing_map.values()))
    return added


def remove_watchlist_keys(sport_label: str, watchlist_keys: list[str]) -> int:
    if not watchlist_keys:
        return 0
    existing = _load_entries(sport_label)
    remaining = [entry for entry in existing if entry.get("watchlist_key") not in set(watchlist_keys)]
    removed = len(existing) - len(remaining)
    _save_entries(sport_label, remaining)
    return removed


def annotate_watchlist(df: pd.DataFrame, sport_label: str) -> pd.DataFrame:
    if df.empty:
        return df

    annotated = df.copy()
    watchlist_keys = {entry.get("watchlist_key") for entry in _load_entries(sport_label)}
    if not watchlist_keys:
        annotated["watchlist_key"] = ""
        annotated["is_watchlisted"] = False
        return annotated

    derived_keys = []
    is_watchlisted = []
    for _, row in annotated.iterrows():
        entry = _serialize_row(row, sport_label=sport_label)
        watchlist_key = entry["watchlist_key"]
        derived_keys.append(watchlist_key)
        is_watchlisted.append(watchlist_key in watchlist_keys)

    annotated["watchlist_key"] = derived_keys
    annotated["is_watchlisted"] = is_watchlisted
    return annotated


def get_watchlist_alerts(edge_df: pd.DataFrame, sport_label: str) -> pd.DataFrame:
    if edge_df.empty:
        return edge_df

    settings = get_watchlist_alert_settings(sport_label)
    alerts_df = edge_df.copy()
    if "is_watchlisted" in alerts_df.columns:
        alerts_df = alerts_df[alerts_df["is_watchlisted"]].copy()
    if alerts_df.empty:
        return alerts_df

    if "edge" in alerts_df.columns:
        alerts_df = alerts_df[alerts_df["edge"].fillna(0.0) * 100 >= settings["min_edge_pct"]].copy()
    if alerts_df.empty:
        return alerts_df
    if "confidence" in alerts_df.columns:
        alerts_df = alerts_df[alerts_df["confidence"].fillna(0.0) >= settings["min_confidence"]].copy()
    return alerts_df.sort_values(["confidence", "edge"], ascending=False)


def annotate_watchlist_movement(df: pd.DataFrame, sport_label: str) -> pd.DataFrame:
    if df.empty:
        return df

    annotated = annotate_watchlist(df, sport_label)
    watchlist_entries = {
        entry.get("watchlist_key"): entry
        for entry in _load_entries(sport_label)
        if entry.get("watchlist_key")
    }
    if not watchlist_entries:
        annotated["saved_line"] = None
        annotated["saved_price"] = None
        annotated["line_move"] = None
        annotated["price_move"] = None
        return annotated

    saved_lines = []
    saved_prices = []
    line_moves = []
    price_moves = []
    line_move_labels = []
    price_move_labels = []
    for _, row in annotated.iterrows():
        entry = watchlist_entries.get(row.get("watchlist_key"))
        if not entry:
            saved_lines.append(None)
            saved_prices.append(None)
            line_moves.append(None)
            price_moves.append(None)
            line_move_labels.append("")
            price_move_labels.append("")
            continue

        saved_line = entry.get("line")
        saved_price = entry.get("price")
        current_line = _normalize_value(row.get("line"))
        current_price = _normalize_value(row.get("price"))
        pick_text = str(row.get("pick") or "").lower()
        saved_lines.append(saved_line)
        saved_prices.append(saved_price)
        line_move = (
            round(float(current_line) - float(saved_line), 3)
            if saved_line is not None and current_line is not None
            else None
        )
        price_move = (
            round(float(current_price) - float(saved_price), 3)
            if saved_price is not None and current_price is not None
            else None
        )
        line_moves.append(line_move)
        price_moves.append(price_move)

        if line_move is None or abs(line_move) < 0.001:
            line_move_labels.append("neutral")
        elif "under" in pick_text:
            line_move_labels.append("better" if line_move > 0 else "worse")
        else:
            line_move_labels.append("better" if line_move < 0 else "worse")

        if price_move is None or abs(price_move) < 0.001:
            price_move_labels.append("neutral")
        else:
            price_move_labels.append("better" if price_move > 0 else "worse")

    annotated["saved_line"] = saved_lines
    annotated["saved_price"] = saved_prices
    annotated["line_move"] = line_moves
    annotated["price_move"] = price_moves
    annotated["line_move_label"] = line_move_labels
    annotated["price_move_label"] = price_move_labels
    return annotated
