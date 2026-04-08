from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import json

import pandas as pd

from builders.slips import format_dfs_slip


@dataclass(frozen=True)
class DfsSlipAdapter:
    key: str
    label: str
    brand_mark: str
    accent: str
    launch_url: str
    handoff_mode: str
    supports_public_prefill: bool
    supports_web_entry: bool
    submission_mode: str
    notes: str


DFS_SLIP_ADAPTERS: tuple[DfsSlipAdapter, ...] = (
    DfsSlipAdapter(
        key="prizepicks",
        label="PrizePicks",
        brand_mark="PP",
        accent="#f97316",
        launch_url="https://app.prizepicks.com/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=False,
        submission_mode="submit_on_destination",
        notes="Official app flow is available, but no public external prefill API was found. Use the generated slip payload after launch.",
    ),
    DfsSlipAdapter(
        key="underdog",
        label="Underdog Fantasy",
        brand_mark="UD",
        accent="#16a34a",
        launch_url="https://underdogfantasy.com/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=True,
        submission_mode="submit_on_destination",
        notes="Underdog supports Pick'em entry flow, but no public external prefill API was found. Use the generated slip payload after launch.",
    ),
    DfsSlipAdapter(
        key="chalkboard",
        label="Chalkboard",
        brand_mark="CB",
        accent="#8b5cf6",
        launch_url="https://chalkboard.io/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=False,
        submission_mode="submit_on_destination",
        notes="No public external prefill API was found. Use the generated slip payload after launch.",
    ),
    DfsSlipAdapter(
        key="betr",
        label="Betr Picks",
        brand_mark="BT",
        accent="#ec4899",
        launch_url="https://betr.app/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=False,
        submission_mode="submit_on_destination",
        notes="Betr Picks is supported in the adapter list, but no public external prefill API was found. Use the generated slip payload after launch.",
    ),
    DfsSlipAdapter(
        key="parlayplay",
        label="ParlayPlay",
        brand_mark="PY",
        accent="#f59e0b",
        launch_url="https://app.parlayplay.io/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=True,
        submission_mode="submit_on_destination",
        notes="ParlayPlay supports web entry, but no public external prefill API was found. Use the generated slip payload after launch.",
    ),
    DfsSlipAdapter(
        key="dabble",
        label="Dabble",
        brand_mark="DB",
        accent="#14b8a6",
        launch_url="https://www.dabble.com/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=True,
        submission_mode="submit_on_destination",
        notes="Dabble supports pick entries, but no public external prefill API was found. Use the generated slip payload after launch.",
    ),
    DfsSlipAdapter(
        key="draftkings_pick6",
        label="DraftKings Pick 6",
        brand_mark="DK",
        accent="#10b981",
        launch_url="https://pick6.draftkings.com/",
        handoff_mode="launch_and_copy",
        supports_public_prefill=False,
        supports_web_entry=True,
        submission_mode="submit_on_destination",
        notes="DraftKings Pick6 has an official web experience, but no public external prefill API was found. Use the generated slip payload after launch.",
    ),
)


def get_dfs_slip_adapters() -> list[dict]:
    return [asdict(adapter) for adapter in DFS_SLIP_ADAPTERS]


def recommend_dfs_slip_adapter(card_df: pd.DataFrame, style_label: str = "") -> dict:
    if card_df.empty:
        adapter = DFS_SLIP_ADAPTERS[0]
        return {
            "adapter": asdict(adapter),
            "reason": "Defaulted to the primary DFS adapter because no card legs were available.",
        }

    searchable_tokens: set[str] = set()
    for column in ["sportsbook", "book_key"]:
        if column in card_df.columns:
            searchable_tokens.update(
                str(value).strip().lower()
                for value in card_df[column].dropna().tolist()
                if str(value).strip()
            )

    for adapter in DFS_SLIP_ADAPTERS:
        adapter_tokens = {
            adapter.key.lower(),
            adapter.label.lower(),
            adapter.label.lower().replace(" fantasy", ""),
            adapter.label.lower().replace(" ", ""),
        }
        if any(any(token in source for token in adapter_tokens) for source in searchable_tokens):
            return {
                "adapter": asdict(adapter),
                "reason": f"Recommended because the current card already references {adapter.label} as the underlying DFS source.",
            }

    safe_style = str(style_label).strip().lower()
    if safe_style == "safe":
        adapter = next(item for item in DFS_SLIP_ADAPTERS if item.key == "prizepicks")
        return {
            "adapter": asdict(adapter),
            "reason": "Recommended as the default safe-profile destination for a clean pick'em handoff.",
        }
    if safe_style == "aggressive":
        adapter = next(item for item in DFS_SLIP_ADAPTERS if item.key == "underdog")
        return {
            "adapter": asdict(adapter),
            "reason": "Recommended as the default aggressive-profile destination when no source-specific app match is detected.",
        }

    adapter = next(item for item in DFS_SLIP_ADAPTERS if item.key == "draftkings_pick6")
    return {
        "adapter": asdict(adapter),
        "reason": "Recommended as the default balanced web-entry destination when no source-specific app match is detected.",
    }


def _normalize_leg_records(card_df: pd.DataFrame) -> list[dict]:
    normalized: list[dict] = []
    working = card_df.copy()
    if "card_slot" not in working.columns and "leg_rank" in working.columns:
        working["card_slot"] = working["leg_rank"]

    for _, row in working.iterrows():
        normalized.append(
            {
                "slot": int(row.get("card_slot", row.get("leg_rank", 0)) or 0),
                "player": str(row.get("player_display") or row.get("player") or "").strip(),
                "team": str(row.get("player_team") or row.get("team") or "").strip(),
                "market": str(row.get("market") or "").strip(),
                "pick": str(row.get("pick") or "").strip(),
                "line": None if pd.isna(row.get("line")) else float(row.get("line")),
                "confidence": None if pd.isna(row.get("confidence")) else float(row.get("confidence")),
                "projection": (
                    None
                    if pd.isna(row.get("projection")) and pd.isna(row.get("predicted_value"))
                    else float(row.get("projection"))
                    if pd.notna(row.get("projection"))
                    else float(row.get("predicted_value"))
                ),
                "sportsbook": str(row.get("sportsbook") or "").strip(),
                "book_key": str(row.get("book_key") or "").strip(),
                "event_id": str(row.get("event_id") or "").strip(),
            }
        )
    return normalized


def build_dfs_slip_payload(
    card_df: pd.DataFrame,
    adapter_key: str,
    sport_label: str,
    source_label: str,
    style_label: str,
) -> dict:
    adapter = next((item for item in DFS_SLIP_ADAPTERS if item.key == adapter_key), DFS_SLIP_ADAPTERS[0])
    payload = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "adapter_key": adapter.key,
        "adapter_label": adapter.label,
        "launch_url": adapter.launch_url,
        "handoff_mode": adapter.handoff_mode,
        "supports_public_prefill": adapter.supports_public_prefill,
        "supports_web_entry": adapter.supports_web_entry,
        "sport_label": sport_label,
        "source_label": source_label,
        "style_label": style_label,
        "leg_count": int(len(card_df)),
        "legs": _normalize_leg_records(card_df),
    }
    return payload


def format_dfs_slip_payload(payload: dict) -> str:
    lines = [
        f"{payload['adapter_label']} auto-slip",
        "-" * 32,
        f"Sport: {payload['sport_label']}",
        f"Source: {payload['source_label']}",
        f"Style: {payload['style_label']}",
        f"Legs: {payload['leg_count']}",
        "",
    ]
    for leg in payload.get("legs", []):
        line_label = "" if leg["line"] is None else f" {leg['line']}"
        team_label = f" ({leg['team']})" if leg.get("team") else ""
        confidence_label = "" if leg["confidence"] is None else f" | conf {leg['confidence']:.1f}"
        lines.append(
            f"{leg['slot']}. {leg['player']}{team_label} | {leg['pick']}{line_label} {leg['market']}{confidence_label}".strip()
        )
    return "\n".join(lines)


def format_dfs_slip_json(payload: dict) -> str:
    return json.dumps(payload, indent=2)


def format_dfs_slip_text(card_df: pd.DataFrame, app_name: str) -> str:
    working = card_df.copy()
    if "card_slot" not in working.columns and "leg_rank" in working.columns:
        working["card_slot"] = working["leg_rank"]
    return format_dfs_slip(working, app_name)
