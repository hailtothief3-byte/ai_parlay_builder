from __future__ import annotations

import pandas as pd

from services.ticket_service import get_ticket_legs


def _round_confidence(value: float | None, fallback: int) -> int:
    if value is None or pd.isna(value):
        return fallback
    return int(min(95, max(50, round(float(value) / 5.0) * 5)))


def _ticket_status_score(status: str) -> float:
    normalized = str(status or "").strip().lower()
    if normalized == "won":
        return 1.0
    if normalized == "push":
        return 0.5
    if normalized == "lost":
        return 0.0
    return float("nan")


def _infer_demo_style(leg_count: int | float | None, avg_confidence: float | None) -> str:
    legs = int(leg_count or 0)
    confidence = float(avg_confidence) if avg_confidence is not None and not pd.isna(avg_confidence) else 0.0
    if legs <= 3 and confidence >= 75:
        return "Safe"
    if legs >= 5 or confidence < 68:
        return "Aggressive"
    return "Balanced"


def _ticket_has_same_player(ticket_id: int) -> bool:
    legs = get_ticket_legs(int(ticket_id))
    if legs.empty or "player" not in legs.columns:
        return False
    players = legs["player"].dropna().astype(str).str.strip()
    return players.duplicated().any()


def _resolve_live_profile(ticket_summary_df: pd.DataFrame) -> dict[str, object]:
    live_tickets = ticket_summary_df[ticket_summary_df["source"] == "live_edges"].copy()
    if live_tickets.empty:
        return {
            "sample_size": 0,
            "recommended_legs": 3,
            "recommended_min_confidence": 65,
            "recommended_same_player": False,
            "reason": "No settled live tickets yet. Using balanced defaults until more live history is graded.",
        }

    live_tickets["status_score"] = live_tickets["ticket_status_live"].map(_ticket_status_score)
    resolved_live = live_tickets[live_tickets["status_score"].notna()].copy()
    if resolved_live.empty:
        return {
            "sample_size": int(len(live_tickets)),
            "recommended_legs": int(live_tickets["leg_count"].mode().iloc[0]) if "leg_count" in live_tickets.columns and not live_tickets["leg_count"].dropna().empty else 3,
            "recommended_min_confidence": _round_confidence(live_tickets["avg_confidence"].median() if "avg_confidence" in live_tickets.columns else None, 65),
            "recommended_same_player": False,
            "reason": "Live tickets have been saved, but not enough of them are settled yet. Leaning on your recent ticket-building tendencies for now.",
        }

    live_leg_source = "build_min_confidence" if "build_min_confidence" in resolved_live.columns else "avg_confidence"
    leg_summary = (
        resolved_live.groupby("leg_count", observed=False)
        .agg(
            sample_size=("ticket_id", "count"),
            avg_score=("status_score", "mean"),
            avg_ticket_outcome=("ticket_outcome_score", "mean"),
        )
        .reset_index()
        .sort_values(["avg_ticket_outcome", "avg_score", "sample_size"], ascending=[False, False, False])
    )
    recommended_legs = int(leg_summary.iloc[0]["leg_count"]) if not leg_summary.empty else 3

    best_leg_tickets = resolved_live[resolved_live["leg_count"] == recommended_legs].copy()
    recommended_min_confidence = _round_confidence(
        best_leg_tickets[live_leg_source].median() if live_leg_source in best_leg_tickets.columns and not best_leg_tickets[live_leg_source].dropna().empty else resolved_live.get(live_leg_source, resolved_live.get("avg_confidence", pd.Series(dtype=float))).median(),
        65,
    )
    same_player_df = pd.DataFrame(
        [
            {
                "ticket_id": int(ticket_id),
                "has_same_player": (
                    bool(ticket_row.get("build_allow_same_player", False))
                    if "build_allow_same_player" in resolved_live.columns and pd.notna(ticket_row.get("build_allow_same_player"))
                    else _ticket_has_same_player(int(ticket_id))
                ),
            }
            for ticket_id, ticket_row in resolved_live.set_index("ticket_id").iterrows()
        ]
    )
    duplicate_penalty = False
    if not same_player_df.empty:
        duplicate_merged = resolved_live.merge(same_player_df, on="ticket_id", how="left")
        duplicate_summary = (
            duplicate_merged.groupby("has_same_player", observed=False)
            .agg(sample_size=("ticket_id", "count"), avg_score=("status_score", "mean"))
            .reset_index()
        )
        duplicate_true = duplicate_summary[duplicate_summary["has_same_player"] == True]
        duplicate_false = duplicate_summary[duplicate_summary["has_same_player"] == False]
        if not duplicate_true.empty and not duplicate_false.empty:
            duplicate_penalty = (
                int(duplicate_true.iloc[0]["sample_size"]) >= 2
                and float(duplicate_true.iloc[0]["avg_score"]) + 0.05 < float(duplicate_false.iloc[0]["avg_score"])
            )

    leg_sample = int(leg_summary.iloc[0]["sample_size"]) if not leg_summary.empty else 0
    leg_score = float(leg_summary.iloc[0]["avg_score"]) if not leg_summary.empty else 0.0
    leg_outcome = float(leg_summary.iloc[0]["avg_ticket_outcome"]) if not leg_summary.empty and pd.notna(leg_summary.iloc[0]["avg_ticket_outcome"]) else leg_score
    reason = (
        f"Settled live tickets lean toward {recommended_legs} legs with an average ticket outcome score of {leg_outcome:.2f} across {leg_sample} tickets. "
        f"The strongest ticket cluster also centers around about {recommended_min_confidence} confidence."
    )
    if duplicate_penalty:
        reason += " Repeated same-player exposure has lagged cleaner ticket builds in your settled history, so the profile recommends keeping that blocked."
    else:
        reason += " Your settled history does not currently show a strong enough same-player penalty to force aggressive filtering."

    return {
        "sample_size": int(len(resolved_live)),
        "recommended_legs": recommended_legs,
        "recommended_min_confidence": recommended_min_confidence,
        "recommended_same_player": not duplicate_penalty,
        "reason": reason,
    }


def _resolve_demo_profile(ticket_summary_df: pd.DataFrame) -> dict[str, object]:
    demo_tickets = ticket_summary_df[ticket_summary_df["source"] == "demo_predictions"].copy()
    if demo_tickets.empty:
        return {
            "sample_size": 0,
            "recommended_legs": 3,
            "recommended_min_confidence": 70,
            "recommended_style": "Balanced",
            "recommended_same_team": False,
            "reason": "No saved demo tickets yet. Using balanced defaults until more demo build history exists.",
        }

    demo_tickets["inferred_style"] = demo_tickets.apply(
        lambda row: str(row.get("build_style") or "").strip() or _infer_demo_style(row.get("leg_count"), row.get("avg_confidence")),
        axis=1,
    )
    style_summary = (
        demo_tickets.groupby("inferred_style", observed=False)
        .agg(sample_size=("ticket_id", "count"), avg_confidence=("avg_confidence", "mean"), avg_legs=("leg_count", "mean"))
        .reset_index()
        .sort_values(["sample_size", "avg_confidence"], ascending=[False, False])
    )
    recommended_style = str(style_summary.iloc[0]["inferred_style"]) if not style_summary.empty else "Balanced"
    recommended_legs = int(round(float(style_summary.iloc[0]["avg_legs"]))) if not style_summary.empty and pd.notna(style_summary.iloc[0]["avg_legs"]) else 3
    preferred_style_tickets = demo_tickets[demo_tickets["inferred_style"] == recommended_style].copy()
    preferred_conf_series = (
        preferred_style_tickets["build_min_confidence"]
        if "build_min_confidence" in preferred_style_tickets.columns and not preferred_style_tickets["build_min_confidence"].dropna().empty
        else preferred_style_tickets.get("avg_confidence", pd.Series(dtype=float))
    )
    recommended_min_confidence = _round_confidence(preferred_conf_series.median() if not preferred_conf_series.empty else None, 70)
    same_team_allowed = False
    if "build_allow_same_team" in preferred_style_tickets.columns and not preferred_style_tickets["build_allow_same_team"].dropna().empty:
        same_team_allowed = bool(preferred_style_tickets["build_allow_same_team"].mode().iloc[0])
    return {
        "sample_size": int(len(demo_tickets)),
        "recommended_legs": max(2, min(6, recommended_legs)),
        "recommended_min_confidence": recommended_min_confidence,
        "recommended_style": recommended_style,
        "recommended_same_team": same_team_allowed,
        "reason": (
            f"Your saved demo builds most often resemble a {recommended_style.lower()} profile, centered around "
            f"{recommended_legs} legs and roughly {recommended_min_confidence} confidence. "
            "This is based on build tendencies rather than settled demo outcomes."
        ),
    }


def _resolve_dfs_preference(ticket_summary_df: pd.DataFrame) -> dict[str, object]:
    dfs_tickets = ticket_summary_df[ticket_summary_df["dfs_target_key"].astype(str).str.strip() != ""].copy() if "dfs_target_key" in ticket_summary_df.columns else pd.DataFrame()
    if dfs_tickets.empty:
        return {
            "sample_size": 0,
            "recommended_target_key": "",
            "recommended_target_label": "",
            "reason": "No DFS destination history yet.",
        }

    dfs_tickets["status_score"] = dfs_tickets["ticket_status_live"].map(_ticket_status_score)
    grouped = (
        dfs_tickets.groupby(["dfs_target_key", "dfs_target_app"], observed=False)
        .agg(sample_size=("ticket_id", "count"), avg_score=("status_score", "mean"))
        .reset_index()
        .sort_values(["avg_score", "sample_size"], ascending=[False, False])
    )
    top_row = grouped.iloc[0]
    return {
        "sample_size": int(len(dfs_tickets)),
        "recommended_target_key": str(top_row["dfs_target_key"]),
        "recommended_target_label": str(top_row["dfs_target_app"]),
        "reason": f"DFS ticket history currently leans toward {top_row['dfs_target_app']} based on saved usage and settled performance where available.",
    }


def build_smart_parlay_profiles(ticket_summary_df: pd.DataFrame) -> dict[str, dict[str, object]]:
    if ticket_summary_df.empty:
        return {
            "live": _resolve_live_profile(ticket_summary_df),
            "demo": _resolve_demo_profile(ticket_summary_df),
            "dfs": _resolve_dfs_preference(ticket_summary_df),
        }

    working = ticket_summary_df.copy()
    if "dfs_target_key" not in working.columns:
        working["dfs_target_key"] = ""
    if "dfs_target_app" not in working.columns:
        working["dfs_target_app"] = ""

    return {
        "live": _resolve_live_profile(working),
        "demo": _resolve_demo_profile(working),
        "dfs": _resolve_dfs_preference(working),
    }
