from __future__ import annotations

import pandas as pd


CONFIDENCE_BUCKET_BINS = [0, 60, 70, 80, 90, 100]
CONFIDENCE_BUCKET_LABELS = ["0-60", "61-70", "71-80", "81-90", "91-100"]
PRIOR_WEIGHT = 12.0
DEFAULT_WEIGHT_PROFILE = {
    "model_score_weight": 0.42,
    "confidence_score_weight": 0.28,
    "edge_multiplier": 1.45,
    "books_multiplier": 0.85,
    "line_delta_multiplier": 0.80,
    "history_market_weight": 0.36,
    "history_confidence_weight": 0.30,
    "history_sportsbook_weight": 0.18,
    "recent_market_weight": 0.18,
    "recent_sportsbook_weight": 0.12,
    "market_roi_multiplier": 10.0,
    "sportsbook_roi_multiplier": 6.0,
    "recent_market_roi_multiplier": 8.0,
    "recent_sportsbook_roi_multiplier": 4.5,
    "expected_model_weight": 0.58,
    "expected_market_weight": 0.22,
    "expected_confidence_weight": 0.14,
    "expected_sportsbook_weight": 0.06,
}


def _empty_summary() -> dict[str, float | int]:
    return {
        "history_picks": 0,
        "overall_hit_rate": 0.0,
        "overall_roi_per_pick": 0.0,
        "overall_avg_model_prob": 0.0,
    }


def _coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    coerced = df.copy()
    for column in columns:
        if column in coerced.columns:
            coerced[column] = pd.to_numeric(coerced[column], errors="coerce")
    return coerced


def _clip(value: float, lower: float, upper: float) -> float:
    return max(lower, min(value, upper))


def _safe_std(series: pd.Series) -> float:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric) <= 1:
        return 0.0
    return float(numeric.std())


def _build_segment_summary(history: pd.DataFrame, group_cols: list[str], prefix: str) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()

    summary = (
        history.groupby(group_cols, observed=False)
        .agg(
            picks=("won", "count"),
            hit_rate=("won", "mean"),
            roi_per_pick=("profit_units", "mean"),
            avg_model_prob=("model_prob", "mean"),
            avg_confidence=("confidence", "mean"),
        )
        .reset_index()
    )

    rename_map = {
        "picks": f"{prefix}_picks",
        "hit_rate": f"{prefix}_hit_rate",
        "roi_per_pick": f"{prefix}_roi_per_pick",
        "avg_model_prob": f"{prefix}_avg_model_prob",
        "avg_confidence": f"{prefix}_avg_confidence",
    }
    return summary.rename(columns=rename_map)


def _blend_metric(
    value_series: pd.Series,
    picks_series: pd.Series,
    overall_value: float,
    prior_weight: float = PRIOR_WEIGHT,
) -> pd.Series:
    picks = pd.to_numeric(picks_series, errors="coerce").fillna(0.0)
    values = pd.to_numeric(value_series, errors="coerce").fillna(overall_value)
    return ((values * picks) + (overall_value * prior_weight)) / (picks + prior_weight)


def _confidence_bucket_from_series(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    bucket = pd.cut(
        numeric,
        bins=CONFIDENCE_BUCKET_BINS,
        labels=CONFIDENCE_BUCKET_LABELS,
        include_lowest=True,
    )
    return bucket.astype(str).replace("nan", "")


def build_smart_pick_profile(graded_df: pd.DataFrame) -> dict[str, object]:
    if graded_df.empty:
        return {
            "summary": _empty_summary(),
            "market_summary": pd.DataFrame(),
            "sportsbook_summary": pd.DataFrame(),
            "confidence_summary": pd.DataFrame(),
            "recent_market_summary": pd.DataFrame(),
            "recent_sportsbook_summary": pd.DataFrame(),
        }

    history = _coerce_numeric(
        graded_df,
        ["won", "profit_units", "model_prob", "confidence", "edge"],
    ).copy()
    if "won" not in history.columns:
        history["won"] = 0
    if "profit_units" not in history.columns:
        history["profit_units"] = 0.0
    history["confidence_bucket"] = _confidence_bucket_from_series(
        history["confidence"] if "confidence" in history.columns else pd.Series(index=history.index, dtype=float)
    )
    recent_history = history.copy()
    if "resolved_at" in recent_history.columns:
        recent_history["resolved_at"] = pd.to_datetime(recent_history["resolved_at"], errors="coerce", utc=True)
        recent_history = recent_history.sort_values("resolved_at", ascending=False)
    recent_history = recent_history.head(min(28, len(recent_history)))

    history = history.dropna(subset=["player"], how="all")
    recent_history = recent_history.dropna(subset=["player"], how="all")

    overall_hit_rate = float(history["won"].mean()) if not history.empty else 0.0
    overall_roi_per_pick = float(history["profit_units"].mean()) if not history.empty else 0.0
    overall_avg_model_prob = float(history["model_prob"].mean()) if "model_prob" in history.columns and not history["model_prob"].dropna().empty else 0.0

    return {
        "summary": {
            "history_picks": int(len(history)),
            "overall_hit_rate": overall_hit_rate,
            "overall_roi_per_pick": overall_roi_per_pick,
            "overall_avg_model_prob": overall_avg_model_prob,
        },
        "market_summary": _build_segment_summary(history, ["market"], "market"),
        "sportsbook_summary": _build_segment_summary(history, ["sportsbook"], "sportsbook"),
        "confidence_summary": _build_segment_summary(history, ["confidence_bucket"], "confidence_bucket"),
        "recent_market_summary": _build_segment_summary(recent_history, ["market"], "recent_market"),
        "recent_sportsbook_summary": _build_segment_summary(recent_history, ["sportsbook"], "recent_sportsbook"),
    }


def build_smart_weight_profile(graded_df: pd.DataFrame) -> dict[str, float | int | str]:
    profile = DEFAULT_WEIGHT_PROFILE.copy()
    if graded_df.empty:
        return {
            **profile,
            "history_picks": 0,
            "profile_mode": "default",
            "profile_reason": "No graded pick history yet, so the smart engine is using the default balance between model signal and historical memory.",
        }

    smart_profile = build_smart_pick_profile(graded_df)
    summary = smart_profile["summary"]
    history_picks = int(summary["history_picks"])
    overall_hit_rate = float(summary["overall_hit_rate"])
    overall_roi_per_pick = float(summary["overall_roi_per_pick"])
    overall_avg_model_prob = float(summary["overall_avg_model_prob"])
    calibration_gap = overall_hit_rate - overall_avg_model_prob
    history_strength = _clip((history_picks - 8) / 36.0, 0.0, 1.0)

    market_signal = _safe_std(smart_profile["market_summary"].get("market_hit_rate", pd.Series(dtype=float))) if not smart_profile["market_summary"].empty else 0.0
    sportsbook_signal = _safe_std(smart_profile["sportsbook_summary"].get("sportsbook_hit_rate", pd.Series(dtype=float))) if not smart_profile["sportsbook_summary"].empty else 0.0
    confidence_signal = _safe_std(smart_profile["confidence_summary"].get("confidence_bucket_hit_rate", pd.Series(dtype=float))) if not smart_profile["confidence_summary"].empty else 0.0
    recent_market_signal = _safe_std(smart_profile["recent_market_summary"].get("recent_market_hit_rate", pd.Series(dtype=float))) if not smart_profile["recent_market_summary"].empty else 0.0
    recent_sportsbook_signal = _safe_std(smart_profile["recent_sportsbook_summary"].get("recent_sportsbook_hit_rate", pd.Series(dtype=float))) if not smart_profile["recent_sportsbook_summary"].empty else 0.0

    profile["model_score_weight"] = round(_clip(0.42 + (calibration_gap * 0.90) - (history_strength * 0.05), 0.28, 0.52), 3)
    profile["confidence_score_weight"] = round(_clip(0.28 + (confidence_signal * 0.35), 0.20, 0.38), 3)
    profile["edge_multiplier"] = round(_clip(1.45 + (overall_roi_per_pick * 0.55), 1.00, 1.95), 3)
    profile["history_market_weight"] = round(_clip(0.24 + (history_strength * 0.14) + (market_signal * 0.70), 0.18, 0.52), 3)
    profile["history_confidence_weight"] = round(_clip(0.22 + (history_strength * 0.10) + (confidence_signal * 0.55), 0.16, 0.42), 3)
    profile["history_sportsbook_weight"] = round(_clip(0.12 + (history_strength * 0.08) + (sportsbook_signal * 0.60), 0.08, 0.30), 3)
    profile["recent_market_weight"] = round(_clip(0.10 + (history_strength * 0.04) + (recent_market_signal * 0.55), 0.08, 0.26), 3)
    profile["recent_sportsbook_weight"] = round(_clip(0.08 + (history_strength * 0.03) + (recent_sportsbook_signal * 0.40), 0.06, 0.18), 3)
    profile["market_roi_multiplier"] = round(_clip(8.0 + (history_strength * 3.5) + (max(overall_roi_per_pick, 0.0) * 2.0), 6.0, 14.0), 3)
    profile["sportsbook_roi_multiplier"] = round(_clip(4.5 + (history_strength * 2.0) + (sportsbook_signal * 4.0), 3.0, 8.5), 3)
    profile["recent_market_roi_multiplier"] = round(_clip(5.0 + (history_strength * 2.0) + (recent_market_signal * 4.0), 4.0, 10.0), 3)
    profile["recent_sportsbook_roi_multiplier"] = round(_clip(3.0 + (history_strength * 1.3) + (recent_sportsbook_signal * 3.0), 2.4, 6.5), 3)
    profile["expected_model_weight"] = round(_clip(0.58 + (calibration_gap * 0.50) - (history_strength * 0.04), 0.46, 0.68), 3)
    profile["expected_market_weight"] = round(_clip(0.18 + (history_strength * 0.06) + (market_signal * 0.28), 0.14, 0.30), 3)
    profile["expected_confidence_weight"] = round(_clip(0.12 + (history_strength * 0.04) + (confidence_signal * 0.22), 0.10, 0.24), 3)
    derived_weight_sum = profile["expected_model_weight"] + profile["expected_market_weight"] + profile["expected_confidence_weight"]
    profile["expected_sportsbook_weight"] = round(max(0.04, 1.0 - derived_weight_sum), 3)

    if history_picks < 12:
        mode = "default"
        reason = "History is still small, so the smart engine is staying close to its default weighting profile."
    elif history_picks < 35:
        mode = "lightly_tuned"
        reason = "The engine is lightly increasing history-aware weights where your graded results show meaningful separation by market, confidence, or sportsbook."
    else:
        mode = "history_informed"
        reason = "The engine has enough graded history to lean more on the patterns that have actually held up in your tracked results."

    return {
        **profile,
        "history_picks": history_picks,
        "calibration_gap": round(calibration_gap, 4),
        "market_signal": round(market_signal, 4),
        "sportsbook_signal": round(sportsbook_signal, 4),
        "confidence_signal": round(confidence_signal, 4),
        "recent_market_signal": round(recent_market_signal, 4),
        "recent_sportsbook_signal": round(recent_sportsbook_signal, 4),
        "profile_mode": mode,
        "profile_reason": reason,
    }


def apply_smart_weight_overrides(base_profile: dict[str, float | int | str], override_profile: dict[str, float] | None = None) -> dict[str, float | int | str]:
    resolved = dict(base_profile)
    if not override_profile:
        return resolved

    for key, value in override_profile.items():
        if key in resolved and value is not None:
            resolved[key] = float(value)

    resolved["profile_mode"] = "manual_override"
    resolved["profile_reason"] = "Manual smart-engine overrides are active, so scoring is using your preferred weighting mix instead of the pure auto-tuned profile."
    return resolved


def build_smart_learning_tables(graded_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    profile = build_smart_pick_profile(graded_df)
    weight_profile = build_smart_weight_profile(graded_df)

    market_summary = profile["market_summary"].copy()
    sportsbook_summary = profile["sportsbook_summary"].copy()
    confidence_summary = profile["confidence_summary"].copy()
    recent_market_summary = profile.get("recent_market_summary", pd.DataFrame()).copy()
    recent_sportsbook_summary = profile.get("recent_sportsbook_summary", pd.DataFrame()).copy()

    if not market_summary.empty:
        market_summary = market_summary.sort_values(
            ["market_roi_per_pick", "market_hit_rate", "market_picks"],
            ascending=[False, False, False],
        )
    if not sportsbook_summary.empty:
        sportsbook_summary = sportsbook_summary.sort_values(
            ["sportsbook_roi_per_pick", "sportsbook_hit_rate", "sportsbook_picks"],
            ascending=[False, False, False],
        )
    if not confidence_summary.empty:
        confidence_summary = confidence_summary.sort_values(
            ["confidence_bucket_hit_rate", "confidence_bucket_roi_per_pick", "confidence_bucket_picks"],
            ascending=[False, False, False],
        )
    if not recent_market_summary.empty:
        recent_market_summary = recent_market_summary.sort_values(
            ["recent_market_roi_per_pick", "recent_market_hit_rate", "recent_market_picks"],
            ascending=[False, False, False],
        )
    if not recent_sportsbook_summary.empty:
        recent_sportsbook_summary = recent_sportsbook_summary.sort_values(
            ["recent_sportsbook_roi_per_pick", "recent_sportsbook_hit_rate", "recent_sportsbook_picks"],
            ascending=[False, False, False],
        )

    return {
        "summary": pd.DataFrame([profile["summary"]]),
        "weight_profile": pd.DataFrame([weight_profile]),
        "market_summary": market_summary,
        "sportsbook_summary": sportsbook_summary,
        "confidence_summary": confidence_summary,
        "recent_market_summary": recent_market_summary,
        "recent_sportsbook_summary": recent_sportsbook_summary,
    }


def _build_reason_text(row: pd.Series) -> str:
    parts: list[str] = []
    model_prob = row.get("model_prob")
    if pd.notna(model_prob):
        parts.append(f"Model {float(model_prob) * 100:.1f}%")
    edge = row.get("edge")
    if pd.notna(edge):
        parts.append(f"Edge {float(edge) * 100:.1f}%")
    market_picks = row.get("market_picks", 0)
    market_hit_rate = row.get("market_blended_hit_rate")
    if pd.notna(market_hit_rate) and float(market_picks or 0) > 0:
        parts.append(f"Market history {float(market_hit_rate) * 100:.1f}% over {int(market_picks)}")
    recent_market_picks = row.get("recent_market_picks", 0)
    recent_market_hit_rate = row.get("recent_market_blended_hit_rate")
    if pd.notna(recent_market_hit_rate) and float(recent_market_picks or 0) > 0:
        parts.append(f"Recent market form {float(recent_market_hit_rate) * 100:.1f}% over {int(recent_market_picks)}")
    confidence_picks = row.get("confidence_bucket_picks", 0)
    confidence_hit_rate = row.get("confidence_bucket_blended_hit_rate")
    if pd.notna(confidence_hit_rate) and float(confidence_picks or 0) > 0:
        parts.append(f"Confidence band {float(confidence_hit_rate) * 100:.1f}% over {int(confidence_picks)}")
    sportsbook_picks = row.get("sportsbook_picks", 0)
    sportsbook_roi = row.get("sportsbook_blended_roi_per_pick")
    if pd.notna(sportsbook_roi) and float(sportsbook_picks or 0) > 0:
        parts.append(f"Book ROI {float(sportsbook_roi):+.2f}u over {int(sportsbook_picks)}")
    recent_sportsbook_picks = row.get("recent_sportsbook_picks", 0)
    recent_sportsbook_hit_rate = row.get("recent_sportsbook_blended_hit_rate")
    if pd.notna(recent_sportsbook_hit_rate) and float(recent_sportsbook_picks or 0) > 0:
        parts.append(f"Recent book form {float(recent_sportsbook_hit_rate) * 100:.1f}% over {int(recent_sportsbook_picks)}")
    return " | ".join(parts[:4])


def _build_audit_label(row: pd.Series) -> str:
    player = str(row.get("player") or "Unknown").strip()
    market = str(row.get("market") or "").strip().replace("_", " ").title()
    pick = str(row.get("pick") or "").strip()
    sportsbook = str(row.get("sportsbook") or "").strip()
    return " | ".join(part for part in [player, market, pick, sportsbook] if part)


def build_smart_pick_audit(candidate_row: pd.Series) -> pd.DataFrame:
    if candidate_row is None or candidate_row.empty:
        return pd.DataFrame()

    audit_rows = [
        {
            "component": "Model probability",
            "impact": candidate_row.get("audit_model_score"),
            "detail": f"{float(candidate_row.get('model_prob', 0.0) or 0.0) * 100:.1f}% model probability multiplied by the current model weight.",
        },
        {
            "component": "Confidence",
            "impact": candidate_row.get("audit_confidence_score"),
            "detail": f"{float(candidate_row.get('confidence', 0.0) or 0.0):.1f} confidence applied through the current confidence weight.",
        },
        {
            "component": "Edge boost",
            "impact": candidate_row.get("audit_edge_score"),
            "detail": f"{float(candidate_row.get('edge', 0.0) or 0.0) * 100:.1f}% edge multiplied by the tuned edge multiplier.",
        },
        {
            "component": "Books support",
            "impact": candidate_row.get("audit_books_score"),
            "detail": f"{float(candidate_row.get('books_count', 1.0) or 1.0):.0f} books contributing confirmation strength.",
        },
        {
            "component": "Line delta",
            "impact": candidate_row.get("audit_line_delta_score"),
            "detail": f"{float(candidate_row.get('line_delta', 0.0) or 0.0):.2f} absolute line delta contributing movement signal.",
        },
        {
            "component": "Market history",
            "impact": candidate_row.get("audit_market_history_score"),
            "detail": f"{float(candidate_row.get('market_blended_hit_rate', 0.0) or 0.0) * 100:.1f}% blended market hit rate and ROI memory.",
        },
        {
            "component": "Confidence history",
            "impact": candidate_row.get("audit_confidence_history_score"),
            "detail": f"{float(candidate_row.get('confidence_bucket_blended_hit_rate', 0.0) or 0.0) * 100:.1f}% blended confidence-band hit rate.",
        },
        {
            "component": "Sportsbook history",
            "impact": candidate_row.get("audit_sportsbook_history_score"),
            "detail": f"{float(candidate_row.get('sportsbook_blended_hit_rate', 0.0) or 0.0) * 100:.1f}% blended sportsbook hit rate and ROI memory.",
        },
        {
            "component": "Recent market form",
            "impact": candidate_row.get("audit_recent_market_score"),
            "detail": f"{float(candidate_row.get('recent_market_blended_hit_rate', 0.0) or 0.0) * 100:.1f}% recent market hit rate from the latest settled sample.",
        },
        {
            "component": "Recent sportsbook form",
            "impact": candidate_row.get("audit_recent_sportsbook_score"),
            "detail": f"{float(candidate_row.get('recent_sportsbook_blended_hit_rate', 0.0) or 0.0) * 100:.1f}% recent sportsbook hit rate from the latest settled sample.",
        },
    ]
    audit_df = pd.DataFrame(audit_rows)
    audit_df["impact"] = pd.to_numeric(audit_df["impact"], errors="coerce").round(2)
    return audit_df


def build_smart_history_comparison(candidate_row: pd.Series) -> pd.DataFrame:
    if candidate_row is None or candidate_row.empty:
        return pd.DataFrame()

    comparison_rows = [
        {
            "memory_type": "Market",
            "full_history": candidate_row.get("market_blended_hit_rate"),
            "recent_form": candidate_row.get("recent_market_blended_hit_rate"),
            "full_sample": candidate_row.get("market_picks"),
            "recent_sample": candidate_row.get("recent_market_picks"),
            "full_roi": candidate_row.get("market_blended_roi_per_pick"),
            "recent_roi": candidate_row.get("recent_market_blended_roi_per_pick"),
        },
        {
            "memory_type": "Sportsbook",
            "full_history": candidate_row.get("sportsbook_blended_hit_rate"),
            "recent_form": candidate_row.get("recent_sportsbook_blended_hit_rate"),
            "full_sample": candidate_row.get("sportsbook_picks"),
            "recent_sample": candidate_row.get("recent_sportsbook_picks"),
            "full_roi": candidate_row.get("sportsbook_blended_roi_per_pick"),
            "recent_roi": candidate_row.get("recent_sportsbook_blended_roi_per_pick"),
        },
    ]
    comparison_df = pd.DataFrame(comparison_rows)
    for column in ["full_history", "recent_form", "full_roi", "recent_roi"]:
        comparison_df[column] = pd.to_numeric(comparison_df[column], errors="coerce")
    for column in ["full_sample", "recent_sample"]:
        comparison_df[column] = pd.to_numeric(comparison_df[column], errors="coerce").fillna(0).astype(int)
    return comparison_df


def score_smart_picks(
    candidates_df: pd.DataFrame,
    graded_df: pd.DataFrame,
    override_profile: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, dict[str, float | int]]:
    if candidates_df.empty:
        return pd.DataFrame(), _empty_summary()

    profile = build_smart_pick_profile(graded_df)
    weight_profile = apply_smart_weight_overrides(build_smart_weight_profile(graded_df), override_profile)
    summary = profile["summary"]
    scored = _coerce_numeric(
        candidates_df,
        ["model_prob", "edge", "confidence", "books_count", "line_delta", "recommended_stake", "recommended_units"],
    ).copy()
    scored["confidence_bucket"] = _confidence_bucket_from_series(
        scored["confidence"] if "confidence" in scored.columns else pd.Series(index=scored.index, dtype=float)
    )

    if not profile["market_summary"].empty and "market" in scored.columns:
        scored = scored.merge(profile["market_summary"], on="market", how="left")
    if not profile["sportsbook_summary"].empty and "sportsbook" in scored.columns:
        scored = scored.merge(profile["sportsbook_summary"], on="sportsbook", how="left")
    if not profile["confidence_summary"].empty:
        scored = scored.merge(profile["confidence_summary"], on="confidence_bucket", how="left")
    recent_market_summary = profile.get("recent_market_summary", pd.DataFrame())
    recent_sportsbook_summary = profile.get("recent_sportsbook_summary", pd.DataFrame())
    if not recent_market_summary.empty and "market" in scored.columns:
        scored = scored.merge(recent_market_summary, on="market", how="left")
    if not recent_sportsbook_summary.empty and "sportsbook" in scored.columns:
        scored = scored.merge(recent_sportsbook_summary, on="sportsbook", how="left")

    overall_hit_rate = float(summary["overall_hit_rate"])
    overall_roi_per_pick = float(summary["overall_roi_per_pick"])
    overall_avg_model_prob = float(summary["overall_avg_model_prob"])

    for prefix in ["market", "sportsbook", "confidence_bucket"]:
        picks_col = f"{prefix}_picks"
        hit_rate_col = f"{prefix}_hit_rate"
        roi_col = f"{prefix}_roi_per_pick"
        if picks_col not in scored.columns:
            scored[picks_col] = 0.0
        if hit_rate_col not in scored.columns:
            scored[hit_rate_col] = overall_hit_rate
        if roi_col not in scored.columns:
            scored[roi_col] = overall_roi_per_pick
        scored[f"{prefix}_blended_hit_rate"] = _blend_metric(
            scored[hit_rate_col],
            scored[picks_col],
            overall_hit_rate,
        )
        scored[f"{prefix}_blended_roi_per_pick"] = _blend_metric(
            scored[roi_col],
            scored[picks_col],
            overall_roi_per_pick,
        )
    for prefix in ["recent_market", "recent_sportsbook"]:
        picks_col = f"{prefix}_picks"
        hit_rate_col = f"{prefix}_hit_rate"
        roi_col = f"{prefix}_roi_per_pick"
        if picks_col not in scored.columns:
            scored[picks_col] = 0.0
        if hit_rate_col not in scored.columns:
            scored[hit_rate_col] = overall_hit_rate
        if roi_col not in scored.columns:
            scored[roi_col] = overall_roi_per_pick
        scored[f"{prefix}_blended_hit_rate"] = _blend_metric(
            scored[hit_rate_col],
            scored[picks_col],
            overall_hit_rate,
            prior_weight=8.0,
        )
        scored[f"{prefix}_blended_roi_per_pick"] = _blend_metric(
            scored[roi_col],
            scored[picks_col],
            overall_roi_per_pick,
            prior_weight=8.0,
        )

    model_prob_pct = pd.to_numeric(scored.get("model_prob", 0.0), errors="coerce").fillna(0.0) * 100.0
    edge_pct = pd.to_numeric(scored.get("edge", 0.0), errors="coerce").fillna(0.0) * 100.0
    confidence_value = pd.to_numeric(scored.get("confidence", 0.0), errors="coerce").fillna(0.0)
    books_count = pd.to_numeric(scored.get("books_count", 1.0), errors="coerce").fillna(1.0)
    line_delta = pd.to_numeric(scored.get("line_delta", 0.0), errors="coerce").fillna(0.0)

    scored["audit_model_score"] = model_prob_pct * float(weight_profile["model_score_weight"])
    scored["audit_confidence_score"] = confidence_value * float(weight_profile["confidence_score_weight"])
    scored["audit_edge_score"] = edge_pct.clip(lower=0.0, upper=18.0) * float(weight_profile["edge_multiplier"])
    scored["audit_books_score"] = books_count.clip(lower=1.0, upper=6.0) * float(weight_profile["books_multiplier"])
    scored["audit_line_delta_score"] = line_delta.abs().clip(lower=0.0, upper=6.0) * float(weight_profile["line_delta_multiplier"])

    scored["audit_market_history_score"] = (
        ((scored["market_blended_hit_rate"] - overall_hit_rate) * 100.0 * float(weight_profile["history_market_weight"]))
        + (scored["market_blended_roi_per_pick"] * float(weight_profile["market_roi_multiplier"]))
    )
    scored["audit_confidence_history_score"] = (
        (scored["confidence_bucket_blended_hit_rate"] - overall_hit_rate) * 100.0 * float(weight_profile["history_confidence_weight"])
    )
    scored["audit_sportsbook_history_score"] = (
        ((scored["sportsbook_blended_hit_rate"] - overall_hit_rate) * 100.0 * float(weight_profile["history_sportsbook_weight"]))
        + (scored["sportsbook_blended_roi_per_pick"] * float(weight_profile["sportsbook_roi_multiplier"]))
    )
    scored["audit_recent_market_score"] = (
        ((scored["recent_market_blended_hit_rate"] - overall_hit_rate) * 100.0 * float(weight_profile["recent_market_weight"]))
        + (scored["recent_market_blended_roi_per_pick"] * float(weight_profile["recent_market_roi_multiplier"]))
    )
    scored["audit_recent_sportsbook_score"] = (
        ((scored["recent_sportsbook_blended_hit_rate"] - overall_hit_rate) * 100.0 * float(weight_profile["recent_sportsbook_weight"]))
        + (scored["recent_sportsbook_blended_roi_per_pick"] * float(weight_profile["recent_sportsbook_roi_multiplier"]))
    )

    base_score = (
        scored["audit_model_score"]
        + scored["audit_confidence_score"]
        + scored["audit_edge_score"]
        + scored["audit_books_score"]
        + scored["audit_line_delta_score"]
    )

    history_lift = (
        scored["audit_market_history_score"]
        + scored["audit_confidence_history_score"]
        + scored["audit_sportsbook_history_score"]
        + scored["audit_recent_market_score"]
        + scored["audit_recent_sportsbook_score"]
    )

    scored["smart_expected_win_rate"] = (
        (pd.to_numeric(scored.get("model_prob", 0.0), errors="coerce").fillna(overall_avg_model_prob) * float(weight_profile["expected_model_weight"]))
        + (scored["market_blended_hit_rate"] * float(weight_profile["expected_market_weight"]))
        + (scored["confidence_bucket_blended_hit_rate"] * float(weight_profile["expected_confidence_weight"]))
        + (scored["sportsbook_blended_hit_rate"] * float(weight_profile["expected_sportsbook_weight"]))
    ).clip(lower=0.01, upper=0.99)

    scored["smart_score"] = (base_score + history_lift).map(lambda value: round(_clip(float(value), 1.0, 99.0), 1))
    scored["smart_history_hit_rate"] = (
        (scored["market_blended_hit_rate"] * 0.45)
        + (scored["confidence_bucket_blended_hit_rate"] * 0.35)
        + (scored["sportsbook_blended_hit_rate"] * 0.20)
    ).clip(lower=0.0, upper=0.99)

    scored["smart_score_delta"] = (
        scored["smart_expected_win_rate"] - pd.to_numeric(scored.get("model_prob", 0.0), errors="coerce").fillna(0.0)
    )

    scored["smart_tier"] = pd.cut(
        scored["smart_score"],
        bins=[0, 68, 78, 88, 100],
        labels=["Watch", "Strong", "Priority", "Elite"],
        include_lowest=True,
    ).astype(str)
    scored["smart_summary"] = scored.apply(_build_reason_text, axis=1)
    scored["history_picks_used"] = (
        pd.to_numeric(scored.get("market_picks", 0), errors="coerce").fillna(0)
        + pd.to_numeric(scored.get("sportsbook_picks", 0), errors="coerce").fillna(0)
        + pd.to_numeric(scored.get("confidence_bucket_picks", 0), errors="coerce").fillna(0)
        + pd.to_numeric(scored.get("recent_market_picks", 0), errors="coerce").fillna(0)
        + pd.to_numeric(scored.get("recent_sportsbook_picks", 0), errors="coerce").fillna(0)
    ).astype(int)
    scored["smart_profile_mode"] = str(weight_profile["profile_mode"])
    scored["smart_audit_label"] = scored.apply(_build_audit_label, axis=1)

    return scored.sort_values(
        ["smart_score", "smart_expected_win_rate", "edge", "confidence"],
        ascending=[False, False, False, False],
    ), summary
