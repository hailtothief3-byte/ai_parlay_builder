from __future__ import annotations

import pandas as pd


CONFIDENCE_BUCKET_BINS = [0, 60, 70, 80, 90, 100]
CONFIDENCE_BUCKET_LABELS = ["0-60", "61-70", "71-80", "81-90", "91-100"]
PRIOR_WEIGHT = 12.0


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

    history = history.dropna(subset=["player"], how="all")

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
    }


def build_smart_learning_tables(graded_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    profile = build_smart_pick_profile(graded_df)

    market_summary = profile["market_summary"].copy()
    sportsbook_summary = profile["sportsbook_summary"].copy()
    confidence_summary = profile["confidence_summary"].copy()

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

    return {
        "summary": pd.DataFrame([profile["summary"]]),
        "market_summary": market_summary,
        "sportsbook_summary": sportsbook_summary,
        "confidence_summary": confidence_summary,
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
    confidence_picks = row.get("confidence_bucket_picks", 0)
    confidence_hit_rate = row.get("confidence_bucket_blended_hit_rate")
    if pd.notna(confidence_hit_rate) and float(confidence_picks or 0) > 0:
        parts.append(f"Confidence band {float(confidence_hit_rate) * 100:.1f}% over {int(confidence_picks)}")
    sportsbook_picks = row.get("sportsbook_picks", 0)
    sportsbook_roi = row.get("sportsbook_blended_roi_per_pick")
    if pd.notna(sportsbook_roi) and float(sportsbook_picks or 0) > 0:
        parts.append(f"Book ROI {float(sportsbook_roi):+.2f}u over {int(sportsbook_picks)}")
    return " | ".join(parts[:4])


def score_smart_picks(candidates_df: pd.DataFrame, graded_df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float | int]]:
    if candidates_df.empty:
        return pd.DataFrame(), _empty_summary()

    profile = build_smart_pick_profile(graded_df)
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

    model_prob_pct = pd.to_numeric(scored.get("model_prob", 0.0), errors="coerce").fillna(0.0) * 100.0
    edge_pct = pd.to_numeric(scored.get("edge", 0.0), errors="coerce").fillna(0.0) * 100.0
    confidence_value = pd.to_numeric(scored.get("confidence", 0.0), errors="coerce").fillna(0.0)
    books_count = pd.to_numeric(scored.get("books_count", 1.0), errors="coerce").fillna(1.0)
    line_delta = pd.to_numeric(scored.get("line_delta", 0.0), errors="coerce").fillna(0.0)

    base_score = (
        (model_prob_pct * 0.42)
        + (confidence_value * 0.28)
        + edge_pct.clip(lower=0.0, upper=18.0) * 1.45
        + books_count.clip(lower=1.0, upper=6.0) * 0.85
        + line_delta.abs().clip(lower=0.0, upper=6.0) * 0.8
    )

    history_lift = (
        ((scored["market_blended_hit_rate"] - overall_hit_rate) * 100.0 * 0.36)
        + ((scored["confidence_bucket_blended_hit_rate"] - overall_hit_rate) * 100.0 * 0.30)
        + ((scored["sportsbook_blended_hit_rate"] - overall_hit_rate) * 100.0 * 0.18)
        + (scored["market_blended_roi_per_pick"] * 10.0)
        + (scored["sportsbook_blended_roi_per_pick"] * 6.0)
    )

    scored["smart_expected_win_rate"] = (
        (pd.to_numeric(scored.get("model_prob", 0.0), errors="coerce").fillna(overall_avg_model_prob) * 0.58)
        + (scored["market_blended_hit_rate"] * 0.22)
        + (scored["confidence_bucket_blended_hit_rate"] * 0.14)
        + (scored["sportsbook_blended_hit_rate"] * 0.06)
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
    ).astype(int)

    return scored.sort_values(
        ["smart_score", "smart_expected_win_rate", "edge", "confidence"],
        ascending=[False, False, False, False],
    ), summary
