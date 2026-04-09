from __future__ import annotations

import pandas as pd

from services.edge_scanner import scan_edges
from services.history_service import get_line_history
from services.results_service import get_graded_picks


def build_clv_backtest(sport_keys: list[str] | str) -> pd.DataFrame:
    edges = scan_edges(sport_key=sport_keys, is_dfs=False)
    history = get_line_history(sport_key=sport_keys)

    if edges.empty or history.empty:
        return pd.DataFrame()

    history = history.sort_values("pulled_at")
    open_lines = (
        history.drop_duplicates(
            subset=["event_id", "book_key", "market", "player", "pick"],
            keep="first",
        )[["event_id", "book_key", "market", "player", "pick", "line"]]
        .rename(columns={"line": "open_line"})
    )
    close_lines = (
        history.drop_duplicates(
            subset=["event_id", "book_key", "market", "player", "pick"],
            keep="last",
        )[["event_id", "book_key", "market", "player", "pick", "line"]]
        .rename(columns={"line": "close_line"})
    )

    merged = edges.merge(
        open_lines,
        left_on=["event_id", "book_key", "market", "player", "pick"],
        right_on=["event_id", "book_key", "market", "player", "pick"],
        how="left",
    ).merge(
        close_lines,
        left_on=["event_id", "book_key", "market", "player", "pick"],
        right_on=["event_id", "book_key", "market", "player", "pick"],
        how="left",
    )

    merged = merged.dropna(subset=["open_line", "close_line"]).copy()
    if merged.empty:
        return pd.DataFrame()

    merged["line_move"] = merged["close_line"] - merged["open_line"]
    merged["model_direction"] = merged["best_for"].map(
        {
            "over": 1,
            "under": -1,
            "yes": 1,
            "no": -1,
        }
    )
    merged["clv_win"] = ((merged["line_move"] * merged["model_direction"]) > 0).astype(int)
    merged["prob_bucket"] = pd.cut(
        merged["model_prob"],
        bins=[0.0, 0.52, 0.56, 0.60, 0.65, 0.70, 1.0],
        include_lowest=True,
    )

    return merged


def build_calibration_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
    if backtest_df.empty:
        return pd.DataFrame()

    summary = (
        backtest_df.groupby("prob_bucket", observed=False)
        .agg(
            picks=("player", "count"),
            avg_model_prob=("model_prob", "mean"),
            clv_hit_rate=("clv_win", "mean"),
            avg_edge=("edge", "mean"),
            avg_line_move=("line_move", "mean"),
        )
        .reset_index()
    )
    return summary[summary["picks"] > 0]


def build_true_backtest(sport_keys: list[str] | str) -> pd.DataFrame:
    graded = get_graded_picks(sport_keys)
    if graded.empty:
        return pd.DataFrame()

    graded = graded.copy()
    graded["prob_bucket"] = pd.cut(
        graded["model_prob"],
        bins=[0.0, 0.52, 0.56, 0.60, 0.65, 0.70, 1.0],
        include_lowest=True,
    )
    return graded


def build_true_calibration_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
    if backtest_df.empty:
        return pd.DataFrame()

    summary = (
        backtest_df.groupby("prob_bucket", observed=False)
        .agg(
            picks=("player", "count"),
            avg_model_prob=("model_prob", "mean"),
            hit_rate=("won", "mean"),
            avg_edge=("edge", "mean"),
            profit_units=("profit_units", "sum"),
        )
        .reset_index()
    )
    return summary[summary["picks"] > 0]


def build_true_market_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
    if backtest_df.empty:
        return pd.DataFrame()

    summary = (
        backtest_df.groupby("market", observed=False)
        .agg(
            picks=("player", "count"),
            hit_rate=("won", "mean"),
            avg_model_prob=("model_prob", "mean"),
            avg_edge=("edge", "mean"),
            profit_units=("profit_units", "sum"),
        )
        .reset_index()
        .sort_values(["profit_units", "hit_rate", "picks"], ascending=[False, False, False])
    )
    return summary[summary["picks"] > 0]


def build_true_sportsbook_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
    if backtest_df.empty:
        return pd.DataFrame()

    summary = (
        backtest_df.groupby("sportsbook", observed=False)
        .agg(
            picks=("player", "count"),
            hit_rate=("won", "mean"),
            avg_edge=("edge", "mean"),
            profit_units=("profit_units", "sum"),
        )
        .reset_index()
        .sort_values(["profit_units", "hit_rate", "picks"], ascending=[False, False, False])
    )
    return summary[summary["picks"] > 0]


def build_true_confidence_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
    if backtest_df.empty:
        return pd.DataFrame()

    scored = backtest_df.copy()
    scored["confidence_bucket"] = pd.cut(
        scored["confidence"],
        bins=[0, 60, 70, 80, 90, 100],
        include_lowest=True,
    )
    summary = (
        scored.groupby("confidence_bucket", observed=False)
        .agg(
            picks=("player", "count"),
            hit_rate=("won", "mean"),
            avg_edge=("edge", "mean"),
            profit_units=("profit_units", "sum"),
        )
        .reset_index()
    )
    return summary[summary["picks"] > 0]


def build_true_source_summary(backtest_df: pd.DataFrame) -> pd.DataFrame:
    if backtest_df.empty or "source" not in backtest_df.columns:
        return pd.DataFrame()

    summary = (
        backtest_df.groupby("source", observed=False)
        .agg(
            picks=("player", "count"),
            hit_rate=("won", "mean"),
            avg_model_prob=("model_prob", "mean"),
            avg_edge=("edge", "mean"),
            avg_confidence=("confidence", "mean"),
            profit_units=("profit_units", "sum"),
            roi_per_pick=("profit_units", "mean"),
        )
        .reset_index()
        .sort_values(["profit_units", "hit_rate", "picks"], ascending=[False, False, False])
    )
    return summary[summary["picks"] > 0]


def build_true_source_timeseries(backtest_df: pd.DataFrame, rolling_window: int = 10) -> tuple[pd.DataFrame, pd.DataFrame]:
    if backtest_df.empty or "source" not in backtest_df.columns or "resolved_at" not in backtest_df.columns:
        return pd.DataFrame(), pd.DataFrame()

    working = backtest_df.copy()
    working["resolved_at"] = pd.to_datetime(working["resolved_at"], errors="coerce")
    working = working[working["resolved_at"].notna()].copy()
    if working.empty:
        return pd.DataFrame(), pd.DataFrame()

    working = working.sort_values("resolved_at")
    working["resolved_day"] = working["resolved_at"].dt.floor("D")

    daily_profit = (
        working.groupby(["resolved_day", "source"], observed=False)
        .agg(profit_units=("profit_units", "sum"))
        .reset_index()
    )
    cumulative_profit = (
        daily_profit.pivot(index="resolved_day", columns="source", values="profit_units")
        .fillna(0.0)
        .sort_index()
        .cumsum()
    )

    rolling_frames: list[pd.DataFrame] = []
    for source, source_df in working.groupby("source", observed=False):
        source_df = source_df.sort_values("resolved_at").copy()
        source_df["rolling_hit_rate"] = source_df["won"].rolling(window=rolling_window, min_periods=1).mean()
        source_df["sample_number"] = range(1, len(source_df) + 1)
        rolling_frames.append(
            source_df[["resolved_at", "sample_number", "rolling_hit_rate"]].assign(source=source)
        )

    if not rolling_frames:
        return cumulative_profit, pd.DataFrame()

    rolling_hit_rate = pd.concat(rolling_frames, ignore_index=True)
    rolling_hit_rate = (
        rolling_hit_rate.pivot(index="resolved_at", columns="source", values="rolling_hit_rate")
        .sort_index()
    )
    return cumulative_profit, rolling_hit_rate


def build_experiment_snapshot(
    graded_df: pd.DataFrame,
    source_summary_df: pd.DataFrame,
    rolling_window: int = 10,
) -> dict[str, object]:
    snapshot: dict[str, object] = {
        "graded_pick_count": int(len(graded_df)) if not graded_df.empty else 0,
        "source_summary": [],
        "recent_experiments": [],
        "cumulative_units": [],
        "rolling_hit_rate": [],
    }
    if not source_summary_df.empty:
        snapshot["source_summary"] = source_summary_df.to_dict(orient="records")

    if graded_df.empty:
        return snapshot

    experiment_log = graded_df.copy()
    if "source" in experiment_log.columns:
        experiment_log = experiment_log[
            experiment_log["source"].isin(["smart_pick_engine_auto", "smart_pick_engine_manual", "edge_scanner"])
        ].copy()
    if not experiment_log.empty:
        keep_columns = [
            column for column in [
                "resolved_at",
                "source",
                "player",
                "market",
                "pick",
                "summary",
                "grade",
                "profit_units",
                "model_prob",
                "edge",
                "confidence",
            ]
            if column in experiment_log.columns
        ]
        if keep_columns:
            trimmed_log = experiment_log[keep_columns].copy()
            if "resolved_at" in trimmed_log.columns:
                trimmed_log["resolved_at"] = pd.to_datetime(trimmed_log["resolved_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
            snapshot["recent_experiments"] = trimmed_log.sort_values("resolved_at", ascending=False).head(50).to_dict(orient="records")

    cumulative_profit, rolling_hit_rate = build_true_source_timeseries(graded_df, rolling_window=rolling_window)
    if not cumulative_profit.empty:
        snapshot["cumulative_units"] = cumulative_profit.reset_index().to_dict(orient="records")
    if not rolling_hit_rate.empty:
        snapshot["rolling_hit_rate"] = rolling_hit_rate.reset_index().to_dict(orient="records")

    return snapshot


def build_weekly_model_review(graded_df: pd.DataFrame) -> dict[str, object]:
    empty_response = {
        "current_window_label": "Last 7 days",
        "prior_window_label": "Prior 7 days",
        "current_summary": {},
        "prior_summary": {},
        "source_breakdown": pd.DataFrame(),
        "market_breakdown": pd.DataFrame(),
        "insights": [],
    }
    if graded_df.empty or "resolved_at" not in graded_df.columns:
        return empty_response

    working = graded_df.copy()
    working["resolved_at"] = pd.to_datetime(working["resolved_at"], errors="coerce")
    working = working[working["resolved_at"].notna()].copy()
    if working.empty:
        return empty_response

    latest_resolved = working["resolved_at"].max()
    if pd.isna(latest_resolved):
        return empty_response
    current_start = latest_resolved - pd.Timedelta(days=7)
    prior_start = current_start - pd.Timedelta(days=7)

    current_df = working[working["resolved_at"] >= current_start].copy()
    prior_df = working[(working["resolved_at"] >= prior_start) & (working["resolved_at"] < current_start)].copy()

    def _build_window_summary(df: pd.DataFrame) -> dict[str, object]:
        if df.empty:
            return {
                "picks": 0,
                "hit_rate": 0.0,
                "profit_units": 0.0,
                "units_per_pick": 0.0,
                "avg_model_prob": 0.0,
                "avg_edge": 0.0,
                "top_source": "",
                "top_market": "",
            }
        summary = {
            "picks": int(len(df)),
            "hit_rate": float(pd.to_numeric(df.get("won"), errors="coerce").fillna(0).mean()) if "won" in df.columns else 0.0,
            "profit_units": float(pd.to_numeric(df.get("profit_units"), errors="coerce").fillna(0.0).sum()) if "profit_units" in df.columns else 0.0,
            "units_per_pick": float(pd.to_numeric(df.get("profit_units"), errors="coerce").fillna(0.0).mean()) if "profit_units" in df.columns else 0.0,
            "avg_model_prob": float(pd.to_numeric(df.get("model_prob"), errors="coerce").dropna().mean()) if "model_prob" in df.columns and not pd.to_numeric(df.get("model_prob"), errors="coerce").dropna().empty else 0.0,
            "avg_edge": float(pd.to_numeric(df.get("edge"), errors="coerce").dropna().mean()) if "edge" in df.columns and not pd.to_numeric(df.get("edge"), errors="coerce").dropna().empty else 0.0,
            "top_source": "",
            "top_market": "",
        }
        if "source" in df.columns and not df["source"].dropna().empty:
            source_summary = (
                df.groupby("source", observed=False)
                .agg(profit_units=("profit_units", "sum"), picks=("source", "count"))
                .reset_index()
                .sort_values(["profit_units", "picks"], ascending=[False, False])
            )
            if not source_summary.empty:
                summary["top_source"] = str(source_summary.iloc[0]["source"])
        if "market" in df.columns and not df["market"].dropna().empty:
            market_summary = (
                df.groupby("market", observed=False)
                .agg(profit_units=("profit_units", "sum"), picks=("market", "count"))
                .reset_index()
                .sort_values(["profit_units", "picks"], ascending=[False, False])
            )
            if not market_summary.empty:
                summary["top_market"] = str(market_summary.iloc[0]["market"])
        return summary

    current_summary = _build_window_summary(current_df)
    prior_summary = _build_window_summary(prior_df)

    source_breakdown = pd.DataFrame()
    if not current_df.empty and "source" in current_df.columns:
        source_breakdown = (
            current_df.groupby("source", observed=False)
            .agg(
                picks=("source", "count"),
                hit_rate=("won", "mean"),
                profit_units=("profit_units", "sum"),
                units_per_pick=("profit_units", "mean"),
            )
            .reset_index()
            .sort_values(["profit_units", "hit_rate", "picks"], ascending=[False, False, False])
        )

    market_breakdown = pd.DataFrame()
    if not current_df.empty and "market" in current_df.columns:
        market_breakdown = (
            current_df.groupby("market", observed=False)
            .agg(
                picks=("market", "count"),
                hit_rate=("won", "mean"),
                profit_units=("profit_units", "sum"),
                units_per_pick=("profit_units", "mean"),
            )
            .reset_index()
            .sort_values(["profit_units", "hit_rate", "picks"], ascending=[False, False, False])
        )

    insights: list[str] = []
    pick_delta = current_summary["picks"] - prior_summary["picks"]
    unit_delta = current_summary["profit_units"] - prior_summary["profit_units"]
    upp_delta = current_summary["units_per_pick"] - prior_summary["units_per_pick"]
    hit_delta = current_summary["hit_rate"] - prior_summary["hit_rate"]

    if current_summary["picks"] <= 0:
        insights.append("No graded picks landed in the last 7 days, so the weekly review is waiting on fresh settled results.")
    else:
        insights.append(
            f"Last 7 days logged {current_summary['picks']} graded picks, {pick_delta:+d} versus the prior week, for {current_summary['profit_units']:+.2f} units."
        )
        insights.append(
            f"Weekly hit rate moved {hit_delta * 100:+.1f} points to {current_summary['hit_rate'] * 100:.1f}%, while units per pick shifted {upp_delta:+.2f} to {current_summary['units_per_pick']:+.2f}."
        )
        if current_summary["top_source"]:
            insights.append(f"Best current workflow source: {current_summary['top_source']}.")
        if current_summary["top_market"]:
            insights.append(f"Best current market pocket: {current_summary['top_market']}.")

    return {
        "current_window_label": "Last 7 days",
        "prior_window_label": "Prior 7 days",
        "current_summary": current_summary,
        "prior_summary": prior_summary,
        "source_breakdown": source_breakdown,
        "market_breakdown": market_breakdown,
        "insights": insights,
    }


def build_ticket_benchmark_summary(graded_picks_df: pd.DataFrame, leg_count: int) -> dict[str, float | int | None]:
    if graded_picks_df.empty or leg_count <= 0:
        return {
            "benchmark_legs": 0,
            "benchmark_avg_model_prob": None,
            "benchmark_avg_confidence": None,
            "benchmark_hit_rate": None,
            "benchmark_profit_units": None,
        }

    benchmark = graded_picks_df.sort_values(
        ["model_prob", "confidence", "edge"],
        ascending=False,
    ).head(leg_count)

    if benchmark.empty:
        return {
            "benchmark_legs": 0,
            "benchmark_avg_model_prob": None,
            "benchmark_avg_confidence": None,
            "benchmark_hit_rate": None,
            "benchmark_profit_units": None,
        }

    return {
        "benchmark_legs": int(len(benchmark)),
        "benchmark_avg_model_prob": float(benchmark["model_prob"].mean()) if "model_prob" in benchmark.columns else None,
        "benchmark_avg_confidence": float(benchmark["confidence"].mean()) if "confidence" in benchmark.columns else None,
        "benchmark_hit_rate": float(benchmark["won"].mean()) if "won" in benchmark.columns else None,
        "benchmark_profit_units": float(benchmark["profit_units"].sum()) if "profit_units" in benchmark.columns else None,
    }
