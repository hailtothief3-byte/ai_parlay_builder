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
