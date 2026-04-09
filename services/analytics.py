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


def build_period_model_review(
    graded_df: pd.DataFrame,
    period_days: int,
    current_label: str,
    prior_label: str,
) -> dict[str, object]:
    empty_response = {
        "current_window_label": current_label,
        "prior_window_label": prior_label,
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
    current_start = latest_resolved - pd.Timedelta(days=period_days)
    prior_start = current_start - pd.Timedelta(days=period_days)

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
        insights.append(f"No graded picks landed in {current_label.lower()}, so the review is waiting on fresh settled results.")
    else:
        insights.append(
            f"{current_label} logged {current_summary['picks']} graded picks, {pick_delta:+d} versus {prior_label.lower()}, for {current_summary['profit_units']:+.2f} units."
        )
        insights.append(
            f"Hit rate moved {hit_delta * 100:+.1f} points to {current_summary['hit_rate'] * 100:.1f}%, while units per pick shifted {upp_delta:+.2f} to {current_summary['units_per_pick']:+.2f}."
        )
        if current_summary["top_source"]:
            insights.append(f"Best current workflow source: {current_summary['top_source']}.")
        if current_summary["top_market"]:
            insights.append(f"Best current market pocket: {current_summary['top_market']}.")

    return {
        "current_window_label": current_label,
        "prior_window_label": prior_label,
        "current_summary": current_summary,
        "prior_summary": prior_summary,
        "source_breakdown": source_breakdown,
        "market_breakdown": market_breakdown,
        "insights": insights,
    }


def build_weekly_model_review(graded_df: pd.DataFrame) -> dict[str, object]:
    return build_period_model_review(
        graded_df=graded_df,
        period_days=7,
        current_label="Last 7 days",
        prior_label="Prior 7 days",
    )


def build_monthly_model_review(graded_df: pd.DataFrame) -> dict[str, object]:
    return build_period_model_review(
        graded_df=graded_df,
        period_days=30,
        current_label="Last 30 days",
        prior_label="Prior 30 days",
    )


def build_model_recommendation_cards(
    weekly_review: dict[str, object],
    monthly_review: dict[str, object],
    sport_label: str = "",
) -> list[dict[str, str]]:
    cards: list[dict[str, str]] = []
    weekly_current = dict(weekly_review.get("current_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    weekly_prior = dict(weekly_review.get("prior_summary") or {})
    monthly_prior = dict(monthly_review.get("prior_summary") or {})

    weekly_units = float(weekly_current.get("profit_units", 0.0) or 0.0)
    weekly_units_per_pick = float(weekly_current.get("units_per_pick", 0.0) or 0.0)
    weekly_hit_delta = float(weekly_current.get("hit_rate", 0.0) or 0.0) - float(weekly_prior.get("hit_rate", 0.0) or 0.0)
    monthly_units = float(monthly_current.get("profit_units", 0.0) or 0.0)
    monthly_units_per_pick = float(monthly_current.get("units_per_pick", 0.0) or 0.0)
    monthly_hit_delta = float(monthly_current.get("hit_rate", 0.0) or 0.0) - float(monthly_prior.get("hit_rate", 0.0) or 0.0)

    top_weekly_source = str(weekly_current.get("top_source") or "")
    top_weekly_market = str(weekly_current.get("top_market") or "")
    sport_prefix = f"{sport_label} " if str(sport_label).strip() else ""

    if weekly_units > 0 and weekly_units_per_pick > 0:
        cards.append(
            {
                "title": "Lean Into What Is Working",
                "status": "Positive Momentum",
                "body": f"The last 7 days of {sport_prefix.lower()}results are profitable at {weekly_units:+.2f} units and {weekly_units_per_pick:+.2f} units per pick. Keep weighting current builds toward the workflow patterns that are already paying off.",
            }
        )
    elif weekly_units < 0 or weekly_units_per_pick < 0:
        cards.append(
            {
                "title": "Tighten Current Exposure",
                "status": "Cooling Off",
                "body": f"The last 7 days of {sport_prefix.lower()}results slipped to {weekly_units:+.2f} units and {weekly_units_per_pick:+.2f} units per pick. This is a good stretch to reduce aggressive forcing and lean on the strongest-ranked edges only.",
            }
        )

    if weekly_hit_delta < -0.04 and monthly_hit_delta < 0:
        cards.append(
            {
                "title": "Reduce Aggressive Builds",
                "status": "Hit Rate Pressure",
                "body": "Short-term and monthly hit rate are both slipping. Consider trimming leg counts, raising minimum confidence, and letting the smart profile stay more selective for now.",
            }
        )
    elif weekly_hit_delta > 0.04 and monthly_hit_delta > 0:
        cards.append(
            {
                "title": "Trust The Current Mix",
                "status": "Hit Rate Improving",
                "body": "Hit rate is improving in both the weekly and monthly windows. This is a healthier time to trust the current smart ranking mix instead of making large manual changes.",
            }
        )

    if top_weekly_market:
        cards.append(
            {
                "title": "Best Current Market Pocket",
                "status": "Market Read",
                "body": f"{top_weekly_market} is the strongest recent market pocket. Keep an eye on whether that edge remains real in the next weekly review before over-expanding into colder markets.",
            }
        )

    if top_weekly_source:
        cards.append(
            {
                "title": "Current Workflow Leader",
                "status": "Source Edge",
                "body": f"{top_weekly_source} is leading the recent window. Let that workflow guide more of the near-term tracking volume while the other paths keep proving themselves.",
            }
        )

    weekly_source_breakdown = weekly_review.get("source_breakdown", pd.DataFrame())
    if isinstance(weekly_source_breakdown, pd.DataFrame) and not weekly_source_breakdown.empty and "source" in weekly_source_breakdown.columns:
        auto_row = weekly_source_breakdown[weekly_source_breakdown["source"] == "smart_pick_engine_auto"].head(1)
        manual_row = weekly_source_breakdown[weekly_source_breakdown["source"] == "smart_pick_engine_manual"].head(1)
        legacy_row = weekly_source_breakdown[weekly_source_breakdown["source"] == "edge_scanner"].head(1)
        if not auto_row.empty and not legacy_row.empty:
            auto_units = float(auto_row.iloc[0].get("units_per_pick", 0.0) or 0.0)
            legacy_units = float(legacy_row.iloc[0].get("units_per_pick", 0.0) or 0.0)
            if auto_units > legacy_units + 0.08:
                cards.append(
                    {
                        "title": "Auto Smart Is Beating Legacy",
                        "status": "Source Comparison",
                        "body": f"Smart Pick Engine (Auto) is ahead of Edge Scanner by {auto_units - legacy_units:+.2f} units per pick in the current weekly window. Keep feeding more volume through auto to verify the edge holds.",
                    }
                )
        if not auto_row.empty and not manual_row.empty:
            auto_units = float(auto_row.iloc[0].get("units_per_pick", 0.0) or 0.0)
            manual_units = float(manual_row.iloc[0].get("units_per_pick", 0.0) or 0.0)
            if manual_units > auto_units + 0.08:
                cards.append(
                    {
                        "title": "Manual Smart Has A Weekly Edge",
                        "status": "Source Comparison",
                        "body": f"Smart Pick Engine (Manual) is ahead of auto by {manual_units - auto_units:+.2f} units per pick this week. Keep the manual mix live, but keep watching the monthly trend before locking it in.",
                    }
                )

    if monthly_units < 0 and not cards:
        cards.append(
            {
                "title": "Monthly Reset",
                "status": "Watch Closely",
                "body": "The last 30 days are still negative overall, even if the latest week is mixed. Stay deliberate with stake sizing and focus on cleaner setups until the broader trend improves.",
            }
        )

    return cards[:6]


def build_coach_mode_summary(weekly_review: dict[str, object], monthly_review: dict[str, object], sport_label: str = "") -> str:
    weekly_current = dict(weekly_review.get("current_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    weekly_units = float(weekly_current.get("profit_units", 0.0) or 0.0)
    monthly_units = float(monthly_current.get("profit_units", 0.0) or 0.0)
    weekly_source = str(weekly_current.get("top_source") or "")
    weekly_market = str(weekly_current.get("top_market") or "")
    sport_prefix = f"{sport_label} " if str(sport_label).strip() else ""

    if int(weekly_current.get("picks", 0) or 0) <= 0:
        return f"Coach Mode: waiting on more settled {sport_prefix.lower()}picks before making a fresh weekly call."

    posture = "press the edge" if weekly_units > 0 and monthly_units >= 0 else "stay selective"
    source_text = f"{weekly_source.replace('_', ' ').title()} is leading the current mix" if weekly_source else "no clear workflow leader has emerged yet"
    market_text = f", with {weekly_market.replace('_', ' ').title()} acting as the strongest market pocket" if weekly_market else ""
    return f"Coach Mode: for {sport_prefix.strip() or 'this board'}, {source_text}{market_text}, so the current posture is to {posture} while the app keeps tracking weekly versus monthly trend strength."


def build_review_action_checklist(
    weekly_review: dict[str, object],
    monthly_review: dict[str, object],
) -> list[dict[str, object]]:
    weekly_current = dict(weekly_review.get("current_summary") or {})
    monthly_current = dict(monthly_review.get("current_summary") or {})
    weekly_prior = dict(weekly_review.get("prior_summary") or {})

    weekly_units = float(weekly_current.get("profit_units", 0.0) or 0.0)
    monthly_units = float(monthly_current.get("profit_units", 0.0) or 0.0)
    weekly_hit_delta = float(weekly_current.get("hit_rate", 0.0) or 0.0) - float(weekly_prior.get("hit_rate", 0.0) or 0.0)
    cooling = weekly_units < 0 or monthly_units < 0 or weekly_hit_delta < -0.04

    action_items = [
        {
            "label": "Use all live edges as the default candidate pool",
            "setting_key": "candidate_pool",
            "value": "All live edges",
            "reason": "Keep the live build sourcing broad enough for the smart ranker to do its job.",
        },
        {
            "label": "Target 3 live legs",
            "setting_key": "live_legs",
            "value": 3 if cooling else 4,
            "reason": "Shorter tickets stay cleaner when results cool off, while 4-leg builds are reasonable when the board is healthy.",
        },
        {
            "label": "Raise live minimum confidence",
            "setting_key": "live_min_confidence",
            "value": 72 if cooling else 66,
            "reason": "A firmer confidence floor helps reduce forcing during rough stretches.",
        },
        {
            "label": "Block same-player live overlap",
            "setting_key": "live_same_player",
            "value": False,
            "reason": "Cleaner exposure usually makes the smart profile easier to trust.",
        },
        {
            "label": "Use a balanced demo profile",
            "setting_key": "demo_style",
            "value": "Balanced" if not cooling else "Safe",
            "reason": "Balanced keeps demo builds realistic unless the current trend says to get more selective.",
        },
        {
            "label": "Keep same-team demo overlap blocked",
            "setting_key": "demo_same_team",
            "value": False,
            "reason": "This avoids piling too much narrative correlation into the demo card.",
        },
    ]
    return action_items


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


def build_ticket_review_insights(
    ticket_row: dict | pd.Series,
    benchmark: dict[str, float | int | None],
    overlap_count: int,
    current_benchmark: pd.DataFrame,
) -> list[dict[str, str]]:
    insights: list[dict[str, str]] = []
    leg_count = int(ticket_row.get("leg_count", 0) or 0)
    if leg_count <= 0:
        return insights

    ticket_avg_confidence = ticket_row.get("avg_confidence")
    benchmark_avg_confidence = benchmark.get("benchmark_avg_confidence")
    ticket_avg_model_prob = ticket_row.get("avg_model_prob")
    benchmark_avg_model_prob = benchmark.get("benchmark_avg_model_prob")
    overlap_ratio = overlap_count / leg_count if leg_count > 0 else 0.0

    if benchmark_avg_confidence is not None and ticket_avg_confidence is not None:
        confidence_gap = float(ticket_avg_confidence) - float(benchmark_avg_confidence)
        if confidence_gap >= 3:
            insights.append(
                {
                    "title": "Ticket Beat The Usual Confidence Bar",
                    "status": "Model Alignment",
                    "body": f"This saved ticket is running {confidence_gap:.1f} confidence points above the benchmark average for the same leg count.",
                }
            )
        elif confidence_gap <= -3:
            insights.append(
                {
                    "title": "Ticket Was Below The Benchmark Confidence Zone",
                    "status": "Model Drift",
                    "body": f"This ticket came in {abs(confidence_gap):.1f} confidence points below the benchmark average for similar builds.",
                }
            )

    if benchmark_avg_model_prob is not None and ticket_avg_model_prob is not None:
        model_gap = (float(ticket_avg_model_prob) - float(benchmark_avg_model_prob)) * 100
        if model_gap >= 2:
            insights.append(
                {
                    "title": "Ticket Cleared The Model Probability Benchmark",
                    "status": "Probability Edge",
                    "body": f"The ticket's average model probability is {model_gap:.2f} points above the benchmark leg set.",
                }
            )
        elif model_gap <= -2:
            insights.append(
                {
                    "title": "Ticket Lagged The Model Probability Benchmark",
                    "status": "Probability Gap",
                    "body": f"The ticket's average model probability is {abs(model_gap):.2f} points below the benchmark leg set.",
                }
            )

    if not current_benchmark.empty:
        if overlap_ratio >= 0.67:
            insights.append(
                {
                    "title": "Ticket Mostly Matched The Current Model",
                    "status": "High Overlap",
                    "body": f"{overlap_count} of {leg_count} legs still overlap with today's top model choices, so the saved ticket is staying close to the current ranking.",
                }
            )
        elif overlap_ratio <= 0.34:
            insights.append(
                {
                    "title": "Ticket Has Drifted From The Current Model",
                    "status": "Low Overlap",
                    "body": f"Only {overlap_count} of {leg_count} legs still overlap with today's top model choices, which suggests the board has moved or the model has shifted.",
                }
            )

    if not insights:
        insights.append(
            {
                "title": "Ticket And Model Are In The Same Neighborhood",
                "status": "Neutral Read",
                "body": "This saved ticket is neither clearly stronger nor clearly weaker than the current benchmark view yet.",
            }
        )
    return insights[:3]
