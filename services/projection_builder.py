from __future__ import annotations

from datetime import datetime, timezone
from statistics import NormalDist

import pandas as pd
from sqlalchemy import delete

from db.models import PropProjection
from db.session import SessionLocal
from services.board_service import get_latest_board
from services.history_service import get_line_history
from services.results_service import get_graded_picks
from services.stats_service import get_latest_stats_snapshots
from sports_config import get_provider_labels, get_sport_config


STD_DEV_BY_MARKET = {
    "player_points": 6.5,
    "player_rebounds": 3.0,
    "player_assists": 2.8,
    "player_points_rebounds_assists": 8.0,
    "player_threes": 1.6,
    "player_home_runs": 0.35,
    "player_hits": 1.1,
    "player_total_bases": 1.8,
    "player_strikeouts": 1.8,
    "player_first_basket": 0.20,
}


MARKET_BIAS = {
    "player_points": 0.18,
    "player_rebounds": 0.08,
    "player_assists": 0.08,
    "player_points_rebounds_assists": 0.22,
    "player_threes": 0.05,
    "player_home_runs": 0.00,
    "player_hits": 0.04,
    "player_total_bases": 0.10,
    "player_strikeouts": 0.15,
    "player_first_basket": 0.00,
}


def american_to_probability(price: float | None) -> float | None:
    if price is None or pd.isna(price):
        return None
    if price > 0:
        return 100 / (price + 100)
    return abs(price) / (abs(price) + 100)


def devig_pair(over_prob: float | None, under_prob: float | None) -> tuple[float | None, float | None]:
    if over_prob is None or under_prob is None:
        return None, None
    total = over_prob + under_prob
    if total <= 0:
        return None, None
    return over_prob / total, under_prob / total


def _infer_projection(consensus_line: float, over_prob: float, market_key: str) -> tuple[float, float]:
    std_dev = STD_DEV_BY_MARKET.get(market_key, 5.0)
    clamped_prob = min(max(over_prob, 0.05), 0.95)
    z_score = NormalDist().inv_cdf(clamped_prob)
    bias = MARKET_BIAS.get(market_key, 0.0)
    projection = consensus_line + (z_score * std_dev * 0.45) + bias
    return projection, std_dev


def _infer_binary_projection(yes_prob: float, market_key: str) -> tuple[float, float]:
    std_dev = STD_DEV_BY_MARKET.get(market_key, 0.20)
    return yes_prob, std_dev


def _build_player_market_priors(sport_key: str) -> dict[tuple[str, str], dict[str, float]]:
    graded = get_graded_picks(sport_key)
    if graded.empty:
        return {}

    priors: dict[tuple[str, str], dict[str, float]] = {}

    numeric = graded.dropna(subset=["actual_value", "line"]).copy()
    if not numeric.empty:
        numeric["actual_minus_line"] = numeric["actual_value"] - numeric["line"]
        grouped = (
            numeric.groupby(["player", "market"], dropna=False)
            .agg(
                avg_actual_minus_line=("actual_minus_line", "mean"),
                graded_hit_rate=("won", "mean"),
                graded_samples=("player", "count"),
            )
            .reset_index()
        )

        for _, row in grouped.iterrows():
            priors[(str(row["player"]), str(row["market"]))] = {
                "avg_actual_minus_line": float(row["avg_actual_minus_line"]),
                "graded_hit_rate": float(row["graded_hit_rate"]),
                "graded_samples": float(row["graded_samples"]),
            }

    return priors


def _build_event_market_history_features(sport_key: str) -> dict[tuple[str, str, str], dict[str, float]]:
    history = get_line_history(sport_key)
    if history.empty:
        return {}

    line_history = history.dropna(subset=["line"]).copy()
    if line_history.empty:
        return {}

    features: dict[tuple[str, str, str], dict[str, float]] = {}

    grouped = (
        line_history.groupby(["event_id", "player", "market", "pulled_at"], dropna=False)
        .agg(
            consensus_line=("line", "mean"),
            line_std=("line", "std"),
            books=("book_key", "nunique"),
        )
        .reset_index()
        .sort_values("pulled_at")
    )

    summary = (
        grouped.groupby(["event_id", "player", "market"], dropna=False)
        .agg(
            open_consensus_line=("consensus_line", "first"),
            latest_consensus_line=("consensus_line", "last"),
            latest_line_std=("line_std", "last"),
            latest_books=("books", "last"),
        )
        .reset_index()
    )

    for _, row in summary.iterrows():
        open_line = row["open_consensus_line"]
        latest_line = row["latest_consensus_line"]
        line_std = row["latest_line_std"] if pd.notnull(row["latest_line_std"]) else 0.0
        features[(str(row["event_id"]), str(row["player"]), str(row["market"]))] = {
            "line_trend": float(latest_line - open_line) if pd.notnull(open_line) and pd.notnull(latest_line) else 0.0,
            "line_std": float(line_std),
            "books": float(row["latest_books"]) if pd.notnull(row["latest_books"]) else 0.0,
        }

    return features


def _build_external_stats_features(sport_key: str) -> dict[tuple[str, str], dict[str, float]]:
    stats_df = get_latest_stats_snapshots(sport_key)
    if stats_df.empty:
        return {}

    features: dict[tuple[str, str], dict[str, float]] = {}
    grouped = (
        stats_df.groupby(["player", "market"], dropna=False)
        .agg(
            season_average=("season_average", "mean"),
            recent_average=("recent_average", "mean"),
            last_5_average=("last_5_average", "mean"),
            trend=("trend", "mean"),
            sample_size=("sample_size", "max"),
        )
        .reset_index()
    )

    for _, row in grouped.iterrows():
        features[(str(row["player"]), str(row["market"]))] = {
            "season_average": float(row["season_average"]) if pd.notnull(row["season_average"]) else 0.0,
            "recent_average": float(row["recent_average"]) if pd.notnull(row["recent_average"]) else 0.0,
            "last_5_average": float(row["last_5_average"]) if pd.notnull(row["last_5_average"]) else 0.0,
            "trend": float(row["trend"]) if pd.notnull(row["trend"]) else 0.0,
            "sample_size": float(row["sample_size"]) if pd.notnull(row["sample_size"]) else 0.0,
        }

    return features


def build_live_projections_for_sports(sport_labels: list[str] | None = None) -> dict[str, int]:
    labels = sport_labels or [
        label for label in get_provider_labels("sportsgameodds")
        if label in {"NBA", "MLB"}
    ]

    inserted_by_label: dict[str, int] = {}
    created_at = datetime.now(timezone.utc)

    with SessionLocal() as db:
        for label in labels:
            sport_key = get_sport_config(label)["live_keys"][0]
            board = get_latest_board(sport_key=sport_key, is_dfs=False)
            player_market_priors = _build_player_market_priors(sport_key)
            event_market_history = _build_event_market_history_features(sport_key)
            external_stats = _build_external_stats_features(sport_key)

            if board.empty:
                inserted_by_label[label] = 0
                continue

            latest = board.sort_values("pulled_at").drop_duplicates(
                subset=["event_id", "book_key", "market", "player", "pick"],
                keep="last",
            )

            agg = (
                latest.groupby(["event_id", "market", "player", "side"], dropna=False)
                .agg(
                    consensus_line=("line", "mean"),
                    avg_price=("price", "mean"),
                    books=("book_key", "nunique"),
                )
                .reset_index()
            )

            wide = agg.pivot_table(
                index=["event_id", "market", "player"],
                columns="side",
                values=["consensus_line", "avg_price", "books"],
                aggfunc="first",
            )

            if wide.empty:
                inserted_by_label[label] = 0
                continue

            wide.columns = [
                f"{metric}_{side}" if side else metric
                for metric, side in wide.columns.to_flat_index()
            ]
            wide = wide.reset_index()

            latest_market_features = (
                latest.groupby(["event_id", "market", "player"], dropna=False)
                .agg(
                    latest_books=("book_key", "nunique"),
                    latest_line_std=("line", "std"),
                )
                .reset_index()
            )
            wide = wide.merge(
                latest_market_features,
                on=["event_id", "market", "player"],
                how="left",
            )

            rows_to_insert = []
            delete_keys = []

            for _, row in wide.iterrows():
                market_key = row["market"]
                player_name = row["player"]
                event_id = row["event_id"]

                over_line = row.get("consensus_line_over")
                under_line = row.get("consensus_line_under")
                consensus_line = over_line if pd.notnull(over_line) else under_line
                over_prob = american_to_probability(row.get("avg_price_over"))
                under_prob = american_to_probability(row.get("avg_price_under"))
                no_vig_over, no_vig_under = devig_pair(over_prob, under_prob)

                yes_prob = american_to_probability(row.get("avg_price_yes"))
                no_prob = american_to_probability(row.get("avg_price_no"))
                no_vig_yes, no_vig_no = devig_pair(yes_prob, no_prob)
                player_market_key = (str(player_name), str(market_key))
                event_market_key = (str(event_id), str(player_name), str(market_key))
                prior = player_market_priors.get(player_market_key, {})
                history_features = event_market_history.get(event_market_key, {})
                stats_features = external_stats.get(player_market_key, {})
                latest_line_std = float(row.get("latest_line_std", 0.0) or 0.0)
                line_agreement = max(0.25, min(1.0, 1.0 - (latest_line_std / 3.5)))
                line_trend = float(history_features.get("line_trend", 0.0))
                graded_samples = float(prior.get("graded_samples", 0.0))
                graded_weight = min(graded_samples, 10.0) / 10.0
                graded_delta = float(prior.get("avg_actual_minus_line", 0.0))
                graded_hit_rate = float(prior.get("graded_hit_rate", 0.5))
                stats_sample_size = float(stats_features.get("sample_size", 0.0))
                stats_weight = min(stats_sample_size, 20.0) / 20.0
                stats_anchor_values = [
                    value
                    for value in [
                        stats_features.get("season_average", 0.0),
                        stats_features.get("recent_average", 0.0),
                        stats_features.get("last_5_average", 0.0),
                    ]
                    if value not in (None, 0.0)
                ]
                stats_anchor = float(sum(stats_anchor_values) / len(stats_anchor_values)) if stats_anchor_values else 0.0
                stats_trend = float(stats_features.get("trend", 0.0))

                if consensus_line is None or pd.isna(consensus_line):
                    if no_vig_yes is None:
                        continue
                    projection, std_dev = _infer_binary_projection(
                        yes_prob=float(no_vig_yes),
                        market_key=market_key,
                    )
                    over_value = float(no_vig_yes)
                    under_value = float(no_vig_no if no_vig_no is not None else 1 - no_vig_yes)
                    books_total = float(row.get("books_yes", 0) or 0) + float(row.get("books_no", 0) or 0)
                    hybrid_yes = over_value
                    hybrid_yes += (graded_hit_rate - 0.5) * 0.18 * graded_weight
                    hybrid_yes += line_trend * 0.015 * line_agreement
                    hybrid_yes += stats_trend * 0.08 * stats_weight
                    over_value = min(max(hybrid_yes, 0.05), 0.95)
                    under_value = 1 - over_value
                    projection = over_value
                else:
                    if no_vig_over is None:
                        no_vig_over = 0.5
                        no_vig_under = 0.5

                    projection, std_dev = _infer_projection(
                        consensus_line=float(consensus_line),
                        over_prob=float(no_vig_over),
                        market_key=market_key,
                    )
                    over_value = float(no_vig_over)
                    under_value = float(no_vig_under if no_vig_under is not None else 1 - no_vig_over)
                    books_total = float(row.get("books_over", 0) or 0) + float(row.get("books_under", 0) or 0)
                    if stats_anchor:
                        projection = (projection * (1 - (0.22 * stats_weight))) + (stats_anchor * (0.22 * stats_weight))
                    projection += stats_trend * 0.35 * stats_weight
                    projection += (graded_delta * 0.35 * graded_weight * line_agreement)
                    projection += (line_trend * 0.25 * line_agreement)
                    if graded_hit_rate > 0.55:
                        projection += 0.12 * line_agreement
                    elif graded_hit_rate < 0.45 and graded_samples >= 3:
                        projection -= 0.12 * line_agreement

                    over_value = min(max(over_value + ((graded_hit_rate - 0.5) * 0.12 * graded_weight), 0.05), 0.95)
                    under_value = 1 - over_value

                confidence = min(
                    92.0,
                    56.0
                    + abs(over_value - 0.5) * 90
                    + min(books_total, 8),
                )
                confidence = min(
                    97.0,
                    confidence
                    + (graded_weight * 6.0)
                    + (stats_weight * 7.0)
                    + (line_agreement * 4.0)
                    + min(abs(line_trend) * 2.0, 5.0),
                )

                delete_keys.append((sport_key, event_id, player_name, market_key))
                rows_to_insert.append(
                    PropProjection(
                        sport_key=sport_key,
                        external_event_id=event_id,
                        player_name=player_name,
                        market_key=market_key,
                        projection=round(float(projection), 3),
                        std_dev=round(float(std_dev), 3),
                        over_prob=round(float(over_value), 4),
                        under_prob=round(float(under_value), 4),
                        confidence=round(float(confidence), 2),
                        model_name="hybrid_history_v1",
                        created_at=created_at,
                    )
                )

            for sport_value, event_id, player_name, market_key in delete_keys:
                db.execute(
                    delete(PropProjection).where(
                        PropProjection.sport_key == sport_value,
                        PropProjection.external_event_id == event_id,
                        PropProjection.player_name == player_name,
                        PropProjection.market_key == market_key,
                    )
                )

            if rows_to_insert:
                db.add_all(rows_to_insert)
            db.commit()
            inserted_by_label[label] = len(rows_to_insert)

    return inserted_by_label
