import pandas as pd

from services.board_service import get_latest_board
from services.best_line_service import get_best_available_lines
from services.consensus_service import build_consensus_lines
from services.projection_service import get_latest_projections
from models.probability import prob_over, prob_under


DEFAULT_STD = {
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


def american_to_implied_prob(odds: float | None) -> float | None:
    if odds is None:
        return None
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)


def confidence_score(edge: float, books_count: int, line_delta: float) -> float:
    base = 50.0
    base += max(min(edge * 100, 30), -30)
    base += min(books_count * 2, 10)
    base += min(abs(line_delta) * 5, 10)
    return max(1.0, min(base, 99.0))


def scan_edges(sport_key: str, is_dfs: bool | None = None) -> pd.DataFrame:
    board = get_latest_board(sport_key=sport_key, is_dfs=is_dfs)
    projections = get_latest_projections(sport_key=sport_key)

    if board.empty or projections.empty:
        return pd.DataFrame()

    best_lines = get_best_available_lines(board)
    consensus = build_consensus_lines(board)

    merged = best_lines.merge(
        projections,
        on=["event_id", "player", "market"],
        how="inner",
    )

    merged = merged.merge(
        consensus[["event_id", "market", "player", "pick", "consensus_line", "books_count"]],
        on=["event_id", "market", "player", "pick"],
        how="left",
    )

    rows = []

    for _, row in merged.iterrows():
        market = row["market"]
        line = row["line"]
        projection = row["projection"]
        std = row["std_dev"] if pd.notnull(row["std_dev"]) else DEFAULT_STD.get(market, 5.0)
        implied = american_to_implied_prob(row["price"])

        if line is None or pd.isna(line):
            continue

        if row["best_for"] == "over":
            model_prob = prob_over(projection, line, std)
        elif row["best_for"] == "under":
            model_prob = prob_under(projection, line, std)
        else:
            model_prob = row.get("over_prob_model")

        if implied is None:
            continue

        edge = model_prob - implied
        line_delta = projection - line

        rows.append(
            {
                "event_id": row["event_id"],
                "player": row["player"],
                "market": market,
                "pick": row["pick"],
                "best_for": row["best_for"],
                "sportsbook": row["book"],
                "book_key": row["book_key"],
                "line": line,
                "price": row["price"],
                "projection": projection,
                "consensus_line": row.get("consensus_line"),
                "books_count": int(row.get("books_count", 1)) if pd.notnull(row.get("books_count")) else 1,
                "implied_prob": implied,
                "model_prob": model_prob,
                "edge": edge,
                "line_delta": line_delta,
                "confidence": confidence_score(
                    edge,
                    int(row.get("books_count", 1) or 1),
                    line_delta,
                ),
                "is_dfs": row["is_dfs"],
            }
        )

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df.sort_values(["edge", "confidence"], ascending=[False, False])
