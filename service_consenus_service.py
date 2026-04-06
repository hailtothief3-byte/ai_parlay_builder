import pandas as pd


def build_consensus_lines(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    latest = df.sort_values("pulled_at").drop_duplicates(
        subset=["event_id", "book_key", "market", "player", "pick"],
        keep="last",
    )

    grouped = (
        latest.groupby(
            ["event_id", "market", "player", "pick", "side", "is_dfs"],
            dropna=False,
        )
        .agg(
            consensus_line=("line", "mean"),
            min_line=("line", "min"),
            max_line=("line", "max"),
            avg_price=("price", "mean"),
            books_count=("book_key", "nunique"),
        )
        .reset_index()
    )

    return grouped
