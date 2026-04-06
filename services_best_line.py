import pandas as pd


def get_best_available_lines(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    latest = df.sort_values("pulled_at").drop_duplicates(
        subset=["event_id", "book_key", "market", "player", "pick"],
        keep="last",
    )

    frames = []

    over_df = latest[latest["side"] == "over"].copy()
    if not over_df.empty:
        over_best = (
            over_df.sort_values(
                ["event_id", "market", "player", "line", "price"],
                ascending=[True, True, True, True, False],
            )
            .groupby(["event_id", "market", "player"], as_index=False)
            .first()
        )
        over_best["best_for"] = "over"
        frames.append(over_best)

    under_df = latest[latest["side"] == "under"].copy()
    if not under_df.empty:
        under_best = (
            under_df.sort_values(
                ["event_id", "market", "player", "line", "price"],
                ascending=[True, True, True, False, False],
            )
            .groupby(["event_id", "market", "player"], as_index=False)
            .first()
        )
        under_best["best_for"] = "under"
        frames.append(under_best)

    yes_df = latest[latest["side"] == "yes"].copy()
    if not yes_df.empty:
        yes_best = (
            yes_df.sort_values(
                ["event_id", "market", "player", "price"],
                ascending=[True, True, True, False],
            )
            .groupby(["event_id", "market", "player"], as_index=False)
            .first()
        )
        yes_best["best_for"] = "yes"
        frames.append(yes_best)

    no_df = latest[latest["side"] == "no"].copy()
    if not no_df.empty:
        no_best = (
            no_df.sort_values(
                ["event_id", "market", "player", "price"],
                ascending=[True, True, True, False],
            )
            .groupby(["event_id", "market", "player"], as_index=False)
            .first()
        )
        no_best["best_for"] = "no"
        frames.append(no_best)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)
