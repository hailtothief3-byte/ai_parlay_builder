from __future__ import annotations

import pandas as pd


def build_dfs_card(predictions: pd.DataFrame, app_name: str, legs: int = 4) -> pd.DataFrame:
    card = predictions.sort_values(["confidence", "win_probability"], ascending=False).head(legs).copy()
    card["app"] = app_name
    card["card_slot"] = range(1, len(card) + 1)
    return card[[
        "card_slot", "app", "sport", "player", "market", "pick", "line",
        "predicted_value", "confidence", "win_probability"
    ]].reset_index(drop=True)
