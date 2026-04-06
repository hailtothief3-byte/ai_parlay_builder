from __future__ import annotations

import math
import pandas as pd

from data.sample_data import probability_from_edge
from features.engine import add_projection_features


SPECIAL_MARKET_MULTIPLIERS = {
    "First Basket": 0.55,
    "Home Run": 0.60,
    "Pitcher Strikeouts": 1.20,
    "Match Winner": 1.10,
    "Team Winner": 1.10,
    "Map Winner": 1.10,
}


class PredictionEngine:
    def predict(self, board: pd.DataFrame) -> pd.DataFrame:
        df = add_projection_features(board)
        df["market_multiplier"] = df["market"].map(SPECIAL_MARKET_MULTIPLIERS).fillna(1.0)
        df["predicted_value"] = (df["line"] + df["feature_score"] * df["market_multiplier"]).round(2)
        df["edge"] = (df["predicted_value"] - df["line"]).round(2)
        df["over_probability"] = df["edge"].apply(probability_from_edge).round(3)
        df["under_probability"] = (1 - df["over_probability"]).round(3)
        df["pick"] = df.apply(lambda row: "Over" if row["over_probability"] >= 0.5 else "Under", axis=1)
        df["win_probability"] = df[["over_probability", "under_probability"]].max(axis=1).round(3)
        df["confidence"] = (
            (df["win_probability"] * 55)
            + (df["data_quality"] * 20)
            + (df["role_stability"] * 15)
            + (df["historical_hit_rate"] * 10)
        ).clip(1, 100).round(1)
        df["risk_flags"] = df.apply(self._risk_flags, axis=1)
        return df.sort_values(["confidence", "win_probability"], ascending=False).reset_index(drop=True)

    @staticmethod
    def _risk_flags(row: pd.Series) -> str:
        flags = []
        if row["data_quality"] < 0.65:
            flags.append("thin-data")
        if row["role_stability"] < 0.6:
            flags.append("volatile-role")
        if abs(row["matchup_score"]) > 1.8:
            flags.append("extreme-matchup")
        if row["market"] in {"First Basket", "Home Run", "Anytime TD", "Anytime Goal"}:
            flags.append("high-variance")
        return ", ".join(flags) if flags else "none"
