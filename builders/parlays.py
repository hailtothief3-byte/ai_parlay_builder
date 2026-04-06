from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd


@dataclass
class ParlaySettings:
    legs: int = 3
    min_confidence: float = 75.0
    allow_same_team: bool = False
    style: str = "Balanced"


STYLE_BONUS = {
    "Safe": 5,
    "Balanced": 0,
    "Aggressive": -5,
}


def build_parlay(predictions: pd.DataFrame, settings: ParlaySettings) -> pd.DataFrame:
    df = predictions.copy()
    df = df[df["confidence"] >= settings.min_confidence].copy()
    df["adjusted_confidence"] = df["confidence"] + STYLE_BONUS.get(settings.style, 0)
    df = df.sort_values(["adjusted_confidence", "win_probability"], ascending=False)

    selected: List[int] = []
    used_teams = set()
    for idx, row in df.iterrows():
        if not settings.allow_same_team and row["team"] in used_teams:
            continue
        selected.append(idx)
        used_teams.add(row["team"])
        if len(selected) >= settings.legs:
            break

    out = df.loc[selected].copy()
    if out.empty:
        return out

    out["leg_rank"] = range(1, len(out) + 1)
    return out[[
        "leg_rank", "sport", "player", "market", "pick", "line", "predicted_value",
        "confidence", "win_probability", "risk_flags", "team", "opponent"
    ]].reset_index(drop=True)
