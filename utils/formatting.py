from __future__ import annotations

import pandas as pd


DISPLAY_COLUMNS = [
    "sport", "player", "market", "line", "predicted_value", "pick",
    "win_probability", "confidence", "risk_flags"
]


def slim_display(df: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in DISPLAY_COLUMNS if c in df.columns]
    return df[cols].copy()
