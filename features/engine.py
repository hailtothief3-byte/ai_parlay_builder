from __future__ import annotations

import pandas as pd

from config import CONFIG


def add_projection_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["feature_score"] = (
        out["recent_form"] * CONFIG.default_weights["recent_form"]
        + out["matchup_score"] * CONFIG.default_weights["matchup"]
        + out["role_stability"] * 3 * CONFIG.default_weights["role_stability"]
        + out["market_signal"] * CONFIG.default_weights["market_signal"]
        + out["historical_hit_rate"] * 3 * CONFIG.default_weights["historical_hit_rate"]
        + out["data_quality"] * 3 * CONFIG.default_weights["data_quality"]
    )
    return out
