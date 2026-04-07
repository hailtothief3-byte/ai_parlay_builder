from __future__ import annotations

import math

import pandas as pd


def _numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def american_to_decimal_profit_multiplier(price: float | None) -> float | None:
    if price is None or pd.isna(price):
        return None
    if price > 0:
        return float(price) / 100.0
    if price < 0:
        return 100.0 / abs(float(price))
    return None


def kelly_fraction(model_prob: float | None, price: float | None) -> float:
    if model_prob is None or pd.isna(model_prob):
        return 0.0
    b = american_to_decimal_profit_multiplier(price)
    if b is None or b <= 0:
        return 0.0
    p = max(0.0, min(1.0, float(model_prob)))
    q = 1.0 - p
    raw = ((b * p) - q) / b
    if not math.isfinite(raw):
        return 0.0
    return max(0.0, raw)


def recommend_stake_units(
    model_prob: float | None,
    price: float | None,
    edge: float | None,
    confidence: float | None,
    bankroll: float,
    unit_size: float,
    kelly_fraction_cap: float = 0.25,
    max_units: float = 3.0,
) -> dict[str, float]:
    raw_kelly = kelly_fraction(model_prob, price)
    edge_multiplier = max(0.25, min(1.5, 0.5 + float(edge or 0.0) * 8.0))
    confidence_multiplier = max(0.35, min(1.25, float(confidence or 50.0) / 80.0))
    adjusted_fraction = raw_kelly * kelly_fraction_cap * edge_multiplier * confidence_multiplier
    stake_dollars = max(0.0, bankroll * adjusted_fraction)
    recommended_units = 0.0 if unit_size <= 0 else min(max_units, stake_dollars / unit_size)
    return {
        "raw_kelly": round(raw_kelly, 4),
        "adjusted_fraction": round(adjusted_fraction, 4),
        "recommended_units": round(recommended_units, 2),
        "recommended_dollars": round(recommended_units * unit_size, 2),
    }


def annotate_stake_recommendations(
    df: pd.DataFrame,
    bankroll: float,
    unit_size: float,
    kelly_fraction_cap: float = 0.25,
    max_units: float = 3.0,
) -> pd.DataFrame:
    if df.empty:
        return df

    enriched = df.copy()
    recommendations = enriched.apply(
        lambda row: recommend_stake_units(
            model_prob=row.get("model_prob"),
            price=row.get("price"),
            edge=row.get("edge"),
            confidence=row.get("confidence"),
            bankroll=bankroll,
            unit_size=unit_size,
            kelly_fraction_cap=kelly_fraction_cap,
            max_units=max_units,
        ),
        axis=1,
    )
    enriched["kelly_raw"] = recommendations.map(lambda item: item["raw_kelly"])
    enriched["stake_fraction"] = recommendations.map(lambda item: item["adjusted_fraction"])
    enriched["recommended_units"] = recommendations.map(lambda item: item["recommended_units"])
    enriched["recommended_stake"] = recommendations.map(lambda item: item["recommended_dollars"])
    return enriched


def recommend_parlay_stake(
    legs_df: pd.DataFrame,
    bankroll: float,
    unit_size: float,
    base_fraction: float = 0.10,
    max_units: float = 2.0,
) -> dict[str, float]:
    if legs_df.empty:
        return {
            "parlay_model_prob": 0.0,
            "parlay_decimal_odds": 0.0,
            "parlay_edge": 0.0,
            "recommended_units": 0.0,
            "recommended_stake": 0.0,
            "avg_leg_units": 0.0,
            "singles_total_stake": 0.0,
        }

    probabilities = (
        _numeric_series(legs_df, "model_prob")
        if "model_prob" in legs_df.columns
        else _numeric_series(legs_df, "win_probability")
    )
    implied_probs = _numeric_series(legs_df, "implied_prob")
    leg_units = _numeric_series(legs_df, "recommended_units")

    decimal_multipliers = compute_parlay_decimal_multipliers(legs_df)

    parlay_model_prob = float(probabilities.prod()) if not probabilities.empty else 0.0
    parlay_implied_prob = float(implied_probs.prod()) if not implied_probs.empty else 0.0
    parlay_decimal_odds = float(pd.Series(decimal_multipliers).prod()) if decimal_multipliers else 0.0
    parlay_edge = max(0.0, parlay_model_prob - parlay_implied_prob)
    avg_leg_units = float(leg_units.mean()) if not leg_units.empty else 0.0
    singles_total_stake = round(float((leg_units * unit_size).sum()), 2)

    suggested_fraction = min(base_fraction, max(0.01, parlay_edge * 0.75))
    parlay_stake_dollars = bankroll * suggested_fraction
    parlay_units = 0.0 if unit_size <= 0 else min(max_units, parlay_stake_dollars / unit_size)

    return {
        "parlay_model_prob": round(parlay_model_prob, 4),
        "parlay_decimal_odds": round(parlay_decimal_odds, 3),
        "parlay_edge": round(parlay_edge, 4),
        "recommended_units": round(parlay_units, 2),
        "recommended_stake": round(parlay_units * unit_size, 2),
        "avg_leg_units": round(avg_leg_units, 2),
        "singles_total_stake": singles_total_stake,
    }


def compute_parlay_decimal_multipliers(legs_df: pd.DataFrame) -> list[float]:
    decimal_multipliers = []
    for price in _numeric_series(legs_df, "price").tolist():
        profit_multiplier = american_to_decimal_profit_multiplier(price)
        if profit_multiplier is None:
            continue
        decimal_multipliers.append(1.0 + profit_multiplier)
    return decimal_multipliers


def compute_parlay_decimal_odds(legs_df: pd.DataFrame) -> float:
    decimal_multipliers = compute_parlay_decimal_multipliers(legs_df)
    return float(pd.Series(decimal_multipliers).prod()) if decimal_multipliers else 0.0
