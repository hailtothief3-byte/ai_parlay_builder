from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)


@dataclass
class SlateRequest:
    sport: str
    n_rows: int = 20


def _bounded_normal(mean: float, std: float, low: float, high: float, size: int) -> np.ndarray:
    values = RNG.normal(mean, std, size)
    return np.clip(values, low, high)


def generate_prop_board(request: SlateRequest) -> pd.DataFrame:
    sport = request.sport.upper()
    n = request.n_rows

    if sport == "NBA":
        players = [f"NBA Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Points", "Rebounds", "Assists", "PRA", "First Basket"], n)
        lines = _bounded_normal(20, 6, 0.5, 45, n)
    elif sport == "MLB":
        players = [f"MLB Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Hits", "Total Bases", "Home Run", "Pitcher Strikeouts"], n)
        lines = _bounded_normal(1.5, 1.2, 0.5, 8, n)
    elif sport == "NFL":
        players = [f"NFL Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Passing Yards", "Rushing Yards", "Receptions", "Anytime TD"], n)
        lines = _bounded_normal(55, 35, 0.5, 325, n)
    elif sport == "NHL":
        players = [f"NHL Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Shots on Goal", "Points", "Assists", "Anytime Goal"], n)
        lines = _bounded_normal(2.5, 1.1, 0.5, 6.5, n)
    elif sport == "CS2":
        players = [f"CS2 Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Kills", "Maps 2+ Kills", "Headshots", "Match Winner"], n)
        lines = _bounded_normal(16, 5, 0.5, 32, n)
    elif sport == "LOL":
        players = [f"LoL Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Kills", "Assists", "Kills+Assists", "Team Winner"], n)
        lines = _bounded_normal(5, 2, 0.5, 16, n)
    elif sport == "DOTA2":
        players = [f"DOTA2 Player {i}" for i in range(1, n + 1)]
        markets = RNG.choice(["Kills", "Assists", "Fantasy Score", "Map Winner"], n)
        lines = _bounded_normal(6, 2.5, 0.5, 20, n)
    else:
        players = [f"Player {i}" for i in range(1, n + 1)]
        markets = np.array(["Projection"] * n)
        lines = _bounded_normal(10, 3, 0.5, 30, n)

    df = pd.DataFrame({
        "sport": sport,
        "player": players,
        "market": markets,
        "line": np.round(lines, 1),
        "team": [f"Team {i % 6 + 1}" for i in range(n)],
        "opponent": [f"Opp {i % 6 + 1}" for i in range(n)],
        "recent_form": np.round(_bounded_normal(0.0, 1.0, -2.5, 2.5, n), 2),
        "matchup_score": np.round(_bounded_normal(0.0, 1.0, -2.5, 2.5, n), 2),
        "role_stability": np.round(_bounded_normal(0.75, 0.18, 0.1, 1.0, n), 2),
        "market_signal": np.round(_bounded_normal(0.0, 1.0, -2.0, 2.0, n), 2),
        "historical_hit_rate": np.round(_bounded_normal(0.55, 0.14, 0.15, 0.92, n), 2),
        "data_quality": np.round(_bounded_normal(0.80, 0.10, 0.4, 1.0, n), 2),
    })

    return df


def probability_from_edge(edge: float) -> float:
    return 1 / (1 + math.exp(-edge / 1.8))
