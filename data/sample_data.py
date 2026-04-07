from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd

RNG = np.random.default_rng(42)

SPORT_IDENTITIES = {
    "NBA": {
        "players": [
            "Jalen Hart",
            "Marcus Vale",
            "Tyrese North",
            "Devin Stone",
            "Luka Frost",
            "Jaylen Cole",
            "Darius Wynn",
            "Caleb Rowe",
            "Malik Drew",
            "Andre Pace",
            "Noah Price",
            "Zion Brooks",
            "Mason Bell",
            "Jordan Pike",
            "Isaiah Cross",
            "Terrence Holt",
            "Kobe Shaw",
            "Evan Fields",
            "Micah Reed",
            "Jasper Long",
            "Elijah Snow",
            "Nico Banks",
            "Owen Blake",
            "Carter Hayes",
        ],
        "teams": [
            "Cleveland",
            "Indiana",
            "Boston",
            "Milwaukee",
            "Denver",
            "Phoenix",
        ],
    },
    "MLB": {
        "players": [
            "Aaron Pike",
            "Mason Reed",
            "Diego Lane",
            "Nolan Fox",
            "Corey Banks",
            "Evan West",
            "Logan Shaw",
            "Brady Miles",
            "Owen Cruz",
            "Tyler Hart",
            "Grant Cole",
            "Riley Ford",
            "Eli Ross",
            "Cole Harper",
            "Noah Dean",
            "Jace Wynn",
        ],
        "teams": [
            "Yankees",
            "Dodgers",
            "Braves",
            "Cubs",
            "Astros",
            "Phillies",
        ],
    },
    "NFL": {
        "players": [
            "C.J. Rivers",
            "Jayden Knox",
            "Malik Ford",
            "Aiden Price",
            "Trevor Hale",
            "Roman Bell",
            "Derrick Moss",
            "Landon Frost",
            "Caleb Wynn",
            "Ethan Brooks",
            "Micah Rowe",
            "Tyson Cole",
        ],
        "teams": [
            "Chiefs",
            "Bills",
            "Cowboys",
            "49ers",
            "Ravens",
            "Lions",
        ],
    },
    "NHL": {
        "players": [
            "Mika Rowan",
            "Lucas Vale",
            "Elias Hart",
            "Noah Quinn",
            "Parker Snow",
            "Logan Chase",
            "Owen Holt",
            "Caleb Frost",
            "Mason Dean",
            "Ryder West",
        ],
        "teams": [
            "Rangers",
            "Bruins",
            "Oilers",
            "Avalanche",
            "Stars",
            "Golden Knights",
        ],
    },
    "CS2": {
        "players": ["NiKo Prime", "Frozen Ace", "Ropz Echo", "M0nesy Beam", "Xantares Bolt", "Twistzz Nova"],
        "teams": ["Falcons", "Spirit", "Vitality", "FaZe", "Eternal Fire", "Liquid"],
    },
    "LOL": {
        "players": ["Faker Pulse", "Knight Drift", "Chovy Flux", "Ruler Spark", "Caps Orbit", "Guma Wave"],
        "teams": ["T1", "BLG", "Gen.G", "G2", "HLE", "TES"],
    },
    "DOTA2": {
        "players": ["Miracle Arc", "Yatoro Rift", "Nisha Core", "Topson Rune", "Ame Surge", "Quinn Ember"],
        "teams": ["Falcons", "Spirit", "Liquid", "Tundra", "XG", "GG"],
    },
}


@dataclass
class SlateRequest:
    sport: str
    n_rows: int = 20


def _bounded_normal(mean: float, std: float, low: float, high: float, size: int) -> np.ndarray:
    values = RNG.normal(mean, std, size)
    return np.clip(values, low, high)


def _build_named_sequence(values: list[str], size: int) -> list[str]:
    if not values:
        return [f"Player {i}" for i in range(1, size + 1)]
    return [values[idx % len(values)] for idx in range(size)]


def generate_prop_board(request: SlateRequest) -> pd.DataFrame:
    sport = request.sport.upper()
    n = request.n_rows
    identities = SPORT_IDENTITIES.get(sport, {"players": [], "teams": []})
    players = _build_named_sequence(identities.get("players", []), n)
    teams = _build_named_sequence(identities.get("teams", []), n)
    opponents = _build_named_sequence(list(reversed(identities.get("teams", []))), n)

    if sport == "NBA":
        markets = RNG.choice(["Points", "Rebounds", "Assists", "PRA", "First Basket"], n)
        lines = _bounded_normal(20, 6, 0.5, 45, n)
    elif sport == "MLB":
        markets = RNG.choice(["Hits", "Total Bases", "Home Run", "Pitcher Strikeouts"], n)
        lines = _bounded_normal(1.5, 1.2, 0.5, 8, n)
    elif sport == "NFL":
        markets = RNG.choice(["Passing Yards", "Rushing Yards", "Receptions", "Anytime TD"], n)
        lines = _bounded_normal(55, 35, 0.5, 325, n)
    elif sport == "NHL":
        markets = RNG.choice(["Shots on Goal", "Points", "Assists", "Anytime Goal"], n)
        lines = _bounded_normal(2.5, 1.1, 0.5, 6.5, n)
    elif sport == "CS2":
        markets = RNG.choice(["Kills", "Maps 2+ Kills", "Headshots", "Match Winner"], n)
        lines = _bounded_normal(16, 5, 0.5, 32, n)
    elif sport == "LOL":
        markets = RNG.choice(["Kills", "Assists", "Kills+Assists", "Team Winner"], n)
        lines = _bounded_normal(5, 2, 0.5, 16, n)
    elif sport == "DOTA2":
        markets = RNG.choice(["Kills", "Assists", "Fantasy Score", "Map Winner"], n)
        lines = _bounded_normal(6, 2.5, 0.5, 20, n)
    else:
        markets = np.array(["Projection"] * n)
        lines = _bounded_normal(10, 3, 0.5, 30, n)

    df = pd.DataFrame({
        "sport": sport,
        "player": players,
        "market": markets,
        "line": np.round(lines, 1),
        "team": teams,
        "opponent": opponents,
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
