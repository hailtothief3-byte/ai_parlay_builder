CLAMPED_STATS = {
    "stress": (0, 100),
    "discipline": (0, 100),
    "morality": (0, 100),
    "temptation": (0, 100),
    "family_bond": (0, 100),
    "education": (0, 100),
    "relationship_trust": (0, 100),
    "legal_risk": (0, 100),
    "hope": (0, 100),
    "reputation": (-100, 100),
    "neighborhood_reputation": (-100, 100),
    "street_influence": (0, 100),
    "housing_stability": (0, 100),
    "school_record": (0, 100),
    "confidence": (0, 100),
}


def apply_background_bonus(game: dict, background: str) -> None:
    player = game["player"]
    log = game["log"]

    if background == "Struggling Household":
        player["cash"] -= 20
        player["stress"] += 10
        player["temptation"] += 12
        player["family_bond"] -= 8
        log.insert(0, "Money was tight growing up, and pressure arrived early.")
    elif background == "Street Exposure":
        player["reputation"] += 10
        player["street_influence"] += 18
        player["temptation"] += 18
        player["morality"] -= 6
        log.insert(0, "You grew up around risky influence, status games, and fast examples.")
    elif background == "Supportive Family":
        player["family_bond"] += 18
        player["discipline"] += 10
        player["education"] += 10
        player["hope"] += 10
        log.insert(0, "You started with support, structure, and people trying to keep you grounded.")
    else:
        log.insert(0, "Your early life felt balanced, but no path stays easy forever.")

    clamp_player(player)


def clamp_player(player: dict) -> None:
    player["cash"] = max(0, player["cash"])
    player["debt"] = max(0, player["debt"])
    player["career_progress"] = max(0, player["career_progress"])
    player["income"] = max(0, player["income"])
    for key, (low, high) in CLAMPED_STATS.items():
        player[key] = max(low, min(high, player[key]))
