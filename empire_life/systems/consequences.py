from systems.player import clamp_player
from systems.events import maybe_trigger_event


def end_turn_update(game: dict) -> None:
    player = game["player"]
    game["turn"] += 1
    player["age"] += 1

    _advance_life_stage(game)
    _apply_milestones(game)
    _apply_passive_pressure(game)
    _apply_delayed_consequences(game)
    clamp_player(player)
    maybe_trigger_event(game)


def _apply_milestones(game: dict) -> None:
    player = game["player"]
    flags = game["story_flags"]
    seen = game["milestones_seen"]

    if "teen_transition" not in seen and player["life_stage"] == "Teen":
        seen.append("teen_transition")
        game["log"].insert(0, "Teen life hits differently now. Your choices start carrying social weight, not just household weight.")

    if "adult_transition" not in seen and player["life_stage"] == "Adult":
        seen.append("adult_transition")
        game["log"].insert(0, "Adulthood is here. Momentum matters more than intention now.")

    if not flags["unlocked_school_path"] and (player["education"] >= 68 or flags["structured_momentum"] >= 5):
        flags["unlocked_school_path"] = True
        game["log"].insert(0, "A more disciplined path is opening. Bigger educational and career-building choices are now available.")

    if not flags["unlocked_risk_path"] and (player["street_influence"] >= 35 or flags["risk_momentum"] >= 5):
        flags["unlocked_risk_path"] = True
        game["log"].insert(0, "Your risky reputation is growing. More dangerous, higher-reward choices are now on the table.")

    if not flags["unlocked_family_path"] and (player["family_bond"] >= 62 or flags["family_momentum"] >= 4):
        flags["unlocked_family_path"] = True
        game["log"].insert(0, "Your home life is becoming central. Deeper family-first choices are now available.")

    if not flags["unlocked_community_path"] and (player["neighborhood_reputation"] >= 28 or flags["community_momentum"] >= 4):
        flags["unlocked_community_path"] = True
        game["log"].insert(0, "People in the neighborhood are noticing you. Community-driven opportunities are now opening up.")


def _advance_life_stage(game: dict) -> None:
    player = game["player"]
    if player["age"] >= 18 and player["life_stage"] != "Adult":
        player["life_stage"] = "Adult"
        game["log"].insert(0, "You reached adulthood. Earlier habits now start shaping real outcomes.")
    elif player["age"] >= 13 and player["life_stage"] == "Childhood":
        player["life_stage"] = "Teen"
        game["log"].insert(0, "You entered your teen years. Influence, ego, and pressure feel stronger now.")


def _apply_passive_pressure(game: dict) -> None:
    player = game["player"]
    world = game["world"]
    player["stress"] += 2

    if player["debt"] > 0:
        player["stress"] += 4
        player["hope"] -= 2
        game["log"].insert(0, "Debt keeps stealing peace from the background.")

    if player["family_bond"] < 35:
        player["stress"] += 3
        game["log"].insert(0, "The distance at home is starting to affect how you carry everything else.")

    if player["life_stage"] == "Adult":
        bills = world["monthly_bills"] + max(0, world["rent_due"] - player["income"])
        if bills > 0:
            player["debt"] += bills
            player["housing_stability"] -= 6
            player["stress"] += 5
            game["log"].insert(0, f"Adult life kept moving. ${bills} in living costs hit whether you were ready or not.")


def _apply_delayed_consequences(game: dict) -> None:
    player = game["player"]
    flags = game["story_flags"]

    if player["temptation"] >= 70 and player["discipline"] <= 40:
        player["stress"] += 6
        player["morality"] -= 4
        player["legal_risk"] += 5
        game["log"].insert(0, "Temptation is becoming a habit instead of a passing thought.")

    if player["street_influence"] >= 65 and player["morality"] <= 35:
        player["legal_risk"] += 7
        player["relationship_trust"] -= 5
        game["log"].insert(0, "Street choices are starting to cost you trust and safety.")

    if player["education"] >= 70 and player["discipline"] >= 60:
        player["career_progress"] += 4
        player["hope"] += 4
        game["log"].insert(0, "Your consistency is turning into opportunities most people cannot see yet.")

    if player["school_record"] < 35 and player["life_stage"] in {"Childhood", "Teen"}:
        player["stress"] += 4
        player["hope"] -= 2
        game["log"].insert(0, "School problems are starting to follow you outside the classroom.")
        flags["dropout_risk"] += 1

    if player["school_record"] >= 70 and player["life_stage"] in {"Childhood", "Teen"}:
        player["confidence"] += 3
        player["hope"] += 2
        game["log"].insert(0, "Your strong school record is quietly building confidence and options.")

    if player["relationship_trust"] < 30:
        player["stress"] += 4
        player["hope"] -= 3
        game["log"].insert(0, "Relationship instability is bleeding into the rest of your life.")

    if player["has_child"]:
        player["stress"] += 4
        player["hope"] += 2
        if player["income"] < 70:
            player["debt"] += 40
            player["housing_stability"] -= 4
            game["log"].insert(0, "Trying to provide for a child raised the stakes on every money decision.")
        else:
            player["family_bond"] += 3
            game["log"].insert(0, "Responsibility is heavy, but stepping up is strengthening your sense of purpose.")

    if player["housing_stability"] < 35:
        player["stress"] += 5
        player["hope"] -= 4
        game["log"].insert(0, "Housing insecurity is making every other decision feel heavier.")

    if player["neighborhood_reputation"] >= 35 and player["legal_risk"] < 55:
        player["hope"] += 3
        player["career_progress"] += 2
        game["log"].insert(0, "Your neighborhood sees you as reliable, and that reputation is opening doors.")

    if player["income"] >= 80 and player["family_bond"] >= 55:
        player["housing_stability"] += 4
        game["log"].insert(0, "Steady income and family support are finally creating some breathing room.")

    if player["confidence"] < 30:
        player["stress"] += 3
        game["log"].insert(0, "Doubt is creeping into the way you see yourself and your future.")

    if flags["mentor_support"] >= 2:
        player["career_progress"] += 2
        player["hope"] += 2
        game["log"].insert(0, "Mentors are helping you see a path beyond survival mode.")

    if flags["community_path"] >= 3:
        player["neighborhood_reputation"] += 3
        player["hope"] += 2
        game["log"].insert(0, "Consistent community involvement is changing how people speak about you.")

    if flags["rivalry_level"] >= 3:
        player["stress"] += 4
        player["legal_risk"] += 4
        game["log"].insert(0, "A rivalry you kept feeding is starting to tax your peace and safety.")

    if flags["relationship_commitment"] >= 2:
        player["housing_stability"] += 2
        player["family_bond"] += 2
        game["log"].insert(0, "Commitment is creating a little more structure in your home life.")

    if flags["recovery_steps"] >= 2:
        player["career_progress"] += 2
        player["confidence"] += 2
        game["log"].insert(0, "The recovery work is starting to restore options that once felt closed.")

    if flags["provider_pressure"] >= 2:
        player["stress"] += 2
        player["family_bond"] += 2
        game["log"].insert(0, "Carrying more for others is exhausting, but it is also changing what your life stands for.")

    if flags["structured_momentum"] >= 5:
        player["confidence"] += 2
        player["career_progress"] += 1
        game["log"].insert(0, "Consistency is starting to compound into trust, confidence, and real options.")

    if flags["risk_momentum"] >= 5:
        player["temptation"] += 2
        player["stress"] += 2
        game["log"].insert(0, "Risk is no longer a momentary choice. It is becoming part of your identity.")

    if flags["family_momentum"] >= 4:
        player["hope"] += 2
        player["housing_stability"] += 1
        game["log"].insert(0, "Repeatedly choosing home is making your life feel steadier, even when it is not easy.")

    if flags["community_momentum"] >= 4:
        player["neighborhood_reputation"] += 2
        player["hope"] += 1
        game["log"].insert(0, "The neighborhood is starting to see a pattern in you, and that pattern matters.")


def check_end_state(game: dict) -> None:
    player = game["player"]
    flags = game["story_flags"]

    if player["stress"] >= 100:
        game["game_over"] = True
        game["ending"] = "burnout"
        game["ending_summary"] = "Pressure kept outpacing your support, and burnout ended the run before stability could form."
        game["log"].insert(0, "The pressure kept stacking until you burned out.")
        return

    if player["legal_risk"] >= 100:
        game["game_over"] = True
        game["ending"] = "legal_collapse"
        game["ending_summary"] = "Risk and short-term wins turned into consequences that finally closed in."
        game["log"].insert(0, "The consequences of your illegal choices finally caught up to you.")
        return

    if player["housing_stability"] <= 0:
        game["game_over"] = True
        game["ending"] = "housing_loss"
        game["ending_summary"] = "Debt, instability, and missed recovery windows became a housing crisis you could not outmaneuver."
        game["log"].insert(0, "Financial pressure finally took the roof from over your head.")
        return

    if player["hope"] <= 0 and player["family_bond"] < 25:
        game["game_over"] = True
        game["ending"] = "isolation"
        game["ending_summary"] = "Support systems fell away, hope collapsed, and isolation took control of the story."
        game["log"].insert(0, "Isolation took over before you could rebuild your footing.")
        return

    if (
        player["age"] >= 26
        and flags["creative_talent"] >= 3
        and player["confidence"] >= 60
        and player["legal_risk"] < 50
    ):
        game["game_over"] = True
        game["ending"] = "creative_breakthrough"
        game["ending_summary"] = "You protected your talent long enough for it to become a real path instead of a forgotten possibility."
        game["log"].insert(0, "Your creative discipline turned into a life direction few people saw coming.")
        return

    if (
        player["age"] >= 26
        and flags["athletic_talent"] >= 3
        and player["discipline"] >= 60
        and player["legal_risk"] < 50
    ):
        game["game_over"] = True
        game["ending"] = "athletic_breakthrough"
        game["ending_summary"] = "You treated your athletic talent like a lane, and that consistency paid off."
        game["log"].insert(0, "Discipline and talent combined into a path out of chaos.")
        return

    if (
        player["age"] >= 22
        and flags["dropout_risk"] >= 5
        and player["school_record"] < 35
        and player["career_progress"] < 15
    ):
        game["game_over"] = True
        game["ending"] = "lost_to_drift"
        game["ending_summary"] = "School disengagement, short-term choices, and lack of structure slowly pushed the story off course."
        game["log"].insert(0, "Too many small decisions pulled you out of structure before a better lane could form.")
        return

    if (
        player["age"] >= 26
        and player["cash"] >= 700
        and player["reputation"] >= 45
        and player["family_bond"] < 35
        and player["relationship_trust"] < 35
    ):
        game["game_over"] = True
        game["ending"] = "wealthy_but_isolated"
        game["ending_summary"] = "You built money and image, but the people closest to you no longer felt close at all."
        game["log"].insert(0, "You won status and cash, but it cost you emotional stability and trust.")
        return

    if (
        player["age"] >= 26
        and player["family_bond"] >= 65
        and player["has_child"]
        and player["housing_stability"] >= 55
        and flags["provider_pressure"] >= 2
        and player["legal_risk"] < 40
    ):
        game["game_over"] = True
        game["ending"] = "family_legacy"
        game["ending_summary"] = "You built a steadier home life and turned responsibility into a different kind of success."
        game["log"].insert(0, "Your story became less about image and more about what you were building for others.")
        return

    if (
        player["age"] >= 26
        and player["neighborhood_reputation"] >= 50
        and player["legal_risk"] < 45
        and player["career_progress"] >= 18
    ):
        game["game_over"] = True
        game["ending"] = "community_builder"
        game["ending_summary"] = "People in your neighborhood came to trust your name, and that trust created a meaningful life."
        game["log"].insert(0, "You became a stabilizing force in the neighborhood instead of another cautionary story.")
        return

    if (
        player["age"] >= 26
        and player["career_progress"] >= 25
        and player["family_bond"] >= 45
        and player["relationship_trust"] >= 40
        and player["housing_stability"] >= 50
        and player["hope"] >= 40
        and player["legal_risk"] < 55
    ):
        game["game_over"] = True
        game["ending"] = "stable_life"
        game["ending_summary"] = "You balanced pressure, responsibility, and opportunity well enough to build a stable adult life."
        game["log"].insert(0, "You built a life with support, stability, and options still ahead of you.")
