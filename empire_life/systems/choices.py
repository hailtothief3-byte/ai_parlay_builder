import random

from systems.family import (
    strain_family_support,
    strain_relationship,
    strengthen_family_support,
    strengthen_relationship,
    update_household_bond,
)
from systems.player import clamp_player


def get_available_choices(game: dict) -> list[dict]:
    stage = game["player"]["life_stage"]
    flags = game["story_flags"]

    if stage == "Childhood":
        choices = [
            {
                "id": "focus_school",
                "label": "Focus on school",
                "description": "Build discipline and education, but carry some pressure.",
            },
            {
                "id": "help_family",
                "label": "Help your family",
                "description": "Support the household and strengthen loyalty.",
            },
            {
                "id": "listen_guardian",
                "label": "Listen to your guardian",
                "description": "Accept guidance and build trust at home.",
            },
            {
                "id": "follow_crowd",
                "label": "Run with the wrong crowd",
                "description": "Earn status early while temptation and influence grow.",
            },
            {
                "id": "stay_grounded",
                "label": "Stay grounded",
                "description": "Protect your peace and avoid unnecessary trouble.",
            },
        ]
        if flags["unlocked_school_path"]:
            choices.append(
                {
                    "id": "after_school_study",
                    "label": "Stay after school with a mentor",
                    "description": "Trade comfort for stronger long-term structure and confidence.",
                }
            )
        return choices

    if stage == "Teen":
        choices = [
            {
                "id": "study_hard",
                "label": "Study and plan ahead",
                "description": "Slow, safe progress that opens adult options later.",
            },
            {
                "id": "part_time_job",
                "label": "Work a part-time job",
                "description": "Bring in honest money while stress rises.",
            },
            {
                "id": "skip_school_for_cash",
                "label": "Skip school for quick cash",
                "description": "Relieve pressure now but weaken your long-term options.",
            },
            {
                "id": "street_hustle",
                "label": "Take a risky street move",
                "description": "Fast cash and respect, with legal and moral cost.",
            },
            {
                "id": "young_love",
                "label": "Get pulled into relationship drama",
                "description": "Emotional highs can damage focus and trust.",
            },
        ]
        if flags["unlocked_school_path"]:
            choices.append(
                {
                    "id": "exam_push",
                    "label": "Push for a major school breakthrough",
                    "description": "Harder effort now for stronger structural momentum and future access.",
                }
            )
        if flags["unlocked_risk_path"]:
            choices.append(
                {
                    "id": "organized_hustle",
                    "label": "Take a bigger organized hustle",
                    "description": "Higher payout and status, but sharper legal and moral consequences.",
                }
            )
        if flags["unlocked_community_path"]:
            choices.append(
                {
                    "id": "lead_youth_program",
                    "label": "Take a visible role in the community program",
                    "description": "Build local trust and confidence through responsibility.",
                }
            )
        return choices

    choices = [
        {
            "id": "steady_work",
            "label": "Commit to steady work",
            "description": "Stable progress, lower temptation, and a better future track.",
        },
        {
            "id": "pay_bills",
            "label": "Cover rent and bills",
            "description": "Protect housing stability and lower household pressure.",
        },
        {
            "id": "chase_status",
            "label": "Chase money and image",
            "description": "Looks good now, but debt and temptation often follow.",
        },
        {
            "id": "build_home",
            "label": "Invest in family and home",
            "description": "Protect relationships and reduce long-term emotional collapse.",
        },
        {
            "id": "illegal_play",
            "label": "Make a high-risk illegal play",
            "description": "Big reward, bigger danger, and consequences that linger.",
        },
        {
            "id": "community_presence",
            "label": "Show up for the neighborhood",
            "description": "Build reputation and safer local support over time.",
        },
    ]
    if flags["unlocked_school_path"]:
        choices.append(
            {
                "id": "certification_grind",
                "label": "Invest in training or certification",
                "description": "Slower now, but it compounds career stability and recovery momentum.",
            }
        )
    if flags["unlocked_risk_path"]:
        choices.append(
            {
                "id": "high_stakes_move",
                "label": "Make a high-stakes move",
                "description": "The biggest shortcut yet: serious money if it hits, serious fallout if it doesn't.",
            }
        )
    if flags["unlocked_family_path"]:
        choices.append(
            {
                "id": "family_sacrifice",
                "label": "Make a family-first sacrifice",
                "description": "Give up short-term gain to stabilize home life and trust.",
            }
        )
    if flags["unlocked_community_path"]:
        choices.append(
            {
                "id": "community_project",
                "label": "Lead a neighborhood project",
                "description": "Turn respect into visible impact and safer opportunities.",
            }
        )
    return choices


def process_choice(game: dict, choice_id: str) -> None:
    player = game["player"]
    log = game["log"]
    household = game["household"]
    world = game["world"]
    flags = game["story_flags"]

    if choice_id == "focus_school":
        player["education"] += 9
        player["school_record"] += 8
        player["discipline"] += 6
        player["stress"] += 3
        player["hope"] += 4
        _nudge_path(flags, structured=2)
        log.insert(0, "You invested in structure instead of chasing noise.")

    elif choice_id == "help_family":
        strengthen_family_support(player, 10)
        update_household_bond(household["guardian"], 8)
        update_household_bond(household["sibling"], 5)
        player["discipline"] += 3
        player["cash"] += 10
        _nudge_path(flags, family=2)
        log.insert(0, "You carried some weight at home and earned trust for it.")

    elif choice_id == "listen_guardian":
        update_household_bond(household["guardian"], 12)
        player["discipline"] += 4
        player["hope"] += 5
        player["school_record"] += 3
        player["stress"] -= 2
        _nudge_path(flags, structured=1, family=1)
        log.insert(0, f"{household['guardian']['name']} kept trying to guide you, and this time you listened.")

    elif choice_id == "follow_crowd":
        player["reputation"] += 8
        player["neighborhood_reputation"] += 4
        player["temptation"] += 10
        player["street_influence"] += 12
        player["discipline"] -= 7
        player["morality"] -= 4
        player["school_record"] -= 6
        player["confidence"] += 4
        strain_family_support(player, 8)
        update_household_bond(household["guardian"], -8)
        _nudge_path(flags, risk=2)
        log.insert(0, "Approval came quickly, and so did the pull of risk and image.")

    elif choice_id == "stay_grounded":
        player["stress"] -= 5
        player["discipline"] += 3
        player["hope"] += 3
        player["confidence"] += 2
        _nudge_path(flags, structured=1)
        log.insert(0, "You kept your peace and refused a shortcut that felt wrong.")

    elif choice_id == "after_school_study":
        player["education"] += 12
        player["school_record"] += 8
        player["discipline"] += 5
        player["stress"] += 4
        player["confidence"] += 4
        flags["mentor_support"] += 1
        _nudge_path(flags, structured=3)
        log.insert(0, "You stayed late, got sharper, and started taking yourself more seriously.")

    elif choice_id == "study_hard":
        player["education"] += 10
        player["school_record"] += 10
        player["discipline"] += 5
        player["stress"] += 4
        player["hope"] += 5
        _nudge_path(flags, structured=2)
        log.insert(0, "You chose the slow path, trusting that delayed wins still matter.")

    elif choice_id == "part_time_job":
        player["cash"] += 55
        player["income"] += 15
        player["stress"] += 6
        player["career_progress"] += 5
        player["discipline"] += 2
        player["confidence"] += 3
        update_household_bond(household["guardian"], 4)
        _nudge_path(flags, structured=1, family=1)
        log.insert(0, "Honest money gave you breathing room, even if it wore you down.")

    elif choice_id == "skip_school_for_cash":
        payout = random.randint(35, 75)
        player["cash"] += payout
        player["education"] -= 8
        player["school_record"] -= 12
        player["discipline"] -= 5
        player["temptation"] += 8
        player["stress"] += 5
        player["street_influence"] += 6
        player["confidence"] += 2
        update_household_bond(household["guardian"], -6)
        _nudge_path(flags, risk=2)
        log.insert(0, f"You made ${payout} instead of going to school, and the tradeoff will echo later.")

    elif choice_id == "street_hustle":
        payout = random.randint(85, 150)
        player["cash"] += payout
        player["reputation"] += 10
        player["neighborhood_reputation"] += 8
        player["legal_risk"] += 15
        player["street_influence"] += 15
        player["temptation"] += 12
        player["morality"] -= 10
        player["stress"] += 9
        player["confidence"] += 5
        _nudge_path(flags, risk=3)
        if random.random() < 0.35:
            penalty = random.randint(30, 90)
            player["cash"] = max(0, player["cash"] - penalty)
            player["hope"] -= 6
            log.insert(0, f"You made ${payout}, but chaos followed and cost you ${penalty}.")
        else:
            log.insert(0, f"Fast money hit: you made ${payout}, and it felt easier than it should have.")

    elif choice_id == "young_love":
        player["stress"] += 10
        player["discipline"] -= 4
        player["confidence"] += 3
        _nudge_path(flags, family=1)
        if not household["partner"]["active"]:
            household["partner"]["active"] = True
            household["partner"]["name"] = random.choice(["Ari", "Jada", "Milan", "Jordan", "Skye"])
        if random.random() < 0.5:
            strengthen_relationship(player, 8)
            update_household_bond(household["partner"], 10)
            player["hope"] += 6
            log.insert(0, f"Things got serious with {household['partner']['name']}, and love started affecting your decisions.")
        else:
            strain_relationship(player, 10)
            update_household_bond(household["partner"], -10)
            player["hope"] -= 5
            log.insert(0, "Relationship drama took your focus and shook your emotional stability.")

    elif choice_id == "exam_push":
        player["education"] += 12
        player["school_record"] += 10
        player["stress"] += 7
        player["discipline"] += 4
        player["confidence"] += 5
        flags["mentor_support"] += 1
        _nudge_path(flags, structured=3)
        log.insert(0, "You pushed through the pressure for a real academic breakthrough.")

    elif choice_id == "organized_hustle":
        payout = random.randint(140, 240)
        player["cash"] += payout
        player["reputation"] += 12
        player["street_influence"] += 14
        player["legal_risk"] += 18
        player["stress"] += 10
        player["morality"] -= 9
        _nudge_path(flags, risk=4)
        if random.random() < 0.42:
            fallout = random.randint(50, 120)
            player["cash"] = max(0, player["cash"] - fallout)
            player["hope"] -= 6
            log.insert(0, f"The bigger hustle paid ${payout}, but the fallout burned ${fallout} right back out of your life.")
        else:
            log.insert(0, f"You stepped into a bigger hustle and pulled ${payout} out of it.")

    elif choice_id == "lead_youth_program":
        player["neighborhood_reputation"] += 12
        player["confidence"] += 5
        player["discipline"] += 3
        player["hope"] += 4
        flags["community_path"] += 2
        _nudge_path(flags, community=4)
        log.insert(0, "You stopped just attending and started becoming someone younger kids could look at.")

    elif choice_id == "steady_work":
        player["cash"] += 120
        player["income"] += 40
        player["career_progress"] += 8
        player["discipline"] += 4
        player["stress"] += 5
        player["temptation"] -= 4
        player["legal_risk"] -= 5
        player["housing_stability"] += 4
        player["confidence"] += 3
        _nudge_path(flags, structured=2, family=1)
        log.insert(0, "You kept showing up, even when steady progress felt less glamorous.")

    elif choice_id == "pay_bills":
        bill_total = world["rent_due"] + world["monthly_bills"]
        if player["cash"] >= bill_total:
            player["cash"] -= bill_total
            player["debt"] = max(0, player["debt"] - 30)
            player["housing_stability"] += 12
            player["stress"] -= 6
            strengthen_family_support(player, 6)
            update_household_bond(household["guardian"], 6)
            _nudge_path(flags, family=2, structured=1)
            log.insert(0, f"You covered ${bill_total} in bills and kept the home stable for another month.")
        else:
            shortfall = bill_total - player["cash"]
            player["debt"] += shortfall
            player["cash"] = 0
            player["housing_stability"] -= 10
            player["stress"] += 8
            strain_family_support(player, 6)
            _nudge_path(flags, risk=1)
            log.insert(0, f"You came up short on bills, and ${shortfall} turned into new debt.")

    elif choice_id == "chase_status":
        player["cash"] += 140
        player["debt"] += 75
        player["temptation"] += 9
        player["stress"] += 8
        player["relationship_trust"] -= 6
        player["reputation"] += 6
        player["neighborhood_reputation"] += 5
        player["confidence"] += 5
        _nudge_path(flags, risk=2)
        log.insert(0, "You bought image and momentum, but some of it was built on borrowed stability.")

    elif choice_id == "build_home":
        player["cash"] = max(0, player["cash"] - 60)
        strengthen_family_support(player, 8)
        strengthen_relationship(player, 8)
        update_household_bond(household["guardian"], 6)
        if household["partner"]["active"]:
            update_household_bond(household["partner"], 8)
        player["stress"] -= 6
        player["hope"] += 6
        player["housing_stability"] += 8
        player["confidence"] += 2
        _nudge_path(flags, family=3)
        log.insert(0, "You chose home, responsibility, and a future that is harder to flex but easier to keep.")

    elif choice_id == "illegal_play":
        payout = random.randint(180, 280)
        player["cash"] += payout
        player["legal_risk"] += 20
        player["morality"] -= 12
        player["temptation"] += 10
        player["reputation"] += 12
        player["neighborhood_reputation"] += 10
        player["stress"] += 12
        player["confidence"] += 6
        strain_family_support(player, 6)
        update_household_bond(household["guardian"], -8)
        _nudge_path(flags, risk=3)
        if household["partner"]["active"]:
            update_household_bond(household["partner"], -6)
        if random.random() < 0.4:
            player["debt"] += 120
            player["hope"] -= 8
            log.insert(0, f"You scored ${payout}, but the fallout created new debt and pressure.")
        else:
            log.insert(0, f"You took a dangerous shortcut and walked away with ${payout}.")

    elif choice_id == "community_presence":
        player["cash"] = max(0, player["cash"] - 25)
        player["stress"] -= 4
        player["hope"] += 5
        player["neighborhood_reputation"] += 10
        player["reputation"] += 4
        player["legal_risk"] -= 4
        player["confidence"] += 3
        _nudge_path(flags, community=3)
        log.insert(0, "You showed up in the neighborhood as someone people could count on, not just notice.")

    elif choice_id == "certification_grind":
        player["cash"] = max(0, player["cash"] - 70)
        player["education"] += 10
        player["career_progress"] += 8
        player["confidence"] += 4
        player["stress"] += 5
        flags["recovery_steps"] += 1
        _nudge_path(flags, structured=3)
        log.insert(0, "You invested in a slower credential that could keep opening doors later.")

    elif choice_id == "high_stakes_move":
        payout = random.randint(260, 420)
        player["cash"] += payout
        player["reputation"] += 15
        player["legal_risk"] += 24
        player["stress"] += 14
        player["morality"] -= 12
        player["temptation"] += 8
        _nudge_path(flags, risk=5)
        if random.random() < 0.45:
            player["debt"] += 140
            player["housing_stability"] -= 8
            player["hope"] -= 8
            log.insert(0, f"The high-stakes move brought in ${payout}, but the blowback hit harder than the rush.")
        else:
            log.insert(0, f"You took the biggest shortcut yet and cleared ${payout}.")

    elif choice_id == "family_sacrifice":
        player["cash"] = max(0, player["cash"] - 90)
        player["family_bond"] += 10
        player["relationship_trust"] += 8
        player["housing_stability"] += 6
        player["hope"] += 5
        player["stress"] += 2
        _nudge_path(flags, family=4)
        log.insert(0, "You gave something up for home, and the people around you felt that choice immediately.")

    elif choice_id == "community_project":
        player["cash"] = max(0, player["cash"] - 60)
        player["neighborhood_reputation"] += 14
        player["career_progress"] += 5
        player["hope"] += 5
        player["confidence"] += 4
        _nudge_path(flags, community=4, structured=1)
        log.insert(0, "You turned local respect into visible action and gave people a reason to trust your name.")

    clamp_player(player)


def _nudge_path(flags: dict, structured: int = 0, risk: int = 0, family: int = 0, community: int = 0) -> None:
    flags["structured_momentum"] = max(0, flags["structured_momentum"] + structured - risk // 2)
    flags["risk_momentum"] = max(0, flags["risk_momentum"] + risk - structured // 2)
    flags["family_momentum"] = max(0, flags["family_momentum"] + family)
    flags["community_momentum"] = max(0, flags["community_momentum"] + community)
