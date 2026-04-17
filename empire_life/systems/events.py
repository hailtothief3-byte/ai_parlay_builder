import random

from systems.family import (
    strain_family_support,
    strain_relationship,
    strengthen_family_support,
    strengthen_relationship,
    update_household_bond,
)
from systems.player import clamp_player


YOUTH_NAMES = ["Jay", "Mia", "Andre", "Nia", "Chris", "Tori", "Malik", "Ava", "Dre", "Zoe"]
TEACHERS = ["Mr. Bennett", "Ms. Alvarez", "Coach Riley", "Ms. Carter"]
NEIGHBOR_ROLES = ["barber", "store owner", "older cousin", "basketball coach", "bus driver"]


def maybe_trigger_event(game: dict) -> None:
    if game.get("current_event") or game["game_over"]:
        return
    if game.get("event_cooldown", 0) > 0:
        game["event_cooldown"] -= 1
        return

    player = game["player"]
    flags = game["story_flags"]
    candidates = []

    if player["life_stage"] in {"Childhood", "Teen"}:
        candidates.extend(_build_youth_event_candidates(game))

    if player["life_stage"] in {"Teen", "Adult"} and player["stress"] >= 35:
        candidates.append(_guardian_argument_event(game))

    if player["life_stage"] == "Adult" and player["debt"] >= 80:
        candidates.append(_surprise_bill_event(game))

    if game["household"]["partner"]["active"] and player["relationship_trust"] <= 55:
        candidates.append(_partner_strain_event(game))
        candidates.append(_commitment_arc_event(game))

    if player["neighborhood_reputation"] >= 20 and player["legal_risk"] < 60:
        candidates.append(_job_opportunity_event(game))

    if player["life_stage"] == "Adult" and game["household"]["partner"]["active"] and not player["has_child"]:
        candidates.append(_pregnancy_event(game))

    if player["street_influence"] >= 40:
        candidates.append(_crew_pressure_event(game))

    if player["life_stage"] == "Adult":
        candidates.extend(_build_adult_followthrough_candidates(game))

    if not candidates:
        return

    event_chance = 0.38
    if player["life_stage"] == "Teen":
        event_chance = 0.46
    elif player["life_stage"] == "Adult":
        event_chance = 0.42
    if flags["risk_momentum"] >= 5 or player["stress"] >= 55:
        event_chance += 0.08
    if flags["structured_momentum"] >= 5 and player["stress"] < 40:
        event_chance -= 0.06

    if random.random() < max(0.2, min(0.62, event_chance)):
        game["current_event"] = random.choice(candidates)
        game["event_cooldown"] = 1


def resolve_event(game: dict, option_id: str) -> None:
    event = game.get("current_event")
    if not event:
        return

    option = next((item for item in event["options"] if item["id"] == option_id), None)
    if not option:
        return

    player = game["player"]
    household = game["household"]
    log = game["log"]
    flags = game["story_flags"]

    if option_id == "guardian_apologize":
        strengthen_family_support(player, 8)
        update_household_bond(household["guardian"], 10)
        player["discipline"] += 3
        player["stress"] -= 4
        flags["structured_momentum"] += 1
        flags["family_momentum"] += 1
        log.insert(0, f"You had the hard conversation with {household['guardian']['name']} instead of running from it.")

    elif option_id == "guardian_rebel":
        strain_family_support(player, 8)
        update_household_bond(household["guardian"], -12)
        player["reputation"] += 4
        player["street_influence"] += 8
        player["discipline"] -= 4
        flags["risk_momentum"] += 1
        log.insert(0, "You chose pride over peace at home, and the distance grew.")

    elif option_id == "bill_pay":
        payment = min(player["cash"], 120)
        player["cash"] -= payment
        player["debt"] = max(0, player["debt"] - payment)
        player["housing_stability"] += 8
        player["stress"] -= 5
        log.insert(0, f"You found ${payment} to handle the emergency bill and bought yourself some time.")

    elif option_id == "bill_delay":
        player["debt"] += 90
        player["housing_stability"] -= 8
        player["stress"] += 7
        player["hope"] -= 4
        log.insert(0, "You pushed the bill down the road, but it came back heavier.")

    elif option_id == "partner_reassure":
        strengthen_relationship(player, 10)
        update_household_bond(household["partner"], 12)
        player["stress"] -= 4
        log.insert(0, f"You slowed down and fought for trust with {household['partner']['name']}.")

    elif option_id == "partner_lie":
        strain_relationship(player, 12)
        update_household_bond(household["partner"], -14)
        player["stress"] += 5
        player["temptation"] += 4
        log.insert(0, "You protected the image in the moment, but trust took the hit.")

    elif option_id == "job_take":
        player["income"] += 45
        player["career_progress"] += 8
        player["hope"] += 6
        player["street_influence"] -= 4
        flags["structured_momentum"] += 2
        log.insert(0, "A local connection opened a real opportunity, and you stepped into it.")

    elif option_id == "job_ignore":
        player["temptation"] += 6
        player["street_influence"] += 6
        player["hope"] -= 4
        flags["risk_momentum"] += 1
        log.insert(0, "You let the opportunity pass, telling yourself something bigger would come.")

    elif option_id == "child_step_up":
        player["has_child"] = True
        player["discipline"] += 6
        player["stress"] += 8
        player["hope"] += 6
        player["housing_stability"] -= 4
        strengthen_relationship(player, 6)
        flags["family_momentum"] += 2
        log.insert(0, "The possibility of a child changed the way you looked at every next move.")

    elif option_id == "child_run":
        player["has_child"] = True
        player["hope"] -= 8
        player["family_bond"] -= 10
        player["relationship_trust"] -= 14
        player["stress"] += 10
        flags["risk_momentum"] += 1
        log.insert(0, "You ran from responsibility, and that decision changed how people saw you.")

    elif option_id == "crew_join":
        player["cash"] += 110
        player["reputation"] += 8
        player["street_influence"] += 12
        player["legal_risk"] += 12
        player["morality"] -= 8
        flags["risk_momentum"] += 2
        log.insert(0, "You accepted protection, money, and a deeper tie to the street.")

    elif option_id == "crew_refuse":
        player["stress"] += 5
        player["hope"] += 3
        player["discipline"] += 4
        player["street_influence"] -= 6
        flags["structured_momentum"] += 1
        log.insert(0, "You refused the pressure, knowing saying no carries its own cost.")

    elif option_id == "school_honest":
        player["school_record"] += 8
        player["discipline"] += 4
        player["stress"] += 2
        player["confidence"] += 3
        flags["dropout_risk"] = max(0, flags["dropout_risk"] - 1)
        log.insert(0, "You told the truth at school even though it cost you comfort in the moment.")

    elif option_id == "school_shortcut":
        player["school_record"] -= 10
        player["stress"] += 5
        player["temptation"] += 4
        player["confidence"] -= 2
        flags["dropout_risk"] += 1
        log.insert(0, "The shortcut felt easy, but now school sees you differently.")

    elif option_id == "friend_defuse":
        player["discipline"] += 3
        player["confidence"] += 4
        player["neighborhood_reputation"] += 4
        player["stress"] -= 2
        log.insert(0, "You kept a public moment from turning into a bigger problem.")

    elif option_id == "friend_perform":
        player["reputation"] += 6
        player["school_record"] -= 6
        player["temptation"] += 5
        player["confidence"] += 2
        log.insert(0, "People noticed you, but not necessarily for the right reason.")

    elif option_id == "mentor_accept":
        player["education"] += 8
        player["school_record"] += 6
        player["hope"] += 5
        player["confidence"] += 4
        flags["mentor_support"] += 1
        log.insert(0, "You accepted help and let someone older invest in your future.")

    elif option_id == "mentor_skip":
        player["street_influence"] += 5
        player["temptation"] += 5
        player["hope"] -= 3
        flags["mentor_support"] = max(0, flags["mentor_support"] - 1)
        log.insert(0, "You brushed off guidance and told yourself you'd figure it out alone.")

    elif option_id == "outside_help":
        player["neighborhood_reputation"] += 7
        player["family_bond"] += 4
        player["confidence"] += 3
        player["cash"] += 15
        flags["community_path"] += 1
        log.insert(0, "A small outside interaction became proof that being solid can build real trust.")

    elif option_id == "outside_flex":
        player["reputation"] += 7
        player["temptation"] += 6
        player["street_influence"] += 5
        player["confidence"] += 2
        flags["rivalry_level"] += 1
        log.insert(0, "You played to the crowd outside school, and that energy followed you home.")

    elif option_id == "police_calm":
        player["discipline"] += 4
        player["legal_risk"] -= 3
        player["stress"] += 4
        player["confidence"] += 2
        log.insert(0, "You kept calm through a tense moment outside and avoided making it worse.")

    elif option_id == "police_run":
        player["legal_risk"] += 10
        player["stress"] += 7
        player["school_record"] -= 4
        player["street_influence"] += 6
        flags["dropout_risk"] += 1
        log.insert(0, "Running felt natural in the moment, but it made the whole situation heavier.")

    elif option_id == "suspension_rebuild":
        player["school_record"] += 6
        player["discipline"] += 5
        player["stress"] += 3
        flags["school_suspensions"] += 1
        flags["dropout_risk"] = max(0, flags["dropout_risk"] - 1)
        log.insert(0, "The suspension hurt, but you treated it like a warning instead of an identity.")

    elif option_id == "suspension_double_down":
        player["school_record"] -= 12
        player["street_influence"] += 8
        player["temptation"] += 6
        flags["school_suspensions"] += 1
        flags["dropout_risk"] += 2
        log.insert(0, "The suspension pushed you further from school and closer to the wrong kind of momentum.")

    elif option_id == "program_join":
        player["confidence"] += 6
        player["hope"] += 5
        player["neighborhood_reputation"] += 5
        flags["community_path"] += 2
        flags["community_momentum"] += 2
        log.insert(0, "You joined something structured and let purpose take up more space in your life.")

    elif option_id == "program_ignore":
        player["temptation"] += 4
        player["confidence"] -= 2
        flags["community_path"] = max(0, flags["community_path"] - 1)
        flags["risk_momentum"] += 1
        log.insert(0, "You ignored the program, and the day drifted back toward old habits.")

    elif option_id == "talent_commit":
        player["confidence"] += 7
        player["discipline"] += 4
        player["education"] += 3
        if event.get("talent_type") == "creative":
            flags["creative_talent"] += 2
        else:
            flags["athletic_talent"] += 2
        log.insert(0, "You committed to a talent that could become a real lane if you stay consistent.")

    elif option_id == "talent_waste":
        player["confidence"] -= 3
        player["temptation"] += 4
        log.insert(0, "You let a real opportunity drift by because something louder felt more immediate.")

    elif option_id == "rival_deescalate":
        player["discipline"] += 4
        player["stress"] -= 2
        flags["rivalry_level"] = max(0, flags["rivalry_level"] - 1)
        flags["structured_momentum"] += 1
        log.insert(0, "You chose not to turn tension into a longer war.")

    elif option_id == "rival_escalate":
        player["reputation"] += 6
        player["stress"] += 6
        player["legal_risk"] += 6
        flags["rivalry_level"] += 2
        flags["risk_momentum"] += 2
        log.insert(0, "The issue got bigger because being seen as weak felt worse than the fallout.")

    elif option_id == "commit_partner":
        strengthen_relationship(player, 10)
        update_household_bond(household["partner"], 8)
        player["housing_stability"] += 4
        flags["relationship_commitment"] += 2
        flags["family_momentum"] += 2
        log.insert(0, "You made a real commitment and accepted the responsibility that comes with it.")

    elif option_id == "avoid_partner":
        strain_relationship(player, 8)
        player["stress"] += 5
        flags["relationship_commitment"] = max(0, flags["relationship_commitment"] - 1)
        flags["risk_momentum"] += 1
        log.insert(0, "You kept dodging commitment, and the relationship started to feel less secure.")

    elif option_id == "rebuild_education":
        player["education"] += 8
        player["school_record"] += 6
        player["confidence"] += 4
        player["hope"] += 5
        flags["recovery_steps"] += 2
        flags["dropout_risk"] = max(0, flags["dropout_risk"] - 2)
        flags["structured_momentum"] += 2
        log.insert(0, "You chose to rebuild your educational footing instead of carrying old labels forever.")

    elif option_id == "ignore_education":
        player["hope"] -= 4
        player["confidence"] -= 3
        player["temptation"] += 3
        log.insert(0, "You told yourself it was too late to circle back, and the gap stayed open.")

    elif option_id == "mentor_referral_take":
        player["career_progress"] += 10
        player["income"] += 35
        player["hope"] += 5
        flags["mentor_support"] += 1
        flags["structured_momentum"] += 2
        log.insert(0, "A mentor vouched for you, and you treated the referral like a real opening.")

    elif option_id == "mentor_referral_waste":
        player["hope"] -= 5
        player["confidence"] -= 2
        player["street_influence"] += 4
        log.insert(0, "You let a real recommendation go cold, and that kind of trust is hard to earn twice.")

    elif option_id == "showcase_prepare":
        player["confidence"] += 6
        player["discipline"] += 4
        player["hope"] += 5
        flags["structured_momentum"] += 1
        if event.get("talent_type") == "creative":
            flags["creative_talent"] += 1
        else:
            flags["athletic_talent"] += 1
        log.insert(0, "You took the showcase seriously and acted like your talent deserved structure.")

    elif option_id == "showcase_skip":
        player["confidence"] -= 4
        player["temptation"] += 4
        log.insert(0, "You skipped the moment and told yourself there would always be another chance.")

    elif option_id == "provider_step_up":
        player["income"] += 25
        player["housing_stability"] += 5
        player["stress"] += 5
        player["hope"] += 4
        flags["provider_pressure"] += 2
        flags["relationship_commitment"] += 1
        flags["family_momentum"] += 2
        log.insert(0, "You stepped harder into the provider role, even knowing it would cost peace in the short term.")

    elif option_id == "provider_escape":
        player["stress"] -= 2
        player["relationship_trust"] -= 8
        player["family_bond"] -= 5
        player["hope"] -= 5
        flags["provider_pressure"] = max(0, flags["provider_pressure"] - 1)
        flags["risk_momentum"] += 1
        log.insert(0, "You avoided the weight for now, but the people around you felt the gap immediately.")

    clamp_player(player)
    game["current_event"] = None


def _build_youth_event_candidates(game: dict) -> list[dict]:
    player = game["player"]
    flags = game["story_flags"]
    candidates = [
        _school_peer_event(game),
        _outside_school_event(game),
    ]

    if player["school_record"] <= 55 or player["stress"] >= 28:
        candidates.append(_school_discipline_event(game))

    if player["school_record"] >= 60 or player["education"] >= 60:
        candidates.append(_mentor_event(game))

    if player["street_influence"] >= 18 or player["neighborhood_reputation"] >= 10:
        candidates.append(_police_or_pressure_event(game))

    if player["school_record"] <= 38 or flags["dropout_risk"] >= 2:
        candidates.append(_suspension_arc_event(game))

    if player["neighborhood_reputation"] >= 14 or flags["mentor_support"] >= 1:
        candidates.append(_community_program_event(game))

    if player["confidence"] >= 55 and (player["education"] >= 58 or player["discipline"] >= 55):
        candidates.append(_talent_path_event(game))

    if flags["rivalry_level"] >= 2 or player["reputation"] >= 18:
        candidates.append(_rival_arc_event(game))

    if flags["community_momentum"] >= 3:
        candidates.append(_community_program_event(game))

    return candidates


def _build_adult_followthrough_candidates(game: dict) -> list[dict]:
    player = game["player"]
    flags = game["story_flags"]
    candidates = []

    if flags["dropout_risk"] >= 3 or flags["school_suspensions"] >= 2:
        candidates.append(_education_recovery_event(game))

    if flags["mentor_support"] >= 2 and player["career_progress"] >= 10:
        candidates.append(_mentor_referral_event(game))

    if flags["creative_talent"] >= 2 or flags["athletic_talent"] >= 2:
        candidates.append(_adult_showcase_event(game))

    if player["has_child"] or flags["relationship_commitment"] >= 2:
        candidates.append(_provider_pressure_event(game))

    return candidates


def _build_dynamic_scene(game: dict, mood: str) -> str:
    world = game["world"]
    place = random.choice(world["favorite_hangouts"])
    name = random.choice(YOUTH_NAMES)

    if mood == "peer":
        openings = [
            f"At {world['school_name']}, {name} starts a scene that puts everybody's eyes on you.",
            f"Between classes, {name} says something loud enough for half the hallway to hear.",
            f"At lunch, a small disagreement with {name} starts turning into a public test.",
        ]
    else:
        openings = [
            f"After school near the {place}, a small moment starts becoming a decision point.",
            f"Walking past the {place}, somebody recognizes you and starts pressing a situation.",
            f"Outside school by the {place}, the day keeps going and the pressure follows you there.",
        ]

    return random.choice(openings)


def _guardian_argument_event(game: dict) -> dict:
    guardian = game["household"]["guardian"]["name"]
    return {
        "title": "Guardian Argument",
        "body": f"{guardian} is tired of your attitude, stress, and half-explanations. This could become a turning point at home.",
        "options": [
            {"id": "guardian_apologize", "label": "Own your part and try to repair it"},
            {"id": "guardian_rebel", "label": "Push back and leave angry"},
        ],
    }


def _school_discipline_event(game: dict) -> dict:
    teacher = random.choice(TEACHERS)
    return {
        "title": "School Pressure",
        "body": f"{teacher} caught something off in your work or behavior. You can own it now, or try to slide past it.",
        "options": [
            {"id": "school_honest", "label": "Be honest and take the consequence"},
            {"id": "school_shortcut", "label": "Lie and try to get away with it"},
        ],
    }


def _school_peer_event(game: dict) -> dict:
    return {
        "title": "School Hallway Moment",
        "body": _build_dynamic_scene(game, "peer"),
        "options": [
            {"id": "friend_defuse", "label": "Defuse it and keep moving"},
            {"id": "friend_perform", "label": "Put on a show and win the crowd"},
        ],
    }


def _mentor_event(game: dict) -> dict:
    teacher = random.choice(TEACHERS)
    return {
        "title": "Unexpected Support",
        "body": f"{teacher} sees more in you than most people do and offers extra help after school. It is not flashy, but it is real.",
        "options": [
            {"id": "mentor_accept", "label": "Accept the help"},
            {"id": "mentor_skip", "label": "Skip it and do your own thing"},
        ],
    }


def _outside_school_event(game: dict) -> dict:
    role = random.choice(NEIGHBOR_ROLES)
    return {
        "title": "After-School Interaction",
        "body": f"{_build_dynamic_scene(game, 'outside')} A neighborhood {role} asks something simple of you, and people are watching how you respond.",
        "options": [
            {"id": "outside_help", "label": "Help and handle it right"},
            {"id": "outside_flex", "label": "Turn it into a status moment"},
        ],
    }


def _police_or_pressure_event(game: dict) -> dict:
    return {
        "title": "Outside Pressure",
        "body": "A tense moment builds outside school when adults start asking questions and everyone around you reacts fast.",
        "options": [
            {"id": "police_calm", "label": "Stay calm and cooperate"},
            {"id": "police_run", "label": "Run because it feels safer"},
        ],
    }


def _surprise_bill_event(game: dict) -> dict:
    return {
        "title": "Unexpected Bill",
        "body": "A repair bill hit at the worst possible time. Ignoring it may spiral into a deeper housing problem.",
        "options": [
            {"id": "bill_pay", "label": "Scrape together what you can and pay now"},
            {"id": "bill_delay", "label": "Delay it and hope you recover later"},
        ],
    }


def _suspension_arc_event(game: dict) -> dict:
    teacher = random.choice(TEACHERS)
    return {
        "title": "Suspension Risk",
        "body": f"{teacher} warns that one more incident could turn into a suspension. This is one of those moments that can push school from obstacle into exit ramp.",
        "options": [
            {"id": "suspension_rebuild", "label": "Take the warning seriously"},
            {"id": "suspension_double_down", "label": "Act like school already gave up on you"},
        ],
    }


def _community_program_event(game: dict) -> dict:
    return {
        "title": "Community Program",
        "body": "A neighborhood coach and a few adults are trying to pull kids into something structured after school. It is not glamorous, but it is a real alternative.",
        "options": [
            {"id": "program_join", "label": "Join and show up consistently"},
            {"id": "program_ignore", "label": "Ignore it and stay unstructured"},
        ],
    }


def _talent_path_event(game: dict) -> dict:
    talent_type = random.choice(["creative", "athletic"])
    if talent_type == "creative":
        body = "Someone notices your creative side and says you could take music, writing, or media seriously if you stopped treating it like a side thought."
    else:
        body = "A coach sees potential in your athletic discipline and tells you there is room for you if you actually commit."
    return {
        "title": "Talent Path",
        "body": body,
        "talent_type": talent_type,
        "options": [
            {"id": "talent_commit", "label": "Commit and build the skill"},
            {"id": "talent_waste", "label": "Let it slide for now"},
        ],
    }


def _rival_arc_event(game: dict) -> dict:
    rival = random.choice(YOUTH_NAMES)
    return {
        "title": "Friend Becoming Rival",
        "body": f"Things with {rival} have shifted from jokes and tension into something more serious. How you handle this could shape your name for years.",
        "options": [
            {"id": "rival_deescalate", "label": "De-escalate and move differently"},
            {"id": "rival_escalate", "label": "Escalate and protect your image"},
        ],
    }


def _commitment_arc_event(game: dict) -> dict:
    partner = game["household"]["partner"]["name"]
    return {
        "title": "Commitment Test",
        "body": f"{partner} wants to know if this relationship is becoming real or just convenient. Avoiding the answer will become an answer of its own.",
        "options": [
            {"id": "commit_partner", "label": "Commit and start acting like it"},
            {"id": "avoid_partner", "label": "Avoid the conversation"},
        ],
    }


def _education_recovery_event(game: dict) -> dict:
    return {
        "title": "Education Recovery",
        "body": "A late chance opens to repair old school damage through classes, certification, or a GED track. It is humbling, but it could redirect the whole run.",
        "options": [
            {"id": "rebuild_education", "label": "Take the recovery path seriously"},
            {"id": "ignore_education", "label": "Leave that chapter closed"},
        ],
    }


def _mentor_referral_event(game: dict) -> dict:
    return {
        "title": "Mentor Referral",
        "body": "Someone who kept believing in you is willing to put your name on the line for a real opening. This is one of those moments where preparation and trust finally meet.",
        "options": [
            {"id": "mentor_referral_take", "label": "Show up and honor the referral"},
            {"id": "mentor_referral_waste", "label": "Let it slip and keep drifting"},
        ],
    }


def _adult_showcase_event(game: dict) -> dict:
    talent_type = "creative" if game["story_flags"]["creative_talent"] >= game["story_flags"]["athletic_talent"] else "athletic"
    body = (
        "A local showcase could turn your creative work into a real next step if you treat it professionally."
        if talent_type == "creative"
        else "A trial or showcase is opening up, and your athletic preparation finally has somewhere concrete to go."
    )
    return {
        "title": "Adult Showcase",
        "body": body,
        "talent_type": talent_type,
        "options": [
            {"id": "showcase_prepare", "label": "Prepare properly and show up ready"},
            {"id": "showcase_skip", "label": "Skip it and keep moving aimlessly"},
        ],
    }


def _provider_pressure_event(game: dict) -> dict:
    return {
        "title": "Provider Pressure",
        "body": "Home needs more from you right now. The real question is whether you step deeper into responsibility or keep trying to protect your freedom.",
        "options": [
            {"id": "provider_step_up", "label": "Step up and carry more"},
            {"id": "provider_escape", "label": "Pull back and protect your space"},
        ],
    }


def _partner_strain_event(game: dict) -> dict:
    partner = game["household"]["partner"]["name"]
    return {
        "title": "Relationship Strain",
        "body": f"{partner} feels you are emotionally somewhere else. They want honesty, not charm.",
        "options": [
            {"id": "partner_reassure", "label": "Slow down and be honest"},
            {"id": "partner_lie", "label": "Say what keeps the peace for now"},
        ],
    }


def _job_opportunity_event(game: dict) -> dict:
    return {
        "title": "Neighborhood Opportunity",
        "body": "Someone in the neighborhood is willing to put your name forward for stable work, but you have to show up right.",
        "options": [
            {"id": "job_take", "label": "Take the opportunity seriously"},
            {"id": "job_ignore", "label": "Pass and keep chasing faster money"},
        ],
    }


def _pregnancy_event(game: dict) -> dict:
    partner = game["household"]["partner"]["name"]
    return {
        "title": "Life-Changing News",
        "body": f"{partner} tells you there may be a child involved. This is the kind of moment that can redirect a whole life.",
        "options": [
            {"id": "child_step_up", "label": "Step up and face it"},
            {"id": "child_run", "label": "Panic and pull away"},
        ],
    }


def _crew_pressure_event(game: dict) -> dict:
    return {
        "title": "Crew Pressure",
        "body": "People around you are pushing hard for loyalty, saying it is time to stop playing both sides.",
        "options": [
            {"id": "crew_join", "label": "Lean in and take the protection"},
            {"id": "crew_refuse", "label": "Refuse and stay independent"},
        ],
    }
