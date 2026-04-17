from systems.player import clamp_player


def update_household_bond(household_member: dict, amount: int) -> None:
    household_member["bond"] = max(0, min(100, household_member["bond"] + amount))


def strengthen_family_support(player: dict, amount: int) -> None:
    player["family_bond"] += amount
    player["hope"] += max(1, amount // 2)
    clamp_player(player)


def strain_family_support(player: dict, amount: int) -> None:
    player["family_bond"] -= amount
    player["stress"] += max(1, amount // 2)
    clamp_player(player)


def strengthen_relationship(player: dict, amount: int) -> None:
    player["relationship_trust"] += amount
    player["hope"] += max(1, amount // 3)
    clamp_player(player)


def strain_relationship(player: dict, amount: int) -> None:
    player["relationship_trust"] -= amount
    player["stress"] += max(1, amount // 2)
    clamp_player(player)
