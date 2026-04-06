def hr_probability(player):
    return (
        player["barrel_rate"] * 0.4 +
        player["hard_hit"] * 0.3 +
        player["flyball_rate"] * 0.3
    )
