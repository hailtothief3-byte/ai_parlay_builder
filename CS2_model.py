def win_probability(team):
    return (
        team["rating"] * 0.4 +
        team["recent_form"] * 0.3 +
        team["map_win_rate"] * 0.3
    )
