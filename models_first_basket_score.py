def first_basket_score(player):
    score = (
        player["first_shot_rate"] * 0.4 +
        player["usage_rate"] * 0.3 +
        player["tipoff_win_rate"] * 0.2 +
        player["minutes_share"] * 0.1
    )
    return score
