def k_projection(pitcher):
    return (
        pitcher["k_per_9"] *
        pitcher["expected_innings"] / 9
    )
