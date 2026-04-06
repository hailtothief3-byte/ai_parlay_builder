def predict_prop(player_data, model, line):
    pred = model.predict([player_data])[0]

    from models.probability import prob_over
    over_prob = prob_over(pred, line)

    edge = over_prob - 0.5

    return {
        "projection": pred,
        "over_prob": over_prob,
        "edge": edge
    }
