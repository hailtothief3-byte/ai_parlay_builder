from math import erf, sqrt


def normal_cdf(x: float, mean: float, std: float) -> float:
    if std <= 0:
        return 0.5
    z = (x - mean) / (std * sqrt(2))
    return 0.5 * (1 + erf(z))


def prob_over(projection: float, line: float, std: float) -> float:
    return 1.0 - normal_cdf(line, projection, std)


def prob_under(projection: float, line: float, std: float) -> float:
    return normal_cdf(line, projection, std)
