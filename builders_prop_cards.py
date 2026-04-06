import pandas as pd


def build_prop_cards(edge_df: pd.DataFrame, top_n: int = 10) -> list[dict]:
    if edge_df.empty:
        return []

    cards = []
    top = edge_df.head(top_n)

    for _, row in top.iterrows():
        cards.append(
            {
                "title": f"{row['player']} - {row['market']}",
                "pick": f"{row['pick']} {row['line']}",
                "sportsbook": row["sportsbook"],
                "projection": round(float(row["projection"]), 2),
                "model_prob": round(float(row["model_prob"]) * 100, 1),
                "implied_prob": round(float(row["implied_prob"]) * 100, 1),
                "edge": round(float(row["edge"]) * 100, 2),
                "confidence": round(float(row["confidence"]), 1),
                "consensus_line": None if pd.isna(row["consensus_line"]) else round(float(row["consensus_line"]), 2),
            }
        )

    return cards
