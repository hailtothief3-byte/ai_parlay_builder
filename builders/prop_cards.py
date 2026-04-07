import pandas as pd


def build_prop_cards(edge_df: pd.DataFrame, top_n: int = 10) -> list[dict]:
    if edge_df.empty:
        return []

    cards = []
    top = edge_df.head(top_n)

    for _, row in top.iterrows():
        player_label = row.get("player_display") or (
            f"{row['player']} ({row.get('player_team')})"
            if row.get("player_team")
            else row["player"]
        )
        cards.append(
            {
                "title": f"{player_label} - {row['market']}",
                "pick": f"{row['pick']} {row['line']}",
                "sportsbook": row["sportsbook"],
                "projection": round(float(row["projection"]), 2),
                "model_prob": round(float(row["model_prob"]) * 100, 2),
                "implied_prob": round(float(row["implied_prob"]) * 100, 2),
                "edge": round(float(row["edge"]) * 100, 2),
                "confidence": round(float(row["confidence"]), 1),
                "consensus_line": row["consensus_line"],
                "coverage_status": row.get("coverage_status", "Unknown"),
                "coverage_note": row.get("coverage_note", ""),
                "recommended_units": round(float(row.get("recommended_units", 0.0)), 2),
                "recommended_stake": round(float(row.get("recommended_stake", 0.0)), 2),
            }
        )

    return cards
