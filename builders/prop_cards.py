import pandas as pd


def _prettify_market_label(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    raw = str(value).strip()
    if not raw:
        return ""
    market_map = {
        "player_points": "Points",
        "player_rebounds": "Rebounds",
        "player_assists": "Assists",
        "player_points_rebounds_assists": "PRA",
        "player_threes": "3PT Made",
        "player_first_basket": "First Basket",
        "player_home_runs": "Home Runs",
        "player_hits": "Hits",
        "player_total_bases": "Total Bases",
        "player_strikeouts": "Strikeouts",
        "player_pass_yds": "Pass Yards",
        "player_rush_yds": "Rush Yards",
        "player_reception_yds": "Receiving Yards",
        "player_receptions": "Receptions",
    }
    return market_map.get(raw, raw.replace("_", " ").title())


def _format_card_number(value, digits: int = 2) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "-"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{numeric:.{digits}f}".rstrip("0").rstrip(".")


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
        market_label = _prettify_market_label(row.get("market"))
        pick_label = str(row.get("pick") or "").strip()
        line_label = _format_card_number(row.get("line"), digits=1)
        cards.append(
            {
                "title": f"{player_label} - {market_label}",
                "pick": " ".join(part for part in [pick_label, line_label] if part and part != "-"),
                "sportsbook": row["sportsbook"],
                "projection": round(float(row["projection"]), 2),
                "model_prob": round(float(row["model_prob"]) * 100, 2),
                "implied_prob": round(float(row["implied_prob"]) * 100, 2),
                "edge": round(float(row["edge"]) * 100, 2),
                "confidence": round(float(row["confidence"]), 1),
                "consensus_line": _format_card_number(row.get("consensus_line"), digits=2),
                "coverage_status": row.get("coverage_status", "Unknown"),
                "coverage_note": row.get("coverage_note", ""),
                "recommended_units": round(float(row.get("recommended_units", 0.0)), 2),
                "recommended_stake": round(float(row.get("recommended_stake", 0.0)), 2),
            }
        )

    return cards
