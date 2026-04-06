from __future__ import annotations

import pandas as pd


def format_sportsbook_slip(parlay_df: pd.DataFrame, book_name: str) -> str:
    if parlay_df.empty:
        return "No qualifying legs found for the selected settings."
    lines = [f"{book_name} slip", "-" * 28]
    for _, row in parlay_df.iterrows():
        lines.append(
            f"{int(row['leg_rank'])}. {row['player']} | {row['market']} | {row['pick']} {row['line']} "
            f"(proj {row['predicted_value']}, conf {row['confidence']})"
        )
    return "\n".join(lines)


def format_dfs_slip(card_df: pd.DataFrame, app_name: str) -> str:
    if card_df.empty:
        return "No qualifying DFS props found."
    lines = [f"{app_name} card", "-" * 28]
    for _, row in card_df.iterrows():
        lines.append(
            f"{int(row['card_slot'])}. {row['player']} | {row['pick']} {row['line']} {row['market']} "
            f"(conf {row['confidence']})"
        )
    return "\n".join(lines)
