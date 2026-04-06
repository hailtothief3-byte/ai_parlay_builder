from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from builders.dfs_cards import build_dfs_card
from builders.parlays import ParlaySettings, build_parlay
from builders.slips import format_dfs_slip, format_sportsbook_slip
from data.sample_data import SlateRequest, generate_prop_board
from models.predictors import PredictionEngine


@dataclass
class ResearchBundle:
    board: pd.DataFrame
    predictions: pd.DataFrame


class ResearchService:
    def __init__(self) -> None:
        self.engine = PredictionEngine()

    def build_predictions(self, sport: str, n_rows: int = 24) -> ResearchBundle:
        board = generate_prop_board(SlateRequest(sport=sport, n_rows=n_rows))
        predictions = self.engine.predict(board)
        return ResearchBundle(board=board, predictions=predictions)

    def build_parlay_bundle(self, predictions: pd.DataFrame, legs: int, min_conf: float, allow_same_team: bool, style: str):
        settings = ParlaySettings(legs=legs, min_confidence=min_conf, allow_same_team=allow_same_team, style=style)
        parlay = build_parlay(predictions, settings)
        return parlay

    def sportsbook_slip_text(self, parlay_df: pd.DataFrame, book_name: str) -> str:
        return format_sportsbook_slip(parlay_df, book_name)

    def dfs_bundle(self, predictions: pd.DataFrame, app_name: str, legs: int):
        return build_dfs_card(predictions, app_name=app_name, legs=legs)

    def dfs_slip_text(self, card_df: pd.DataFrame, app_name: str) -> str:
        return format_dfs_slip(card_df, app_name)
