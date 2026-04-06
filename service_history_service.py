import pandas as pd
from sqlalchemy import select

from db.session import SessionLocal
from db.models import MarketLine


def get_line_history(
    sport_key: str,
    player_name: str | None = None,
    market_key: str | None = None,
    event_id: str | None = None,
):
    with SessionLocal() as db:
        stmt = select(MarketLine).where(MarketLine.sport_key == sport_key)

        if player_name:
            stmt = stmt.where(MarketLine.player_name == player_name)
        if market_key:
            stmt = stmt.where(MarketLine.market_key == market_key)
        if event_id:
            stmt = stmt.where(MarketLine.external_event_id == event_id)

        rows = db.execute(stmt).scalars().all()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            [
                {
                    "event_id": r.external_event_id,
                    "book": r.bookmaker_title,
                    "book_key": r.bookmaker_key,
                    "market": r.market_key,
                    "player": r.player_name,
                    "pick": r.outcome_name,
                    "line": r.line,
                    "price": r.price,
                    "side": r.side,
                    "is_dfs": r.is_dfs,
                    "commence_time": r.event_commence_time,
                    "last_update": r.last_update,
                    "pulled_at": r.pulled_at,
                }
                for r in rows
            ]
        )

        return df.sort_values(["player", "market", "book", "pulled_at"])
