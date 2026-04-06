import pandas as pd
from sqlalchemy import select

from db.session import SessionLocal
from db.models import MarketLine


def get_latest_board(sport_key: str, is_dfs: bool | None = None):
    with SessionLocal() as db:
        stmt = select(MarketLine).where(MarketLine.sport_key == sport_key)

        if is_dfs is not None:
            stmt = stmt.where(MarketLine.is_dfs == is_dfs)

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

        df = df.sort_values("pulled_at")
        df = df.drop_duplicates(
            subset=["event_id", "book_key", "market", "player", "pick", "line"],
            keep="last",
        )
        return df
