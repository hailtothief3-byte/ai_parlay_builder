import pandas as pd
from sqlalchemy import select

from db.session import SessionLocal
from db.models import MarketLine


def _coerce_sport_keys(sport_key: str | list[str]) -> list[str]:
    if isinstance(sport_key, str):
        return [sport_key]
    return [key for key in sport_key if key]


def get_latest_board(sport_key: str | list[str], is_dfs: bool | None = None):
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        stmt = select(MarketLine).where(MarketLine.sport_key.in_(sport_keys))

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
