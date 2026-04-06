import pandas as pd
from sqlalchemy import select

from db.models import MarketLine
from db.session import SessionLocal


def _coerce_sport_keys(sport_key: str | list[str]) -> list[str]:
    if isinstance(sport_key, str):
        return [sport_key]
    return [key for key in sport_key if key]


def get_line_history(
    sport_key: str | list[str],
    player_name: str | None = None,
    market_key: str | None = None,
    event_id: str | None = None,
) -> pd.DataFrame:
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        stmt = select(MarketLine).where(MarketLine.sport_key.in_(sport_keys))

        if player_name:
            stmt = stmt.where(MarketLine.player_name.ilike(f"%{player_name.strip()}%"))
        if market_key:
            stmt = stmt.where(MarketLine.market_key == market_key)
        if event_id:
            stmt = stmt.where(MarketLine.external_event_id == event_id)

        rows = db.execute(stmt).scalars().all()

    if not rows:
        return pd.DataFrame()

    history = pd.DataFrame(
        [
            {
                "event_id": row.external_event_id,
                "book": row.bookmaker_title,
                "book_key": row.bookmaker_key,
                "market": row.market_key,
                "player": row.player_name,
                "pick": row.outcome_name,
                "line": row.line,
                "price": row.price,
                "side": row.side,
                "is_dfs": row.is_dfs,
                "commence_time": row.event_commence_time,
                "last_update": row.last_update,
                "pulled_at": row.pulled_at,
            }
            for row in rows
        ]
    )

    return history.sort_values(["pulled_at", "book", "market", "player"]).reset_index(drop=True)


def get_history_suggestions(
    sport_key: str | list[str],
    is_dfs: bool | None = None,
    limit_players: int = 8,
    limit_markets: int = 8,
) -> dict[str, list[str]]:
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return {"players": [], "markets": []}

    with SessionLocal() as db:
        stmt = select(MarketLine).where(MarketLine.sport_key.in_(sport_keys))

        if is_dfs is not None:
            stmt = stmt.where(MarketLine.is_dfs == is_dfs)

        rows = db.execute(stmt).scalars().all()

    if not rows:
        return {"players": [], "markets": []}

    history = pd.DataFrame(
        [
            {
                "player": row.player_name,
                "market": row.market_key,
            }
            for row in rows
            if row.player_name
        ]
    )

    if history.empty:
        return {"players": [], "markets": []}

    players = sorted(history["player"].dropna().unique().tolist())[:limit_players]
    markets = sorted(history["market"].dropna().unique().tolist())[:limit_markets]
    return {"players": players, "markets": markets}
