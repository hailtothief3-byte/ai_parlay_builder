from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import select

from db.models import PropProjection
from db.session import SessionLocal


def _coerce_sport_keys(sport_key: str | list[str]) -> list[str]:
    if isinstance(sport_key, str):
        return [sport_key]
    return [key for key in sport_key if key]


def save_projection(
    sport_key: str,
    external_event_id: str,
    player_name: str,
    market_key: str,
    projection: float,
    std_dev: float,
    over_prob: float,
    under_prob: float,
    confidence: float,
    model_name: str = "baseline_v1",
):
    with SessionLocal() as db:
        db.add(
            PropProjection(
                sport_key=sport_key,
                external_event_id=external_event_id,
                player_name=player_name,
                market_key=market_key,
                projection=projection,
                std_dev=std_dev,
                over_prob=over_prob,
                under_prob=under_prob,
                confidence=confidence,
                model_name=model_name,
                created_at=datetime.now(timezone.utc),
            )
        )
        db.commit()


def get_latest_projections(sport_key: str | list[str]) -> pd.DataFrame:
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        stmt = select(PropProjection).where(PropProjection.sport_key.in_(sport_keys))
        rows = db.execute(stmt).scalars().all()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(
        [
            {
                "event_id": row.external_event_id,
                "sport_key": row.sport_key,
                "player": row.player_name,
                "market": row.market_key,
                "projection": row.projection,
                "std_dev": row.std_dev,
                "over_prob_model": row.over_prob,
                "under_prob_model": row.under_prob,
                "confidence_model": row.confidence,
                "model_name": row.model_name,
                "created_at": row.created_at,
            }
            for row in rows
        ]
    )

    return df.sort_values("created_at").drop_duplicates(
        subset=["event_id", "player", "market"],
        keep="last",
    )
