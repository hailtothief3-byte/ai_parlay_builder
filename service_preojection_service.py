from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import select

from db.session import SessionLocal
from db.models import PropProjection


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


def get_latest_projections(sport_key: str) -> pd.DataFrame:
    with SessionLocal() as db:
        stmt = select(PropProjection).where(PropProjection.sport_key == sport_key)
        rows = db.execute(stmt).scalars().all()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            [
                {
                    "event_id": r.external_event_id,
                    "sport_key": r.sport_key,
                    "player": r.player_name,
                    "market": r.market_key,
                    "projection": r.projection,
                    "std_dev": r.std_dev,
                    "over_prob_model": r.over_prob,
                    "under_prob_model": r.under_prob,
                    "confidence_model": r.confidence,
                    "model_name": r.model_name,
                    "created_at": r.created_at,
                }
                for r in rows
            ]
        )

        df = df.sort_values("created_at").drop_duplicates(
            subset=["event_id", "player", "market"],
            keep="last",
        )
        return df
