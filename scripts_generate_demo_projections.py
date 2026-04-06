from datetime import datetime, timezone
import random
from sqlalchemy import delete

from db.session import SessionLocal
from db.models import MarketLine, PropProjection
from services.projection_builder import build_live_projections_for_sports


MARKET_RULES = {
    "player_points": (18, 7),
    "player_rebounds": (6, 3),
    "player_assists": (5, 2.5),
    "player_points_rebounds_assists": (28, 8),
    "player_threes": (2.2, 1.4),
    "player_home_runs": (0.22, 0.32),
    "player_hits": (1.1, 1.0),
    "player_total_bases": (1.8, 1.7),
    "player_strikeouts": (5.8, 1.8),
    "player_first_basket": (0.08, 0.18),
    "player_kills": (18, 5),
    "player_headshots": (9, 3.5),
    "player_kills_assists": (24, 6),
    "player_fantasy_score": (42, 10),
    "match_winner": (0.52, 0.45),
    "map_winner": (0.52, 0.45),
    "team_winner": (0.52, 0.45),
}


def create_projections():
    live_counts = build_live_projections_for_sports(["NBA", "MLB"])

    with SessionLocal() as db:
        lines = db.query(MarketLine).all()

        seen = set()

        for line in lines:
            key = (line.external_event_id, line.player_name, line.market_key)
            if not line.player_name or key in seen:
                continue
            seen.add(key)
            if line.sport_key in {"basketball_nba", "baseball_mlb"}:
                continue

            base_mean, base_std = MARKET_RULES.get(line.market_key, (10, 5))

            projection = base_mean
            if line.line is not None:
                projection = float(line.line) + random.uniform(-1.5, 1.5)

            over_prob = 0.50 + random.uniform(-0.08, 0.08)
            under_prob = 1.0 - over_prob
            confidence = 55 + random.uniform(0, 25)

            db.execute(
                delete(PropProjection).where(
                    PropProjection.sport_key == line.sport_key,
                    PropProjection.external_event_id == line.external_event_id,
                    PropProjection.player_name == line.player_name,
                    PropProjection.market_key == line.market_key,
                )
            )

            db.add(
                PropProjection(
                    sport_key=line.sport_key,
                    external_event_id=line.external_event_id,
                    player_name=line.player_name,
                    market_key=line.market_key,
                    projection=projection,
                    std_dev=base_std,
                    over_prob=over_prob,
                    under_prob=under_prob,
                    confidence=confidence,
                    model_name="baseline_seed_v1",
                    created_at=datetime.now(timezone.utc),
                )
            )

        db.commit()
        print(f"Live projections generated: {live_counts}")
        print("Fallback/demo projections generated.")


if __name__ == "__main__":
    create_projections()
