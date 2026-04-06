from sqlalchemy.exc import IntegrityError

from db.session import SessionLocal
from db.models import Event, MarketLine
from ingestion.odds_api import OddsApiClient
from ingestion.normalize import normalize_market_lines

NBA_PROP_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_points_rebounds_assists",
    "player_first_basket",
]

MLB_PROP_MARKETS = [
    "player_home_runs",
    "player_hits",
    "player_total_bases",
    "player_strikeouts",
]

NFL_PROP_MARKETS = [
    "player_pass_yds",
    "player_pass_tds",
    "player_rush_yds",
    "player_reception_yds",
    "player_receptions",
]

SPORT_MARKETS = {
    "basketball_nba": NBA_PROP_MARKETS,
    "baseball_mlb": MLB_PROP_MARKETS,
    "americanfootball_nfl": NFL_PROP_MARKETS,
}

def save_rows(db, rows: list[dict]):
    inserted = 0
    for row in rows:
        try:
            db.add(MarketLine(**row))
            db.flush()
            inserted += 1
        except IntegrityError:
            db.rollback()
    db.commit()
    return inserted

def sync_props():
    client = OddsApiClient()

    with SessionLocal() as db:
        events = db.query(Event).all()

        for event in events:
            markets = SPORT_MARKETS.get(event.sport_key)
            if not markets:
                continue

            try:
                payload, headers = client.get_event_props(
                    sport_key=event.sport_key,
                    event_id=event.external_event_id,
                    markets=markets,
                    regions="us,us_dfs",
                )
                rows = normalize_market_lines(payload, client.utcnow())
                inserted = save_rows(db, rows)
                print(
                    f"{event.sport_key} {event.away_team} @ {event.home_team}: "
                    f"{inserted} rows"
                )
            except Exception as exc:
                print(
                    f"Prop sync failed for {event.external_event_id} "
                    f"({event.away_team} @ {event.home_team}): {exc}"
                )

if __name__ == "__main__":
    sync_props()
