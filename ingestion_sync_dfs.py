from sqlalchemy.exc import IntegrityError

from db.session import SessionLocal
from db.models import Event, MarketLine
from ingestion.odds_api import OddsApiClient
from ingestion.normalize import normalize_market_lines

DFS_BOOKMAKERS = "prizepicks,underdog,pick6,betr_us_dfs"

DFS_MARKETS = {
    "basketball_nba": [
        "player_points",
        "player_rebounds",
        "player_assists",
        "player_points_rebounds_assists",
        "player_threes",
        "player_first_basket",
    ],
    "baseball_mlb": [
        "player_home_runs",
        "player_hits",
        "player_total_bases",
        "player_strikeouts",
    ],
}

def save_rows(db, rows: list[dict]):
    count = 0
    for row in rows:
        try:
            db.add(MarketLine(**row))
            db.flush()
            count += 1
        except IntegrityError:
            db.rollback()
    db.commit()
    return count

def sync_dfs():
    client = OddsApiClient()

    with SessionLocal() as db:
        events = db.query(Event).all()

        for event in events:
            markets = DFS_MARKETS.get(event.sport_key)
            if not markets:
                continue

            try:
                payload, _ = client.get_event_props(
                    sport_key=event.sport_key,
                    event_id=event.external_event_id,
                    markets=markets,
                    regions="us_dfs",
                    bookmakers=DFS_BOOKMAKERS,
                )
                rows = normalize_market_lines(payload, client.utcnow())
                rows = [r for r in rows if r["is_dfs"]]

                inserted = save_rows(db, rows)
                print(f"DFS synced {inserted} rows for {event.external_event_id}")
            except Exception as exc:
                print(f"DFS sync failed for {event.external_event_id}: {exc}")

if __name__ == "__main__":
    sync_dfs()
