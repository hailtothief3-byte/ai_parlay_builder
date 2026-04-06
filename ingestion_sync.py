from db.session import SessionLocal
from db.models import Event
from ingestion.odds_api import OddsApiClient
from ingestion.normalize import normalize_event

SUPPORTED_SPORTS = [
    "basketball_nba",
    "baseball_mlb",
    "icehockey_nhl",
    "americanfootball_nfl",
    "basketball_ncaab",
    "baseball_mlb_preseason",
]

def upsert_event(db, payload: dict):
    existing = db.query(Event).filter(
        Event.external_event_id == payload["external_event_id"]
    ).one_or_none()

    if existing:
        existing.sport_key = payload["sport_key"]
        existing.commence_time = payload["commence_time"]
        existing.home_team = payload["home_team"]
        existing.away_team = payload["away_team"]
    else:
        db.add(Event(**payload))

def sync_events():
    client = OddsApiClient()

    with SessionLocal() as db:
        for sport_key in SUPPORTED_SPORTS:
            try:
                events, headers = client.get_events(sport_key=sport_key, markets="h2h")
                print(f"{sport_key}: {len(events)} events")
                for event in events:
                    upsert_event(db, normalize_event(event))
                db.commit()
            except Exception as exc:
                print(f"Failed {sport_key}: {exc}")

if __name__ == "__main__":
    sync_events()
