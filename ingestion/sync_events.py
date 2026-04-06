from ingestion.providers import get_provider


def sync_events() -> bool:
    result = get_provider("the_odds_api").sync_events()
    return result.events_ok


if __name__ == "__main__":
    sync_events()
