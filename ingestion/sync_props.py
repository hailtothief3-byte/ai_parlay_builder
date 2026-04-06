from ingestion.providers import get_provider


def sync_props() -> bool:
    result = get_provider("the_odds_api").sync_props()
    return result.props_ok


if __name__ == "__main__":
    sync_props()
