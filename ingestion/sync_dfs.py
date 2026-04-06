from ingestion.providers import get_provider


def sync_dfs() -> bool:
    result = get_provider("the_odds_api").sync_dfs()
    return result.dfs_ok


if __name__ == "__main__":
    sync_dfs()
