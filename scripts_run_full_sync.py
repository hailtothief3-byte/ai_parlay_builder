from db import init_db
from ingestion.providers import sync_all_providers
from scripts_generate_demo_projections import create_projections
from services.results_service import sync_prop_results_from_sportsgameodds
from services.usage_guard import safe_get_sportsgameodds_usage_summary

def main():
    init_db()
    sgo_usage = safe_get_sportsgameodds_usage_summary()
    if sgo_usage.get("enabled") and not sgo_usage.get("ok_to_sync"):
        print(sgo_usage.get("message"))
        return

    provider_results = sync_all_providers()

    live_rows_synced = sum(
        result.props_count + result.dfs_count
        for provider_runs in provider_results.values()
        for result in provider_runs.values()
    )

    if live_rows_synced == 0:
        print("No live rows were synced from configured providers.")

    auto_settle = sync_prop_results_from_sportsgameodds(
        ["basketball_nba", "baseball_mlb", "americanfootball_nfl"],
        days=7,
    )
    if auto_settle["rows_imported"] > 0:
        print(
            f"Auto-settled {auto_settle['rows_imported']} tracked props from "
            f"{auto_settle['events_fetched']} finalized SportsGameOdds events."
        )

    create_projections()
    print("Full sync complete.")

if __name__ == "__main__":
    main()
