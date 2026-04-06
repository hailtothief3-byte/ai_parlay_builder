from __future__ import annotations

from ingestion.providers.base import SyncResult
from ingestion.providers.esports_placeholder_provider import EsportsPlaceholderProvider
from ingestion.providers.odds_api_provider import OddsApiProvider
from ingestion.providers.sportsgameodds_provider import SportsGameOddsProvider


_PROVIDERS = {
    "the_odds_api": OddsApiProvider,
    "sportsgameodds": SportsGameOddsProvider,
    "esports_placeholder": EsportsPlaceholderProvider,
}


def get_provider(name: str):
    provider_cls = _PROVIDERS[name]
    return provider_cls()


def get_provider_names() -> list[str]:
    return list(_PROVIDERS)


def sync_all_providers() -> dict[str, dict[str, SyncResult]]:
    results: dict[str, dict[str, SyncResult]] = {}

    for provider_name in get_provider_names():
        provider = get_provider(provider_name)
        results[provider_name] = {
            "events": provider.sync_events(),
            "props": provider.sync_props(),
            "dfs": provider.sync_dfs(),
        }

    return results
