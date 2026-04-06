from __future__ import annotations

from ingestion.providers.base import BaseProvider, SyncResult
from sports_config import get_provider_labels


class EsportsPlaceholderProvider(BaseProvider):
    name = "esports_placeholder"

    def _message(self) -> str:
        esports_labels = ", ".join(get_provider_labels(self.name))
        return (
            f"Esports live sync is not configured yet for {esports_labels}. "
            "Plug in Abios or PandaScore credentials and implement this provider next."
        )

    def sync_events(self) -> SyncResult:
        result = SyncResult(provider=self.name)
        result.events_ok = False
        message = self._message()
        result.messages.append(message)
        print(message)
        return result

    def sync_props(self) -> SyncResult:
        result = SyncResult(provider=self.name)
        result.props_ok = False
        message = self._message()
        result.messages.append(message)
        print(message)
        return result

    def sync_dfs(self) -> SyncResult:
        result = SyncResult(provider=self.name)
        result.dfs_ok = False
        return result
