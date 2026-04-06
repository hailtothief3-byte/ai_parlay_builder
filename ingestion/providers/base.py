from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SyncResult:
    provider: str
    events_ok: bool = True
    props_ok: bool = True
    dfs_ok: bool = True
    events_count: int = 0
    props_count: int = 0
    dfs_count: int = 0
    messages: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.events_ok and self.props_ok and self.dfs_ok


class BaseProvider:
    name: str = "base"

    def sync_events(self) -> SyncResult:
        raise NotImplementedError

    def sync_props(self) -> SyncResult:
        raise NotImplementedError

    def sync_dfs(self) -> SyncResult:
        raise NotImplementedError
