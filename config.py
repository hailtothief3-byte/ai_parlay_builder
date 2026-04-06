from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    odds_api_key: str = ""
    odds_api_base: str = "https://api.the-odds-api.com/v4"
    sportsgameodds_api_key: str = ""
    sportsgameodds_api_base: str = "https://api.sportsgameodds.com/v2"
    balldontlie_api_key: str = ""
    balldontlie_api_base: str = "https://api.balldontlie.io"
    esports_provider: str = "esports_placeholder"
    pandascore_api_key: str = ""
    abios_api_key: str = ""
    sportsgameodds_min_monthly_entities_remaining: int = 25000
    sportsgameodds_min_daily_entities_remaining: int = 2500
    sportsgameodds_minute_request_buffer: int = 3
    sportsgameodds_max_events_per_league_sync: int = 8
    sportsgameodds_sync_cooldown_minutes: int = 30
    sportsgameodds_only_future_events: bool = True
    sportsgameodds_future_window_hours: int = 72
    sportsgameodds_include_nba_exotics: bool = False
    default_weights: dict[str, float] = field(
        default_factory=lambda: {
            "recent_form": 0.35,
            "matchup": 0.20,
            "role_stability": 0.15,
            "market_signal": 0.10,
            "historical_hit_rate": 0.12,
            "data_quality": 0.08,
        }
    )

    @property
    def has_odds_api_key(self) -> bool:
        return bool(self.odds_api_key.strip())

CONFIG = AppConfig(
    odds_api_key=os.getenv("ODDS_API_KEY", ""),
    sportsgameodds_api_key=os.getenv("SPORTSGAMEODDS_API_KEY", ""),
    sportsgameodds_api_base=os.getenv("SPORTSGAMEODDS_API_BASE", "https://api.sportsgameodds.com/v2"),
    balldontlie_api_key=os.getenv("BALLDONTLIE_API_KEY", ""),
    balldontlie_api_base=os.getenv("BALLDONTLIE_API_BASE", "https://api.balldontlie.io"),
    esports_provider=os.getenv("ESPORTS_PROVIDER", "esports_placeholder"),
    pandascore_api_key=os.getenv("PANDASCORE_API_KEY", ""),
    abios_api_key=os.getenv("ABIOS_API_KEY", ""),
    sportsgameodds_min_monthly_entities_remaining=int(os.getenv("SPORTSGAMEODDS_MIN_MONTHLY_ENTITIES_REMAINING", "25000")),
    sportsgameodds_min_daily_entities_remaining=int(os.getenv("SPORTSGAMEODDS_MIN_DAILY_ENTITIES_REMAINING", "2500")),
    sportsgameodds_minute_request_buffer=int(os.getenv("SPORTSGAMEODDS_MINUTE_REQUEST_BUFFER", "3")),
    sportsgameodds_max_events_per_league_sync=int(os.getenv("SPORTSGAMEODDS_MAX_EVENTS_PER_LEAGUE_SYNC", "8")),
    sportsgameodds_sync_cooldown_minutes=int(os.getenv("SPORTSGAMEODDS_SYNC_COOLDOWN_MINUTES", "30")),
    sportsgameodds_only_future_events=os.getenv("SPORTSGAMEODDS_ONLY_FUTURE_EVENTS", "true").strip().lower() == "true",
    sportsgameodds_future_window_hours=int(os.getenv("SPORTSGAMEODDS_FUTURE_WINDOW_HOURS", "72")),
    sportsgameodds_include_nba_exotics=os.getenv("SPORTSGAMEODDS_INCLUDE_NBA_EXOTICS", "false").strip().lower() == "true",
)

ODDS_API_KEY = CONFIG.odds_api_key
ODDS_API_BASE = CONFIG.odds_api_base
SPORTSGAMEODDS_API_KEY = CONFIG.sportsgameodds_api_key
SPORTSGAMEODDS_API_BASE = CONFIG.sportsgameodds_api_base
BALLDONTLIE_API_KEY = CONFIG.balldontlie_api_key
BALLDONTLIE_API_BASE = CONFIG.balldontlie_api_base
