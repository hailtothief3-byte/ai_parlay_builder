from __future__ import annotations

from typing import Any


SPORT_CONFIG = {
    "NBA": {
        "demo_key": "NBA",
        "provider": "sportsgameodds",
        "sync_enabled": True,
        "live_keys": ["basketball_nba"],
        "lookup_terms": ["nba", "basketball"],
        "prop_markets": [
            "player_points",
            "player_rebounds",
            "player_assists",
            "player_threes",
            "player_points_rebounds_assists",
            "player_first_basket",
        ],
        "dfs_markets": [
            "player_points",
            "player_rebounds",
            "player_assists",
            "player_points_rebounds_assists",
            "player_threes",
            "player_first_basket",
        ],
        "market_coverage": {
            "player_points": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_rebounds": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_assists": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_threes": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_points_rebounds_assists": {
                "status": "demo_only",
                "note": "Shown in demo workflows, but not currently returned by the live SportsGameOdds sync.",
            },
            "player_first_basket": {
                "status": "demo_only",
                "note": "The app supports it, but the current SportsGameOdds NBA feed is not returning first-basket markets.",
            },
        },
    },
    "MLB": {
        "demo_key": "MLB",
        "provider": "sportsgameodds",
        "sync_enabled": True,
        "live_keys": ["baseball_mlb"],
        "lookup_terms": ["mlb", "baseball"],
        "prop_markets": [
            "player_home_runs",
            "player_hits",
            "player_total_bases",
            "player_strikeouts",
        ],
        "dfs_markets": [
            "player_home_runs",
            "player_hits",
            "player_total_bases",
            "player_strikeouts",
        ],
        "market_coverage": {
            "player_home_runs": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_hits": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_total_bases": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
            "player_strikeouts": {
                "status": "live",
                "note": "Available from SportsGameOdds live sync.",
            },
        },
    },
    "NFL": {
        "demo_key": "NFL",
        "provider": "sportsgameodds",
        "sync_enabled": True,
        "live_keys": ["americanfootball_nfl"],
        "lookup_terms": ["nfl", "football"],
        "prop_markets": [
            "player_pass_yds",
            "player_pass_tds",
            "player_rush_yds",
            "player_reception_yds",
            "player_receptions",
        ],
        "dfs_markets": [],
        "market_coverage": {
            "player_pass_yds": {
                "status": "live",
                "note": "Supported by the live provider when NFL events are in season.",
            },
            "player_pass_tds": {
                "status": "provider_unavailable",
                "note": "Configured in the app, but not yet mapped from the current SportsGameOdds sync.",
            },
            "player_rush_yds": {
                "status": "live",
                "note": "Supported by the live provider when NFL events are in season.",
            },
            "player_reception_yds": {
                "status": "live",
                "note": "Supported by the live provider when NFL events are in season.",
            },
            "player_receptions": {
                "status": "live",
                "note": "Supported by the live provider when NFL events are in season.",
            },
        },
    },
    "CS2": {
        "demo_key": "CS2",
        "provider": "esports_placeholder",
        "sync_enabled": False,
        "live_keys": [
            "esports_counterstrike",
            "esports_counter_strike",
            "esports_cs2",
            "counterstrike",
            "counter_strike",
            "cs2",
        ],
        "lookup_terms": ["counter-strike", "counter strike", "cs2"],
        "prop_markets": [
            "player_kills",
            "player_headshots",
            "match_winner",
            "map_winner",
        ],
        "dfs_markets": [],
        "market_coverage": {
            "player_kills": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "player_headshots": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "match_winner": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "map_winner": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
        },
    },
    "LoL": {
        "demo_key": "LOL",
        "provider": "esports_placeholder",
        "sync_enabled": False,
        "live_keys": [
            "esports_leagueoflegends",
            "esports_league_of_legends",
            "esports_lol",
            "leagueoflegends",
            "league_of_legends",
            "lol",
        ],
        "lookup_terms": ["league of legends", "leagueoflegends", "lol"],
        "prop_markets": [
            "player_kills",
            "player_assists",
            "player_kills_assists",
            "team_winner",
        ],
        "dfs_markets": [],
        "market_coverage": {
            "player_kills": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "player_assists": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "player_kills_assists": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "team_winner": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
        },
    },
    "DOTA2": {
        "demo_key": "DOTA2",
        "provider": "esports_placeholder",
        "sync_enabled": False,
        "live_keys": [
            "esports_dota2",
            "esports_dota_2",
            "dota2",
            "dota_2",
        ],
        "lookup_terms": ["dota 2", "dota2"],
        "prop_markets": [
            "player_kills",
            "player_assists",
            "player_fantasy_score",
            "map_winner",
        ],
        "dfs_markets": [],
        "market_coverage": {
            "player_kills": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "player_assists": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "player_fantasy_score": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
            "map_winner": {
                "status": "demo_only",
                "note": "Esports live provider is not connected yet.",
            },
        },
    },
}

STATUS_LABELS = {
    "live": "Live",
    "demo_only": "Demo Only",
    "provider_unavailable": "Provider Unavailable",
}


def get_sport_labels() -> list[str]:
    return list(SPORT_CONFIG)


def get_sport_config(label: str) -> dict[str, Any]:
    return SPORT_CONFIG[label]


def get_market_coverage(label: str) -> list[dict[str, str]]:
    config = get_sport_config(label)
    coverage = config.get("market_coverage", {})
    rows: list[dict[str, str]] = []

    for market in config.get("prop_markets", []):
        info = coverage.get(
            market,
            {
                "status": "provider_unavailable" if config.get("sync_enabled") else "demo_only",
                "note": "No explicit coverage note has been configured yet.",
            },
        )
        rows.append(
            {
                "market": market,
                "status": STATUS_LABELS.get(str(info.get("status")), str(info.get("status"))),
                "note": str(info.get("note", "")),
            }
        )

    return rows


def get_market_coverage_map(label: str) -> dict[str, dict[str, str]]:
    return {
        row["market"]: {
            "status": row["status"],
            "note": row["note"],
        }
        for row in get_market_coverage(label)
    }


def resolve_live_keys_for_label(label: str) -> list[str]:
    return list(SPORT_CONFIG[label]["live_keys"])


def get_sport_provider_name(label: str) -> str:
    return str(SPORT_CONFIG[label]["provider"])


def is_live_sync_enabled(label: str) -> bool:
    return bool(SPORT_CONFIG[label]["sync_enabled"])


def get_syncable_labels() -> list[str]:
    return [label for label in SPORT_CONFIG if SPORT_CONFIG[label].get("sync_enabled")]


def get_provider_labels(provider_name: str) -> list[str]:
    return [
        label
        for label, config in SPORT_CONFIG.items()
        if config.get("provider") == provider_name
    ]


def find_sport_label_for_key(sport_key: str | None) -> str | None:
    if not sport_key:
        return None

    normalized = sport_key.strip().lower()

    for label, config in SPORT_CONFIG.items():
        if normalized in {value.lower() for value in config["live_keys"]}:
            return label

    for label, config in SPORT_CONFIG.items():
        terms = [term.lower() for term in config["lookup_terms"]]
        if any(term in normalized for term in terms):
            return label

    return None


def discover_live_sport_keys(available_sports: list[dict[str, Any]]) -> dict[str, list[str]]:
    discovered: dict[str, list[str]] = {}

    for label, config in SPORT_CONFIG.items():
        discovered[label] = []
        candidate_keys = {value.lower() for value in config["live_keys"]}
        terms = [term.lower() for term in config["lookup_terms"]]

        for sport in available_sports:
            sport_key = str(sport.get("key", "")).strip()
            haystack = " ".join(
                str(sport.get(field, "")).lower()
                for field in ["key", "title", "description", "group"]
            )
            if sport_key.lower() in candidate_keys or any(term in haystack for term in terms):
                discovered[label].append(sport_key)

        discovered[label] = list(dict.fromkeys(discovered[label]))

    return discovered
