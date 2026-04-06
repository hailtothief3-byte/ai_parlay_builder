from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from sqlalchemy import select

from ingestion.sportsgameodds_api import SportsGameOddsClient, format_sgo_error
from db.models import PropResult, TrackedPick
from db.session import SessionLocal
from services.sync_policy import record_sync_payload


def _coerce_sport_keys(sport_key: str | list[str]) -> list[str]:
    if isinstance(sport_key, str):
        return [sport_key]
    return [key for key in sport_key if key]


SPORTSGAMEODDS_LEAGUE_IDS = {
    "basketball_nba": "NBA",
    "baseball_mlb": "MLB",
    "americanfootball_nfl": "NFL",
}

SPORTSGAMEODDS_RESULT_ODD_IDS = {
    "basketball_nba": [
        "points-PLAYER_ID-game-ou-over",
        "points-PLAYER_ID-game-ou-under",
        "rebounds-PLAYER_ID-game-ou-over",
        "rebounds-PLAYER_ID-game-ou-under",
        "assists-PLAYER_ID-game-ou-over",
        "assists-PLAYER_ID-game-ou-under",
        "threes_made-PLAYER_ID-game-ou-over",
        "threes_made-PLAYER_ID-game-ou-under",
    ],
    "baseball_mlb": [
        "batting_hits-PLAYER_ID-game-ou-over",
        "batting_hits-PLAYER_ID-game-ou-under",
        "total_bases-PLAYER_ID-game-ou-over",
        "total_bases-PLAYER_ID-game-ou-under",
        "home_runs-PLAYER_ID-game-ou-over",
        "home_runs-PLAYER_ID-game-ou-under",
        "pitcher_strikeouts-PLAYER_ID-game-ou-over",
        "pitcher_strikeouts-PLAYER_ID-game-ou-under",
    ],
    "americanfootball_nfl": [
        "passing_yards-PLAYER_ID-game-ou-over",
        "passing_yards-PLAYER_ID-game-ou-under",
        "rushing_yards-PLAYER_ID-game-ou-over",
        "rushing_yards-PLAYER_ID-game-ou-under",
        "receiving_yards-PLAYER_ID-game-ou-over",
        "receiving_yards-PLAYER_ID-game-ou-under",
        "receptions-PLAYER_ID-game-ou-over",
        "receptions-PLAYER_ID-game-ou-under",
    ],
}

SPORTSGAMEODDS_MARKET_KEY_MAP = {
    "points": "player_points",
    "rebounds": "player_rebounds",
    "assists": "player_assists",
    "threes_made": "player_threes",
    "batting_hits": "player_hits",
    "total_bases": "player_total_bases",
    "home_runs": "player_home_runs",
    "pitcher_strikeouts": "player_strikeouts",
    "passing_yards": "player_pass_yds",
    "rushing_yards": "player_rush_yds",
    "receiving_yards": "player_reception_yds",
    "receptions": "player_receptions",
}


def track_edge_rows(edge_df: pd.DataFrame, sport_key: str, source: str = "manual_track") -> int:
    if edge_df.empty:
        return 0

    tracked_at = datetime.now(timezone.utc)
    rows_added = 0

    with SessionLocal() as db:
        for _, row in edge_df.iterrows():
            db.add(
                TrackedPick(
                    sport_key=sport_key,
                    external_event_id=str(row["event_id"]),
                    bookmaker_key=str(row["book_key"]),
                    bookmaker_title=str(row["sportsbook"]),
                    market_key=str(row["market"]),
                    player_name=str(row["player"]),
                    pick=str(row["pick"]),
                    side=str(row.get("best_for") or row.get("side") or "").lower() or None,
                    line=float(row["line"]) if pd.notnull(row["line"]) else None,
                    price=float(row["price"]) if pd.notnull(row["price"]) else None,
                    projection=float(row["projection"]) if pd.notnull(row["projection"]) else None,
                    implied_prob=float(row["implied_prob"]) if pd.notnull(row["implied_prob"]) else None,
                    model_prob=float(row["model_prob"]) if pd.notnull(row["model_prob"]) else None,
                    edge=float(row["edge"]) if pd.notnull(row["edge"]) else None,
                    confidence=float(row["confidence"]) if pd.notnull(row["confidence"]) else None,
                    is_dfs=bool(row.get("is_dfs", False)),
                    source=source,
                    tracked_at=tracked_at,
                )
            )
            rows_added += 1
        db.commit()

    return rows_added


def get_tracked_picks(sport_key: str | list[str]) -> pd.DataFrame:
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        stmt = select(TrackedPick).where(TrackedPick.sport_key.in_(sport_keys))
        rows = db.execute(stmt).scalars().all()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "tracked_pick_id": row.id,
                "sport_key": row.sport_key,
                "event_id": row.external_event_id,
                "book_key": row.bookmaker_key,
                "sportsbook": row.bookmaker_title,
                "market": row.market_key,
                "player": row.player_name,
                "pick": row.pick,
                "side": row.side,
                "line": row.line,
                "price": row.price,
                "projection": row.projection,
                "implied_prob": row.implied_prob,
                "model_prob": row.model_prob,
                "edge": row.edge,
                "confidence": row.confidence,
                "is_dfs": row.is_dfs,
                "source": row.source,
                "tracked_at": row.tracked_at,
            }
            for row in rows
        ]
    ).sort_values("tracked_at", ascending=False)


def get_unresolved_tracked_picks(sport_key: str | list[str]) -> pd.DataFrame:
    tracked = get_tracked_picks(sport_key)
    if tracked.empty:
        return tracked

    results = get_prop_results(sport_key)
    if results.empty:
        return tracked

    unresolved = tracked.merge(
        results[["sport_key", "event_id", "market", "player"]],
        on=["sport_key", "event_id", "market", "player"],
        how="left",
        indicator=True,
    )
    unresolved = unresolved[unresolved["_merge"] == "left_only"].drop(columns=["_merge"])
    return unresolved


def upsert_prop_result(
    sport_key: str,
    event_id: str,
    market_key: str,
    player_name: str,
    actual_value: float | None = None,
    winning_side: str | None = None,
    source: str = "manual_result",
    notes: str | None = None,
) -> None:
    normalized_winning_side = winning_side.strip().lower() if winning_side else None
    settled_at = datetime.now(timezone.utc)

    with SessionLocal() as db:
        existing = db.execute(
            select(PropResult).where(
                PropResult.sport_key == sport_key,
                PropResult.external_event_id == event_id,
                PropResult.market_key == market_key,
                PropResult.player_name == player_name,
            )
        ).scalar_one_or_none()

        if existing:
            existing.actual_value = actual_value
            existing.winning_side = normalized_winning_side
            existing.status = "settled"
            existing.source = source
            existing.notes = notes
            existing.settled_at = settled_at
        else:
            db.add(
                PropResult(
                    sport_key=sport_key,
                    external_event_id=event_id,
                    market_key=market_key,
                    player_name=player_name,
                    actual_value=actual_value,
                    winning_side=normalized_winning_side,
                    status="settled",
                    source=source,
                    notes=notes,
                    settled_at=settled_at,
                )
            )
        db.commit()


def get_prop_results(sport_key: str | list[str]) -> pd.DataFrame:
    sport_keys = _coerce_sport_keys(sport_key)
    if not sport_keys:
        return pd.DataFrame()

    with SessionLocal() as db:
        stmt = select(PropResult).where(PropResult.sport_key.in_(sport_keys))
        rows = db.execute(stmt).scalars().all()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "result_id": row.id,
                "sport_key": row.sport_key,
                "event_id": row.external_event_id,
                "market": row.market_key,
                "player": row.player_name,
                "actual_value": row.actual_value,
                "winning_side": row.winning_side,
                "status": row.status,
                "source": row.source,
                "notes": row.notes,
                "settled_at": row.settled_at,
            }
            for row in rows
        ]
    ).sort_values("settled_at", ascending=False)


def _american_profit_units(price: float | None) -> float:
    if price is None or pd.isna(price):
        return 0.0
    if price > 0:
        return price / 100.0
    if price < 0:
        return 100.0 / abs(price)
    return 0.0


def _grade_pick(row: pd.Series) -> str | None:
    side = str(row.get("side") or "").lower()
    line = row.get("line")
    actual_value = row.get("actual_value")
    winning_side = str(row.get("winning_side") or "").lower()

    if side in {"over", "under"} and actual_value is not None and pd.notnull(actual_value) and line is not None and pd.notnull(line):
        if side == "over":
            if actual_value > line:
                return "win"
            if actual_value < line:
                return "loss"
            return "push"
        if side == "under":
            if actual_value < line:
                return "win"
            if actual_value > line:
                return "loss"
            return "push"

    if side in {"yes", "no"} and winning_side in {"yes", "no"}:
        return "win" if side == winning_side else "loss"

    return None


def get_graded_picks(sport_key: str | list[str]) -> pd.DataFrame:
    tracked = get_tracked_picks(sport_key)
    results = get_prop_results(sport_key)

    if tracked.empty or results.empty:
        return pd.DataFrame()

    merged = tracked.merge(
        results,
        on=["sport_key", "event_id", "market", "player"],
        how="inner",
    )

    if merged.empty:
        return pd.DataFrame()

    merged["grade"] = merged.apply(_grade_pick, axis=1)
    merged = merged[merged["grade"].notna()].copy()
    if merged.empty:
        return pd.DataFrame()

    merged["profit_units"] = merged.apply(
        lambda row: _american_profit_units(row["price"]) if row["grade"] == "win"
        else (0.0 if row["grade"] == "push" else -1.0),
        axis=1,
    )
    merged["won"] = (merged["grade"] == "win").astype(int)
    merged["resolved_at"] = merged["settled_at"]
    return merged.sort_values("resolved_at", ascending=False)


def _safe_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _sportsgameodds_player_name(event_payload: dict, odd: dict) -> str:
    player_id = odd.get("playerID") or odd.get("statEntityID")
    players = event_payload.get("players", {}) if isinstance(event_payload.get("players"), dict) else {}
    player = players.get(player_id, {}) if isinstance(players.get(player_id), dict) else {}
    if player.get("name"):
        return str(player["name"]).strip()
    return str(
        odd.get("playerName")
        or odd.get("name")
        or odd.get("statEntityName")
        or odd.get("participantName")
        or ""
    ).strip()


def _sportsgameodds_market_key(odd: dict) -> str | None:
    odd_id = str(odd.get("oddID") or "").strip()
    stat_id = odd_id.split("-")[0] if odd_id else str(odd.get("statID") or "").strip()
    return SPORTSGAMEODDS_MARKET_KEY_MAP.get(stat_id.lower())


def sync_prop_results_from_sportsgameodds(
    sport_key: str | list[str],
    days: int = 7,
) -> dict[str, int]:
    sport_keys = _coerce_sport_keys(sport_key)
    unresolved = get_unresolved_tracked_picks(sport_keys)
    if unresolved.empty:
        return {"rows_imported": 0, "events_fetched": 0, "matched_results": 0}

    grouped_targets = {}
    for current_sport_key, group in unresolved.groupby("sport_key", dropna=False):
        current_sport_key = str(current_sport_key)
        league_id = SPORTSGAMEODDS_LEAGUE_IDS.get(current_sport_key)
        odd_ids = SPORTSGAMEODDS_RESULT_ODD_IDS.get(current_sport_key, [])
        if not league_id or not odd_ids:
            continue
        grouped_targets[current_sport_key] = {
            "league_id": league_id,
            "odd_ids": odd_ids,
            "event_ids": {str(value) for value in group["event_id"].dropna().tolist()},
            "match_keys": {
                (str(row["event_id"]), str(row["player"]).strip().lower(), str(row["market"]))
                for _, row in group.iterrows()
            },
        }

    if not grouped_targets:
        return {"rows_imported": 0, "events_fetched": 0, "matched_results": 0}

    client = SportsGameOddsClient()
    imported = 0
    matched_results = 0
    events_fetched = 0
    today = datetime.now(timezone.utc).date()

    try:
        for current_sport_key, target in grouped_targets.items():
            seen_event_ids: set[str] = set()
            matched_rows: list[dict] = []

            for offset in range(days):
                day = today - pd.Timedelta(days=offset)
                day_start = day.strftime("%Y-%m-%d")
                day_end = (day + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                cursor: str | None = None
                pages_fetched = 0

                while True:
                    params = {
                        "leagueID": target["league_id"],
                        "finalized": "true",
                        "startsAfter": day_start,
                        "startsBefore": day_end,
                        "limit": 25,
                        "oddIDs": ",".join(target["odd_ids"]),
                    }
                    if cursor:
                        params["cursor"] = cursor

                    payload = client._get("/events", params=params)
                    batch = payload.get("data", []) if isinstance(payload, dict) else []
                    if not isinstance(batch, list) or not batch:
                        break

                    pages_fetched += 1
                    for event in batch:
                        if not isinstance(event, dict):
                            continue
                        event_id = str(event.get("eventID") or event.get("id") or "").strip()
                        if not event_id or event_id in seen_event_ids:
                            continue
                        seen_event_ids.add(event_id)
                        if event_id not in target["event_ids"]:
                            continue

                        events_fetched += 1
                        odds = event.get("odds", {}) if isinstance(event.get("odds"), dict) else {}
                        seen_market_results: set[tuple[str, str, str]] = set()
                        for odd in odds.values():
                            if not isinstance(odd, dict):
                                continue

                            player_name = _sportsgameodds_player_name(event, odd)
                            market_key = _sportsgameodds_market_key(odd)
                            score = _safe_float(odd.get("score"))
                            if not player_name or market_key is None or score is None:
                                continue

                            result_key = (event_id, player_name.strip().lower(), market_key)
                            if result_key not in target["match_keys"] or result_key in seen_market_results:
                                continue

                            seen_market_results.add(result_key)
                            matched_rows.append(
                                {
                                    "sport_key": current_sport_key,
                                    "event_id": event_id,
                                    "market_key": market_key,
                                    "player_name": player_name,
                                    "actual_value": score,
                                    "winning_side": None,
                                    "source": "sportsgameodds_finalized",
                                    "notes": "Auto-settled from finalized SportsGameOdds event data.",
                                }
                            )

                    cursor = payload.get("nextCursor") if isinstance(payload, dict) else None
                    if not cursor or pages_fetched >= 3:
                        break

            for row in matched_rows:
                upsert_prop_result(
                    sport_key=row["sport_key"],
                    event_id=row["event_id"],
                    market_key=row["market_key"],
                    player_name=row["player_name"],
                    actual_value=row["actual_value"],
                    winning_side=row["winning_side"],
                    source=row["source"],
                    notes=row["notes"],
                )
                imported += 1
            matched_results += len(matched_rows)
    except Exception as exc:
        raise ValueError(f"SportsGameOdds auto-settle failed: {format_sgo_error(exc)}") from exc

    result = {
        "rows_imported": imported,
        "events_fetched": events_fetched,
        "matched_results": matched_results,
    }
    record_sync_payload(
        "sportsgameodds_auto_settle",
        ",".join(sport_keys),
        result,
    )
    return result


def import_prop_results_csv(file_bytes: bytes, default_source: str = "csv_import") -> dict[str, int]:
    df = pd.read_csv(BytesIO(file_bytes))
    required = ["sport_key", "event_id", "market", "player"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError("Prop results CSV is missing required columns: " + ", ".join(missing))

    imported = 0
    for _, row in df.iterrows():
        sport_key = str(row.get("sport_key") or "").strip()
        event_id = str(row.get("event_id") or "").strip()
        market_key = str(row.get("market") or "").strip()
        player_name = str(row.get("player") or "").strip()
        if not sport_key or not event_id or not market_key or not player_name:
            continue

        upsert_prop_result(
            sport_key=sport_key,
            event_id=event_id,
            market_key=market_key,
            player_name=player_name,
            actual_value=float(row["actual_value"]) if pd.notnull(row.get("actual_value")) else None,
            winning_side=str(row.get("winning_side")).strip().lower() if pd.notnull(row.get("winning_side")) and str(row.get("winning_side")).strip() else None,
            source=str(row.get("source") or default_source),
            notes=str(row.get("notes")) if pd.notnull(row.get("notes")) else None,
        )
        imported += 1

    return {"rows_imported": imported}


def import_tracked_picks_csv(file_bytes: bytes, default_source: str = "csv_import") -> dict[str, int]:
    df = pd.read_csv(BytesIO(file_bytes))
    required = ["sport_key", "event_id", "book_key", "sportsbook", "market", "player", "pick"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError("Tracked picks CSV is missing required columns: " + ", ".join(missing))

    tracked_at = datetime.now(timezone.utc)
    imported = 0

    with SessionLocal() as db:
        for _, row in df.iterrows():
            sport_key = str(row.get("sport_key") or "").strip()
            event_id = str(row.get("event_id") or "").strip()
            book_key = str(row.get("book_key") or "").strip()
            sportsbook = str(row.get("sportsbook") or "").strip()
            market = str(row.get("market") or "").strip()
            player = str(row.get("player") or "").strip()
            pick = str(row.get("pick") or "").strip()
            if not all([sport_key, event_id, book_key, sportsbook, market, player, pick]):
                continue

            db.add(
                TrackedPick(
                    sport_key=sport_key,
                    external_event_id=event_id,
                    bookmaker_key=book_key,
                    bookmaker_title=sportsbook,
                    market_key=market,
                    player_name=player,
                    pick=pick,
                    side=str(row.get("side")).strip().lower() if pd.notnull(row.get("side")) and str(row.get("side")).strip() else None,
                    line=float(row["line"]) if pd.notnull(row.get("line")) else None,
                    price=float(row["price"]) if pd.notnull(row.get("price")) else None,
                    projection=float(row["projection"]) if pd.notnull(row.get("projection")) else None,
                    implied_prob=float(row["implied_prob"]) if pd.notnull(row.get("implied_prob")) else None,
                    model_prob=float(row["model_prob"]) if pd.notnull(row.get("model_prob")) else None,
                    edge=float(row["edge"]) if pd.notnull(row.get("edge")) else None,
                    confidence=float(row["confidence"]) if pd.notnull(row.get("confidence")) else None,
                    is_dfs=bool(row.get("is_dfs", False)),
                    source=str(row.get("source") or default_source),
                    tracked_at=pd.to_datetime(row.get("tracked_at"), utc=True).to_pydatetime() if pd.notnull(row.get("tracked_at")) else tracked_at,
                )
            )
            imported += 1
        db.commit()

    return {"rows_imported": imported}
