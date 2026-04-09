from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
import json

import pandas as pd
from sqlalchemy import select

from db.models import SavedTicket, SavedTicketLeg
from db.session import SessionLocal
from services.results_service import get_prop_results
from sports_config import get_sport_config

TICKET_META_PREFIX = "[APB_META]"


def _pack_ticket_notes(notes: str | None = None, metadata: dict | None = None) -> str | None:
    clean_notes = str(notes or "").strip()
    if not metadata:
        return clean_notes or None
    payload = {
        "notes": clean_notes,
        "metadata": metadata,
    }
    return TICKET_META_PREFIX + json.dumps(payload)


def unpack_ticket_notes(raw_notes: str | None) -> tuple[str | None, dict]:
    if not raw_notes:
        return None, {}
    raw_text = str(raw_notes)
    if not raw_text.startswith(TICKET_META_PREFIX):
        return raw_text, {}
    try:
        payload = json.loads(raw_text[len(TICKET_META_PREFIX):])
        return str(payload.get("notes") or "").strip() or None, dict(payload.get("metadata") or {})
    except Exception:
        return raw_text, {}


def save_ticket(
    ticket_name: str,
    sport_label: str,
    source: str,
    legs_df: pd.DataFrame,
    notes: str | None = None,
    metadata: dict | None = None,
) -> int | None:
    if legs_df.empty:
        return None

    created_at = datetime.now(timezone.utc)
    leg_count = len(legs_df)
    avg_confidence = float(legs_df["confidence"].mean()) if "confidence" in legs_df.columns and not legs_df["confidence"].isna().all() else None
    model_prob_col = "model_prob" if "model_prob" in legs_df.columns else "win_probability"
    avg_model_prob = float(legs_df[model_prob_col].mean()) if model_prob_col in legs_df.columns and not legs_df[model_prob_col].isna().all() else None

    with SessionLocal() as db:
        ticket = SavedTicket(
            name=ticket_name.strip() or f"{sport_label} Ticket",
            sport_label=sport_label,
            source=source,
            ticket_type="parlay",
            leg_count=leg_count,
            avg_confidence=avg_confidence,
            avg_model_prob=avg_model_prob,
            status="open",
            notes=_pack_ticket_notes(notes, metadata),
            created_at=created_at,
        )
        db.add(ticket)
        db.flush()

        sport_key = get_sport_config(sport_label)["live_keys"][0] if source == "live_edges" else None

        for _, row in legs_df.iterrows():
            db.add(
                SavedTicketLeg(
                    ticket_id=ticket.id,
                    leg_rank=int(row.get("leg_rank", 0) or 0),
                    sport_key=sport_key if source == "live_edges" else row.get("sport_key"),
                    external_event_id=str(row.get("event_id")) if pd.notnull(row.get("event_id")) else None,
                    bookmaker_key=str(row.get("book_key")) if pd.notnull(row.get("book_key")) else None,
                    bookmaker_title=str(row.get("sportsbook")) if pd.notnull(row.get("sportsbook")) else None,
                    market_key=str(row.get("market")),
                    player_name=str(row.get("player")),
                    pick=str(row.get("pick")),
                    side=str(row.get("best_for") or row.get("side") or "").lower() or None,
                    line=float(row["line"]) if pd.notnull(row.get("line")) else None,
                    price=float(row["price"]) if pd.notnull(row.get("price")) else None,
                    projection=float(row["projection"]) if pd.notnull(row.get("projection")) else float(row["predicted_value"]) if pd.notnull(row.get("predicted_value")) else None,
                    model_prob=float(row["model_prob"]) if pd.notnull(row.get("model_prob")) else float(row["win_probability"]) if pd.notnull(row.get("win_probability")) else None,
                    confidence=float(row["confidence"]) if pd.notnull(row.get("confidence")) else None,
                    status="open",
                )
            )

        db.commit()
        return ticket.id


def get_saved_tickets(sport_label: str | None = None) -> pd.DataFrame:
    with SessionLocal() as db:
        stmt = select(SavedTicket)
        if sport_label:
            stmt = stmt.where(SavedTicket.sport_label == sport_label)
        rows = db.execute(stmt).scalars().all()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            (
                lambda note_text, metadata: {
                    "ticket_id": row.id,
                    "name": row.name,
                    "sport_label": row.sport_label,
                    "source": row.source,
                    "leg_count": row.leg_count,
                    "avg_confidence": row.avg_confidence,
                    "avg_model_prob": row.avg_model_prob,
                    "status": row.status,
                    "notes": note_text,
                    "ticket_metadata": metadata,
                    "created_at": row.created_at,
                    "dfs_target_app": str(metadata.get("dfs_target_label") or ""),
                    "dfs_target_key": str(metadata.get("dfs_target_key") or ""),
                    "build_candidate_pool": str(metadata.get("candidate_pool") or ""),
                    "build_style": str(metadata.get("style") or ""),
                    "build_min_confidence": metadata.get("min_confidence"),
                    "build_allow_same_player": bool(metadata.get("allow_same_player", False)),
                    "build_allow_same_team": bool(metadata.get("allow_same_team", False)),
                    "build_smart_profile_mode": str(metadata.get("smart_profile_mode") or ""),
                }
            )(*unpack_ticket_notes(row.notes))
            for row in rows
        ]
    ).sort_values("created_at", ascending=False)


def get_ticket_legs(ticket_id: int) -> pd.DataFrame:
    with SessionLocal() as db:
        rows = db.execute(select(SavedTicketLeg).where(SavedTicketLeg.ticket_id == ticket_id)).scalars().all()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "ticket_leg_id": row.id,
                "ticket_id": row.ticket_id,
                "leg_rank": row.leg_rank,
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
                "model_prob": row.model_prob,
                "confidence": row.confidence,
                "status": row.status,
            }
            for row in rows
        ]
    ).sort_values("leg_rank")


def get_ticket_legs_with_results(ticket_id: int, sport_label: str) -> pd.DataFrame:
    legs = get_ticket_legs(ticket_id)
    if legs.empty:
        return pd.DataFrame()

    sport_key = get_sport_config(sport_label)["live_keys"][0]
    results = get_prop_results(sport_key)
    if results.empty:
        legs["actual_value"] = None
        legs["winning_side"] = None
        legs["grade"] = "open"
        return legs

    merged = legs.merge(
        results[["event_id", "market", "player", "actual_value", "winning_side"]],
        on=["event_id", "market", "player"],
        how="left",
    )

    def _grade_leg(row: pd.Series) -> str:
        side = str(row.get("side") or "").lower()
        line = row.get("line")
        actual_value = row.get("actual_value")
        winning_side = str(row.get("winning_side") or "").lower()

        if side in {"over", "under"} and pd.notnull(actual_value) and pd.notnull(line):
            if side == "over":
                return "win" if actual_value > line else "loss" if actual_value < line else "push"
            return "win" if actual_value < line else "loss" if actual_value > line else "push"

        if side in {"yes", "no"} and winning_side in {"yes", "no"}:
            return "win" if side == winning_side else "loss"

        return "open"

    merged["grade"] = merged.apply(_grade_leg, axis=1)
    return merged.sort_values("leg_rank")


def get_ticket_summary_with_grades(sport_label: str | None = None) -> pd.DataFrame:
    tickets = get_saved_tickets(sport_label)
    if tickets.empty:
        return pd.DataFrame()

    graded_rows: list[dict] = []
    for _, ticket in tickets.iterrows():
        legs = get_ticket_legs(int(ticket["ticket_id"]))
        if legs.empty:
            continue

        resolved_legs = 0
        won_legs = 0
        push_legs = 0
        open_legs = 0

        if ticket["source"] == "live_edges":
            sport_key = get_sport_config(str(ticket["sport_label"]))["live_keys"][0]
            results = get_prop_results(sport_key)
            if not results.empty:
                merged = legs.merge(
                    results[["event_id", "market", "player", "actual_value", "winning_side"]],
                    on=["event_id", "market", "player"],
                    how="left",
                )
            else:
                merged = legs.copy()
                merged["actual_value"] = None
                merged["winning_side"] = None

            for _, leg in merged.iterrows():
                side = str(leg.get("side") or "").lower()
                line = leg.get("line")
                actual_value = leg.get("actual_value")
                winning_side = str(leg.get("winning_side") or "").lower()

                grade = "open"
                if side in {"over", "under"} and pd.notnull(actual_value) and pd.notnull(line):
                    if side == "over":
                        grade = "win" if actual_value > line else "loss" if actual_value < line else "push"
                    else:
                        grade = "win" if actual_value < line else "loss" if actual_value > line else "push"
                elif side in {"yes", "no"} and winning_side in {"yes", "no"}:
                    grade = "win" if side == winning_side else "loss"

                if grade == "win":
                    resolved_legs += 1
                    won_legs += 1
                elif grade == "loss":
                    resolved_legs += 1
                elif grade == "push":
                    resolved_legs += 1
                    push_legs += 1
                else:
                    open_legs += 1
        else:
            open_legs = len(legs)

        ticket_status = "open"
        if open_legs == 0 and len(legs) > 0:
            ticket_status = "won" if won_legs + push_legs == len(legs) else "lost"
            if won_legs == 0 and push_legs == len(legs):
                ticket_status = "push"

        ticket_outcome_score = None
        resolved_ratio = None
        if len(legs) > 0:
            resolved_ratio = resolved_legs / len(legs)
        if open_legs == 0 and len(legs) > 0:
            ticket_outcome_score = (won_legs + (push_legs * 0.5)) / len(legs)

        graded_rows.append(
            {
                **ticket.to_dict(),
                "resolved_legs": resolved_legs,
                "won_legs": won_legs,
                "push_legs": push_legs,
                "open_legs": open_legs,
                "resolved_ratio": resolved_ratio,
                "ticket_outcome_score": ticket_outcome_score,
                "ticket_status_live": ticket_status,
            }
        )

    if not graded_rows:
        return tickets

    return pd.DataFrame(graded_rows).sort_values("created_at", ascending=False)


def export_ticket_legs_for_csv(sport_label: str | None = None) -> pd.DataFrame:
    tickets = get_saved_tickets(sport_label)
    if tickets.empty:
        return pd.DataFrame()

    legs_frames = []
    for _, ticket in tickets.iterrows():
        legs = get_ticket_legs(int(ticket["ticket_id"]))
        if legs.empty:
            continue
        legs = legs.copy()
        legs.insert(1, "ticket_name", ticket["name"])
        legs.insert(2, "sport_label", ticket["sport_label"])
        legs.insert(3, "ticket_source", ticket["source"])
        legs_frames.append(legs)

    if not legs_frames:
        return pd.DataFrame()

    return pd.concat(legs_frames, ignore_index=True)


def import_ticket_legs_csv(file_bytes: bytes, default_source: str = "csv_import") -> dict[str, int]:
    df = pd.read_csv(BytesIO(file_bytes))
    required = ["ticket_name", "sport_label", "market", "player", "pick"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError("Ticket legs CSV is missing required columns: " + ", ".join(missing))

    tickets_created = 0
    grouped = df.groupby("ticket_name", dropna=False)
    for ticket_name, ticket_df in grouped:
        sport_label = str(ticket_df.iloc[0].get("sport_label") or "").strip()
        if not sport_label:
            continue
        source = str(ticket_df.iloc[0].get("ticket_source") or default_source).strip()
        ticket_id = save_ticket(
            ticket_name=str(ticket_name),
            sport_label=sport_label,
            source=source,
            legs_df=ticket_df.rename(
                columns={
                    "market": "market",
                    "player": "player",
                    "pick": "pick",
                    "sportsbook": "sportsbook",
                    "book_key": "book_key",
                    "event_id": "event_id",
                    "line": "line",
                    "price": "price",
                    "projection": "projection",
                    "model_prob": "model_prob",
                    "confidence": "confidence",
                    "side": "side",
                    "leg_rank": "leg_rank",
                }
            ),
            notes=str(ticket_df.iloc[0].get("notes")) if pd.notnull(ticket_df.iloc[0].get("notes")) else None,
        )
        if ticket_id:
            tickets_created += 1

    return {"tickets_created": tickets_created}
