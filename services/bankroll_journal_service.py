from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import select

from db.models import BankrollJournalEntry
from db.session import SessionLocal
from services.bankroll_service import compute_parlay_decimal_odds
from services.ticket_service import get_ticket_legs, get_ticket_summary_with_grades


def add_journal_entry(
    entry_type: str,
    label: str,
    stake_dollars: float,
    stake_units: float | None = None,
    suggested_stake_dollars: float | None = None,
    suggested_stake_units: float | None = None,
    sport_label: str | None = None,
    ticket_id: int | None = None,
    tracked_pick_id: int | None = None,
    potential_payout_dollars: float | None = None,
    notes: str | None = None,
) -> int:
    created_at = datetime.now(timezone.utc)
    with SessionLocal() as db:
        entry = BankrollJournalEntry(
            entry_type=entry_type,
            sport_label=sport_label,
            ticket_id=ticket_id,
            tracked_pick_id=tracked_pick_id,
            label=label,
            stake_dollars=stake_dollars,
            stake_units=stake_units,
            suggested_stake_dollars=suggested_stake_dollars,
            suggested_stake_units=suggested_stake_units,
            potential_payout_dollars=potential_payout_dollars,
            status="open",
            notes=notes,
            created_at=created_at,
        )
        db.add(entry)
        db.commit()
        db.refresh(entry)
        return int(entry.id)


def settle_journal_entry(entry_id: int, realized_profit_dollars: float, status: str) -> None:
    with SessionLocal() as db:
        entry = db.execute(
            select(BankrollJournalEntry).where(BankrollJournalEntry.id == entry_id)
        ).scalar_one_or_none()
        if not entry:
            return
        entry.realized_profit_dollars = realized_profit_dollars
        entry.status = status
        entry.resolved_at = datetime.now(timezone.utc)
        db.commit()


def get_journal_entries(sport_label: str | None = None) -> pd.DataFrame:
    with SessionLocal() as db:
        stmt = select(BankrollJournalEntry)
        if sport_label:
            stmt = stmt.where(
                (BankrollJournalEntry.sport_label == sport_label) | (BankrollJournalEntry.sport_label.is_(None))
            )
        rows = db.execute(stmt).scalars().all()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        [
            {
                "journal_entry_id": row.id,
                "entry_type": row.entry_type,
                "sport_label": row.sport_label,
                "ticket_id": row.ticket_id,
                "tracked_pick_id": row.tracked_pick_id,
                "label": row.label,
                "stake_dollars": row.stake_dollars,
                "stake_units": row.stake_units,
                "suggested_stake_dollars": row.suggested_stake_dollars,
                "suggested_stake_units": row.suggested_stake_units,
                "potential_payout_dollars": row.potential_payout_dollars,
                "realized_profit_dollars": row.realized_profit_dollars,
                "status": row.status,
                "notes": row.notes,
                "created_at": row.created_at,
                "resolved_at": row.resolved_at,
            }
            for row in rows
        ]
    ).sort_values("created_at", ascending=False)


def build_bankroll_summary(entries_df: pd.DataFrame, starting_bankroll: float) -> dict[str, float]:
    if entries_df.empty:
        return {
            "starting_bankroll": starting_bankroll,
            "open_risk": 0.0,
            "realized_profit": 0.0,
            "current_bankroll": starting_bankroll,
        }

    open_risk = float(entries_df.loc[entries_df["status"] == "open", "stake_dollars"].sum())
    realized_profit = float(entries_df["realized_profit_dollars"].fillna(0.0).sum())
    current_bankroll = float(starting_bankroll + realized_profit)
    return {
        "starting_bankroll": round(starting_bankroll, 2),
        "open_risk": round(open_risk, 2),
        "realized_profit": round(realized_profit, 2),
        "current_bankroll": round(current_bankroll, 2),
    }


def build_bankroll_kpis(entries_df: pd.DataFrame, starting_bankroll: float) -> dict[str, float]:
    if entries_df.empty:
        return {
            "turnover": 0.0,
            "resolved_stake": 0.0,
            "roi": 0.0,
            "yield_pct": 0.0,
            "open_entries": 0,
            "resolved_entries": 0,
            "win_rate": 0.0,
            "avg_stake": 0.0,
            "bankroll_change_pct": 0.0,
        }

    resolved = entries_df[entries_df["status"].isin(["won", "lost", "push"])].copy()
    turnover = float(entries_df["stake_dollars"].fillna(0.0).sum())
    resolved_stake = float(resolved["stake_dollars"].fillna(0.0).sum()) if not resolved.empty else 0.0
    realized_profit = float(entries_df["realized_profit_dollars"].fillna(0.0).sum())
    roi = realized_profit / starting_bankroll if starting_bankroll > 0 else 0.0
    yield_pct = realized_profit / resolved_stake if resolved_stake > 0 else 0.0
    open_entries = int((entries_df["status"] == "open").sum())
    resolved_entries = int(len(resolved))
    win_rate = float((resolved["status"] == "won").mean()) if not resolved.empty else 0.0
    avg_stake = float(entries_df["stake_dollars"].fillna(0.0).mean()) if not entries_df.empty else 0.0
    bankroll_change_pct = realized_profit / starting_bankroll if starting_bankroll > 0 else 0.0
    return {
        "turnover": round(turnover, 2),
        "resolved_stake": round(resolved_stake, 2),
        "roi": round(roi, 4),
        "yield_pct": round(yield_pct, 4),
        "open_entries": open_entries,
        "resolved_entries": resolved_entries,
        "win_rate": round(win_rate, 4),
        "avg_stake": round(avg_stake, 2),
        "bankroll_change_pct": round(bankroll_change_pct, 4),
    }


def sync_ticket_journal_entries(sport_label: str) -> dict[str, int]:
    entries_df = get_journal_entries(sport_label)
    if entries_df.empty:
        return {"settled_entries": 0, "won_entries": 0, "lost_entries": 0, "push_entries": 0}

    open_ticket_entries = entries_df[
        (entries_df["entry_type"] == "ticket")
        & (entries_df["status"] == "open")
        & (entries_df["ticket_id"].notna())
    ].copy()
    if open_ticket_entries.empty:
        return {"settled_entries": 0, "won_entries": 0, "lost_entries": 0, "push_entries": 0}

    ticket_summary = get_ticket_summary_with_grades(sport_label)
    if ticket_summary.empty:
        return {"settled_entries": 0, "won_entries": 0, "lost_entries": 0, "push_entries": 0}

    settled_entries = 0
    won_entries = 0
    lost_entries = 0
    push_entries = 0

    for _, entry in open_ticket_entries.iterrows():
        ticket_id = int(entry["ticket_id"])
        ticket_row = ticket_summary[ticket_summary["ticket_id"] == ticket_id].head(1)
        if ticket_row.empty:
            continue

        ticket_status = str(ticket_row.iloc[0]["ticket_status_live"])
        if ticket_status not in {"won", "lost", "push"}:
            continue

        stake_dollars = float(entry.get("stake_dollars") or 0.0)
        realized_profit = 0.0
        if ticket_status == "won":
            legs_df = get_ticket_legs(ticket_id)
            decimal_odds = compute_parlay_decimal_odds(legs_df)
            realized_profit = stake_dollars * max(0.0, decimal_odds - 1.0)
            won_entries += 1
        elif ticket_status == "lost":
            realized_profit = -stake_dollars
            lost_entries += 1
        else:
            realized_profit = 0.0
            push_entries += 1

        settle_journal_entry(entry_id=int(entry["journal_entry_id"]), realized_profit_dollars=round(realized_profit, 2), status=ticket_status)
        settled_entries += 1

    return {
        "settled_entries": settled_entries,
        "won_entries": won_entries,
        "lost_entries": lost_entries,
        "push_entries": push_entries,
    }
