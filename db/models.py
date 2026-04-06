from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    Text,
    UniqueConstraint,
)
from db.session import Base


class Event(Base):
    __tablename__ = "events"

    id = Column(Integer, primary_key=True)
    external_event_id = Column(String, index=True, nullable=False, unique=True)
    sport_key = Column(String, index=True, nullable=False)
    commence_time = Column(DateTime, index=True)
    home_team = Column(String, index=True)
    away_team = Column(String, index=True)


class MarketLine(Base):
    __tablename__ = "market_lines"

    id = Column(Integer, primary_key=True)
    external_event_id = Column(String, index=True, nullable=False)
    sport_key = Column(String, index=True, nullable=False)
    bookmaker_key = Column(String, index=True, nullable=False)
    bookmaker_title = Column(String, nullable=False)
    market_key = Column(String, index=True, nullable=False)

    player_name = Column(String, index=True, nullable=True)
    outcome_name = Column(String, nullable=False)
    line = Column(Float, nullable=True)
    price = Column(Float, nullable=True)

    side = Column(String, nullable=True)
    is_dfs = Column(Boolean, default=False)

    event_commence_time = Column(DateTime, index=True, nullable=True)
    last_update = Column(DateTime, index=True, nullable=True)
    pulled_at = Column(DateTime, index=True, nullable=False)

    raw_json = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "external_event_id",
            "bookmaker_key",
            "market_key",
            "player_name",
            "outcome_name",
            "line",
            "last_update",
            name="uq_market_snapshot",
        ),
    )


class PropProjection(Base):
    __tablename__ = "prop_projections"

    id = Column(Integer, primary_key=True)

    sport_key = Column(String, index=True, nullable=False)
    external_event_id = Column(String, index=True, nullable=False)
    player_name = Column(String, index=True, nullable=False)
    market_key = Column(String, index=True, nullable=False)

    projection = Column(Float, nullable=False)
    std_dev = Column(Float, nullable=True)
    over_prob = Column(Float, nullable=True)
    under_prob = Column(Float, nullable=True)

    confidence = Column(Float, nullable=True)
    model_name = Column(String, nullable=True)

    created_at = Column(DateTime, index=True, nullable=False)


class TrackedPick(Base):
    __tablename__ = "tracked_picks"

    id = Column(Integer, primary_key=True)

    sport_key = Column(String, index=True, nullable=False)
    external_event_id = Column(String, index=True, nullable=False)
    bookmaker_key = Column(String, index=True, nullable=False)
    bookmaker_title = Column(String, nullable=False)
    market_key = Column(String, index=True, nullable=False)
    player_name = Column(String, index=True, nullable=False)

    pick = Column(String, nullable=False)
    side = Column(String, nullable=True)
    line = Column(Float, nullable=True)
    price = Column(Float, nullable=True)
    projection = Column(Float, nullable=True)
    implied_prob = Column(Float, nullable=True)
    model_prob = Column(Float, nullable=True)
    edge = Column(Float, nullable=True)
    confidence = Column(Float, nullable=True)
    is_dfs = Column(Boolean, default=False)

    source = Column(String, nullable=True)
    tracked_at = Column(DateTime, index=True, nullable=False)


class PropResult(Base):
    __tablename__ = "prop_results"

    id = Column(Integer, primary_key=True)

    sport_key = Column(String, index=True, nullable=False)
    external_event_id = Column(String, index=True, nullable=False)
    market_key = Column(String, index=True, nullable=False)
    player_name = Column(String, index=True, nullable=False)

    actual_value = Column(Float, nullable=True)
    winning_side = Column(String, nullable=True)
    status = Column(String, nullable=False, default="settled")
    source = Column(String, nullable=True)
    notes = Column(Text, nullable=True)
    settled_at = Column(DateTime, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "sport_key",
            "external_event_id",
            "market_key",
            "player_name",
            name="uq_prop_result_market",
        ),
    )


class SavedTicket(Base):
    __tablename__ = "saved_tickets"

    id = Column(Integer, primary_key=True)

    name = Column(String, index=True, nullable=False)
    sport_label = Column(String, index=True, nullable=False)
    source = Column(String, index=True, nullable=False)
    ticket_type = Column(String, nullable=False, default="parlay")
    leg_count = Column(Integer, nullable=False, default=0)
    avg_confidence = Column(Float, nullable=True)
    avg_model_prob = Column(Float, nullable=True)
    status = Column(String, index=True, nullable=False, default="open")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, index=True, nullable=False)


class SavedTicketLeg(Base):
    __tablename__ = "saved_ticket_legs"

    id = Column(Integer, primary_key=True)

    ticket_id = Column(Integer, index=True, nullable=False)
    leg_rank = Column(Integer, nullable=False)
    sport_key = Column(String, index=True, nullable=True)
    external_event_id = Column(String, index=True, nullable=True)
    bookmaker_key = Column(String, index=True, nullable=True)
    bookmaker_title = Column(String, nullable=True)
    market_key = Column(String, index=True, nullable=False)
    player_name = Column(String, index=True, nullable=False)
    pick = Column(String, nullable=False)
    side = Column(String, nullable=True)
    line = Column(Float, nullable=True)
    price = Column(Float, nullable=True)
    projection = Column(Float, nullable=True)
    model_prob = Column(Float, nullable=True)
    confidence = Column(Float, nullable=True)
    status = Column(String, nullable=False, default="open")


class PlayerStatSnapshot(Base):
    __tablename__ = "player_stat_snapshots"

    id = Column(Integer, primary_key=True)

    sport_key = Column(String, index=True, nullable=False)
    player_name = Column(String, index=True, nullable=False)
    market_key = Column(String, index=True, nullable=False)
    season_average = Column(Float, nullable=True)
    recent_average = Column(Float, nullable=True)
    last_5_average = Column(Float, nullable=True)
    trend = Column(Float, nullable=True)
    sample_size = Column(Integer, nullable=True)
    source = Column(String, nullable=True)
    created_at = Column(DateTime, index=True, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "sport_key",
            "player_name",
            "market_key",
            "source",
            name="uq_player_stat_snapshot",
        ),
    )


class BankrollJournalEntry(Base):
    __tablename__ = "bankroll_journal_entries"

    id = Column(Integer, primary_key=True)

    entry_type = Column(String, index=True, nullable=False, default="ticket")
    sport_label = Column(String, index=True, nullable=True)
    ticket_id = Column(Integer, index=True, nullable=True)
    tracked_pick_id = Column(Integer, index=True, nullable=True)
    label = Column(String, nullable=False)
    stake_dollars = Column(Float, nullable=False, default=0.0)
    stake_units = Column(Float, nullable=True)
    suggested_stake_dollars = Column(Float, nullable=True)
    suggested_stake_units = Column(Float, nullable=True)
    potential_payout_dollars = Column(Float, nullable=True)
    realized_profit_dollars = Column(Float, nullable=True)
    status = Column(String, index=True, nullable=False, default="open")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, index=True, nullable=False)
    resolved_at = Column(DateTime, index=True, nullable=True)
