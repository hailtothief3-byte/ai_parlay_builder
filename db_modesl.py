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
