import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

default_sqlite_path = os.getenv("SQLITE_PATH", "parlay_builder.db")
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{default_sqlite_path}")

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()
