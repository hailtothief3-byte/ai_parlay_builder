import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

try:
    import streamlit as st  # type: ignore
except Exception:  # pragma: no cover
    st = None


def _get_database_url() -> str:
    env_url = os.getenv("DATABASE_URL")
    if env_url:
        return env_url

    try:
        if st is not None and hasattr(st, "secrets") and "DATABASE_URL" in st.secrets:
            return str(st.secrets["DATABASE_URL"])
    except Exception:
        pass

    default_sqlite_path = os.getenv("SQLITE_PATH", "parlay_builder.db")
    return f"sqlite:///{default_sqlite_path}"


DATABASE_URL = _get_database_url()

engine = create_engine(DATABASE_URL, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()
