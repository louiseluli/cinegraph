"""
Centralized database helpers for CineGraph.

What this file provides:
- A single SQLAlchemy Engine created from the env var DATABASE_URL.
- A SessionLocal factory for ORM sessions (one per request).
- A FastAPI dependency `get_db()` to inject sessions into routes/services.
- A lightweight `ping_db()` to check connectivity.

Notes:
- We use SQLAlchemy 2.x style (but sessions work the same).
- Alembic migrations will hook into the same ENGINE later.
"""

from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ---- Configuration ----
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://cinegraph_user:changeme_strong_password@db:5432/cinegraph"
)

# ---- Engine & Session Factory ----
# pool_pre_ping: detect dropped connections
ENGINE = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=5,
    future=True,  # opt into SQLAlchemy 2.x behavior where helpful
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=ENGINE,
    future=True,
)

def get_db() -> Generator:
    """
    FastAPI dependency that yields a DB session per request.
    Always closes the session once the request is done.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def ping_db() -> bool:
    """
    Lightweight connectivity check. Returns True if SELECT 1 works.
    """
    try:
        with ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
