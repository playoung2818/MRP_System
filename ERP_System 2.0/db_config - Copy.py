from __future__ import annotations

import os
from typing import Any

from sqlalchemy import create_engine

try:
    # Optional: load variables from a local .env file if python-dotenv
    # is installed. This keeps secrets out of the code while still being
    # convenient for local development.
    from dotenv import load_dotenv  # type: ignore
    from pathlib import Path

    # Load from current working dir first, then fall back to the .env
    # that sits next to this file so it works regardless of where you run.
    load_dotenv()  # CWD
    env_nearby = Path(__file__).resolve().parent / ".env"
    if env_nearby.exists():
        load_dotenv(env_nearby)
except Exception:
    # If python-dotenv is not installed, we simply skip .env loading.
    pass


# Read the database DSN from environment so credentials
# are not hard-coded in the repository.
#
# Example .env line:
#   DATABASE_DSN=postgresql+psycopg://user:password@host:6543/postgres?sslmode=require
DATABASE_DSN: str | None = os.getenv("DATABASE_DSN")


def require_dsn() -> str:
    """
    Return DATABASE_DSN or raise a clear error if it's missing.
    """
    if not DATABASE_DSN:
        raise RuntimeError(
            "DATABASE_DSN environment variable is not set. "
            "Set it before running the ERP ETL or web server."
        )
    return DATABASE_DSN


def get_engine(*, pool_pre_ping: bool = True, **kwargs: Any):
    """
    Central place to construct a SQLAlchemy engine.
    All components (ETL, IO ops, web server) should call this instead
    of using create_engine directly so the DSN and options stay in sync.
    """
    dsn = require_dsn()
    return create_engine(dsn, pool_pre_ping=pool_pre_ping, **kwargs)


__all__ = ["DATABASE_DSN", "get_engine", "require_dsn"]
