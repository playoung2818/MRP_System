from __future__ import annotations

import os
from typing import Any
from pathlib import Path

from sqlalchemy import create_engine


def _load_env_file_fallback(env_path: str, *, override: bool = False) -> None:
    """
    Minimal .env loader used when python-dotenv is unavailable.
    """
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and (override or key not in os.environ):
                    os.environ[key] = value
    except Exception:
        # Do not fail import due to optional local env loading.
        pass


try:

    from dotenv import load_dotenv  # type: ignore

    # Load from current working dir first.
    load_dotenv()  # CWD

    # Prefer repository-local .env (ERP_System 2.0/.env), then runtime/.env.
    env_root = Path(__file__).resolve().parents[2] / ".env"
    env_runtime = Path(__file__).resolve().parent / ".env"
    if env_root.exists():
        load_dotenv(env_root, override=True)
    elif env_runtime.exists():
        load_dotenv(env_runtime, override=True)
except Exception:
    # If python-dotenv is not installed, fall back to a minimal parser.
    _load_env_file_fallback(".env")
    _load_env_file_fallback(str(Path(__file__).resolve().parents[2] / ".env"), override=True)
    _load_env_file_fallback(str(Path(__file__).resolve().parent / ".env"), override=True)


DATABASE_DSN: str | None = os.getenv("DATABASE_DSN")
DUCKDB_PATH: str | None = os.getenv("DUCKDB_PATH")


def resolve_dsn() -> str | None:
    """
    Resolve an effective DSN from environment.
    Priority:
      1) DATABASE_DSN
      2) DUCKDB_PATH -> duckdb:///...
    """
    if DATABASE_DSN:
        return DATABASE_DSN
    if DUCKDB_PATH:
        path = DUCKDB_PATH.replace("\\", "/")
        return f"duckdb:///{path}"
    return None


def require_dsn() -> str:
    """
    Return DATABASE_DSN or raise a clear error if it's missing.
    """
    dsn = resolve_dsn()
    if not dsn:
        raise RuntimeError(
            "Database DSN is not set. Set DATABASE_DSN, or set DUCKDB_PATH "
            "to use a local DuckDB file."
        )
    return dsn


def get_engine(*, pool_pre_ping: bool = True, **kwargs: Any):
    """
    Central place to construct a SQLAlchemy engine.
    All components (ETL, IO ops, web server) should call this instead
    of using create_engine directly so the DSN and options stay in sync.
    """
    dsn = require_dsn()
    # DuckDB engines do not need connection pool pre-ping.
    if dsn.startswith("duckdb:"):
        return create_engine(dsn, **kwargs)
    return create_engine(dsn, pool_pre_ping=pool_pre_ping, **kwargs)


__all__ = ["DATABASE_DSN", "DUCKDB_PATH", "resolve_dsn", "get_engine", "require_dsn"]
