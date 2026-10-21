"""Cached, read-only alias_part -> canonical_part lookup from the database."""
from threading import RLock

from sqlalchemy import text


_lock = RLock()
_aliases: dict[str, str] | None = None


def _build_alias_map(rows) -> dict[str, str]:
    aliases = {}
    canonical_names = {}
    for row in rows:
        alias = str(row["alias_part"]).strip() if row["alias_part"] is not None else ""
        canonical = str(row["canonical_part"]).strip() if row["canonical_part"] is not None else ""
        if not alias or not canonical:
            raise ValueError("part_number_aliases rows require alias_part and canonical_part.")
        key = alias.casefold()
        previous = aliases.get(key)
        if previous is not None and previous != canonical:
            raise ValueError(f"Alias {alias!r} has conflicting canonical parts: {previous!r}, {canonical!r}.")
        aliases[key] = canonical
        canonical_key = canonical.casefold()
        previous = canonical_names.get(canonical_key)
        if previous is not None and previous != canonical:
            raise ValueError(f"Canonical part {canonical!r} has inconsistent capitalization.")
        canonical_names[canonical_key] = canonical
    # Canonical parts are terminal: normalization must not map them elsewhere.
    for key, canonical in canonical_names.items():
        if key in aliases and aliases[key] != canonical:
            raise ValueError(f"Canonical part {canonical!r} is also an alias of {aliases[key]!r}.")
        aliases[key] = canonical
    return aliases


def refresh_part_number_aliases(engine=None) -> int:
    """Refresh once per ETL/reload; failed reads retain the previous cache.

    With no engine supplied, create and dispose a temporary engine. No database
    access happens merely by importing this module.
    """
    global _aliases
    from mrp_system.runtime.db_config import get_engine

    owned_engine = engine is None
    with _lock:
        engine = get_engine() if owned_engine else engine
        try:
            with engine.connect() as conn:
                rows = conn.execute(text('''SELECT alias_part, canonical_part
                    FROM public.part_number_aliases ORDER BY id''')).mappings().all()
            updated = _build_alias_map(rows)
        finally:
            if owned_engine:
                engine.dispose()
        _aliases = updated  # Atomic replacement: readers never see a partial map.
    return len(rows)


def canonical_part_for(name: str) -> str | None:
    """Case-insensitive lookup preserving the exact canonical spelling in DB."""
    if _aliases is None:
        with _lock:
            if _aliases is None:
                refresh_part_number_aliases()
    return _aliases.get(name.strip().casefold())
