"""Read-only queries for the homepage item price-history chart (public.item_cost)."""
from __future__ import annotations

from typing import Callable

from sqlalchemy import text

SUGGEST_SQL = text("""
    SELECT item, COUNT(*) AS purchases, MAX(order_date) AS last_date
    FROM public.item_cost
    WHERE lower(item) LIKE lower(:pattern) ESCAPE '!' AND qty > 0 AND amount > 0
    GROUP BY item
    ORDER BY (CASE WHEN lower(item) LIKE lower(:prefix) ESCAPE '!' THEN 0 ELSE 1 END), MAX(order_date) DESC, item
    LIMIT 15
""")

MATCH_SQL = text("SELECT DISTINCT item FROM public.item_cost WHERE lower(item) = lower(:item) LIMIT 2")

# One point per POD + item: the most recently seen version (latest last_seen,
# then latest first_seen, then newest row). Earlier/revised prices stay in the table.
HISTORY_SQL = text("""
    SELECT order_date, pod_number, qty, unit_cost, vendor FROM (
        SELECT order_date, pod_number, qty, unit_cost, vendor,
               ROW_NUMBER() OVER (PARTITION BY pod_number
                                  ORDER BY last_seen DESC, first_seen DESC, id DESC) AS version
        FROM public.item_cost
        WHERE item = :item AND qty > 0 AND amount > 0 AND unit_cost IS NOT NULL
    ) latest
    WHERE version = 1
    ORDER BY order_date, pod_number
""")


def _iso(value) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)[:10]


def _escape_like(value: str) -> str:
    return value.replace("!", "!!").replace("%", "!%").replace("_", "!_")


def suggest_items(engine, query: str) -> list[dict]:
    query = (query or "").strip()
    if not query:
        return []
    escaped = _escape_like(query)
    with engine.connect() as conn:
        rows = conn.execute(SUGGEST_SQL, {"pattern": f"%{escaped}%", "prefix": f"{escaped}%"}).mappings().all()
    return [{"item": r["item"], "purchases": int(r["purchases"]),
             "last_date": _iso(r["last_date"])} for r in rows]


def item_history(engine, item: str, normalize: Callable[[str], str] | None = None) -> dict:
    """Resolve the item case-insensitively (then via normalisation) and return its price points."""
    item = (item or "").strip()
    if not item:
        return {"item": None, "points": []}
    candidates = [item]
    if normalize is not None:
        try:
            normalized = str(normalize(item) or "").strip()
        except Exception:
            normalized = ""
        if normalized and normalized.lower() != item.lower():
            candidates.append(normalized)
    with engine.connect() as conn:
        resolved = None
        for candidate in candidates:
            matches = conn.execute(MATCH_SQL, {"item": candidate}).scalars().all()
            if len(matches) == 1:
                resolved = matches[0]
                break
        if resolved is None:
            return {"item": None, "points": []}
        rows = conn.execute(HISTORY_SQL, {"item": resolved}).mappings().all()
    points = [{
        "date": _iso(r["order_date"]),
        "pod": r["pod_number"],
        "qty": float(r["qty"]),
        "unit_cost": float(r["unit_cost"]),
        "vendor": r["vendor"] or "",
    } for r in rows]
    return {"item": resolved, "points": points}
