"""Collect item purchase-price history from saved daily POD_*.CSV exports.

Each export lists only PODs open on that day, so the history is partial by
design: PODs opened and closed between saved snapshots are absent. Lines are
deduplicated across snapshots by POD, item, order date, quantity and amount;
``Amount`` is the full line amount (Open Balance is the unreceived part).

Usage: python -m mrp_system.cli.load_item_cost [--folder PATH] [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import re
from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from mrp_system.runtime.db_config import get_engine
from mrp_system.transform.pod import transform_pod

FILE_RE = re.compile(r"^POD_(\d{1,2})_(\d{1,2})(?:_(\d{4}))?\.csv$", re.IGNORECASE)
KEY = ["pod_number", "item", "order_date", "qty", "amount"]

DDL = """
CREATE TABLE IF NOT EXISTS public.item_cost (
    id bigserial PRIMARY KEY,
    pod_number text NOT NULL,
    item text NOT NULL,
    order_date date NOT NULL,
    qty numeric NOT NULL,
    amount numeric NOT NULL,
    unit_cost numeric,
    vendor text,
    source_name text,
    memo text,
    deliv_date date,
    inventory_site text,
    first_seen date NOT NULL,
    last_seen date NOT NULL,
    snapshot_count integer NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT item_cost_line_uniq UNIQUE (pod_number, item, order_date, qty, amount)
);
CREATE INDEX IF NOT EXISTS item_cost_item_date_idx ON public.item_cost (item, order_date);
"""

UPSERT = """
INSERT INTO public.item_cost (pod_number, item, order_date, qty, amount, unit_cost, vendor,
    source_name, memo, deliv_date, inventory_site, first_seen, last_seen, snapshot_count)
VALUES (:pod_number, :item, :order_date, :qty, :amount, :unit_cost, :vendor, :source_name,
    :memo, :deliv_date, :inventory_site, :first_seen, :last_seen, :snapshot_count)
ON CONFLICT ON CONSTRAINT item_cost_line_uniq DO UPDATE SET
    first_seen = LEAST(item_cost.first_seen, EXCLUDED.first_seen),
    last_seen = GREATEST(item_cost.last_seen, EXCLUDED.last_seen),
    snapshot_count = GREATEST(item_cost.snapshot_count, EXCLUDED.snapshot_count),
    unit_cost = EXCLUDED.unit_cost,
    vendor = COALESCE(EXCLUDED.vendor, item_cost.vendor),
    source_name = COALESCE(EXCLUDED.source_name, item_cost.source_name),
    memo = COALESCE(EXCLUDED.memo, item_cost.memo),
    deliv_date = COALESCE(EXCLUDED.deliv_date, item_cost.deliv_date),
    inventory_site = COALESCE(EXCLUDED.inventory_site, item_cost.inventory_site),
    loaded_at = now()
"""


def default_folder() -> Path:
    return Path(os.environ["OneDrive"]) / "Share NTA Warehouse" / "Daily Update"


def snapshot_date(name: str, max_order: pd.Timestamp | None) -> date | None:
    """Date from the file name; year-less names use the earliest year not before the newest order."""
    m = FILE_RE.match(name)
    if not m:
        return None
    month, day = int(m[1]), int(m[2])
    if m[3]:
        return date(int(m[3]), month, day)
    if max_order is None or pd.isna(max_order):
        return None
    year = max_order.year
    try:
        candidate = date(year, month, day)
        return candidate if candidate >= max_order.date() else date(year + 1, month, day)
    except ValueError:
        return None


def parse_file(path: Path) -> pd.DataFrame:
    raw = pd.read_csv(path, encoding="ISO-8859-1", engine="python")
    if not {"Date", "Num", "Qty", "Amount"}.issubset(raw.columns):
        raise ValueError("missing Date/Num/Qty/Amount columns")
    pod = transform_pod(raw)
    snap = snapshot_date(path.name, pod["Order Date"].max() if not pod.empty else None)
    if snap is None:
        raise ValueError("cannot determine snapshot date")
    out = pd.DataFrame({
        "pod_number": pod["QB Num"].astype(str).str.strip(),
        "item": pod["Item"].astype(str).str.strip(),
        "order_date": pd.to_datetime(pod["Order Date"], errors="coerce").dt.date,
        "qty": pd.to_numeric(pod.get("Qty"), errors="coerce"),
        "amount": pd.to_numeric(pod.get("Amount"), errors="coerce"),
        "vendor": pod.get("Name"),
        "source_name": pod.get("Source Name"),
        "memo": pod.get("Memo"),
        "deliv_date": pd.to_datetime(pod.get("Deliv Date"), errors="coerce").dt.date,
        "inventory_site": pod.get("Inventory Site"),
    })
    out["snapshot"] = snap
    return out.dropna(subset=["pod_number", "item", "order_date", "qty", "amount"])


def collect(folder: Path) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    frames, skipped = [], []
    for path in sorted(folder.rglob("POD_*")):
        if not FILE_RE.match(path.name):
            continue
        try:
            frames.append(parse_file(path))
        except Exception as exc:  # keep going: missing data is acceptable
            skipped.append((path.name, str(exc)[:120]))
    if not frames:
        return pd.DataFrame(), skipped
    lines = pd.concat(frames, ignore_index=True)
    lines = lines.sort_values("snapshot")
    grouped = lines.groupby(KEY, dropna=False, sort=False)
    result = grouped.agg(
        vendor=("vendor", "last"), source_name=("source_name", "last"), memo=("memo", "last"),
        deliv_date=("deliv_date", "last"), inventory_site=("inventory_site", "last"),
        first_seen=("snapshot", "min"), last_seen=("snapshot", "max"),
        snapshot_count=("snapshot", "nunique"),
    ).reset_index()
    qty = result["qty"].astype(float)
    result["unit_cost"] = (result["amount"].astype(float) / qty).where(qty > 0).round(6)
    return result, skipped


def clean(value):
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA or value is pd.NaT:
        return None
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--folder", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true", help="parse and summarise without writing")
    args = parser.parse_args(argv)
    folder = args.folder or default_folder()
    result, skipped = collect(folder)
    print(f"lines={len(result)} pods={result['pod_number'].nunique() if len(result) else 0} "
          f"items={result['item'].nunique() if len(result) else 0} skipped_files={len(skipped)}")
    for name, reason in skipped:
        print(f"  skipped {name}: {reason}")
    if args.dry_run or result.empty:
        return 0
    records = [{k: clean(v) for k, v in row.items()} for row in result.to_dict(orient="records")]
    with get_engine().begin() as conn:
        for statement in DDL.strip().split(";\n"):
            if statement.strip():
                conn.execute(text(statement))
        conn.execute(text(UPSERT), records)
        total = conn.execute(text("SELECT COUNT(*) FROM public.item_cost")).scalar_one()
    print(f"public.item_cost rows: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
