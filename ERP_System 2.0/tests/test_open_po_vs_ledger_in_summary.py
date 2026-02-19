from __future__ import annotations

from pathlib import Path
import sys
from uuid import uuid4

import pandas as pd
from sqlalchemy import text


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_config import get_engine  # noqa: E402
from core import _norm_key  # noqa: E402


EPS = 1e-9
SCHEMA = "public"
REPORT_TABLE = "qa_open_po_vs_ledger_in_report"


def _short(df: pd.DataFrame, n: int = 30) -> str:
    if df.empty:
        return "(none)"
    return df.head(n).to_string(index=False)


def _ensure_report_schema(eng) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.{REPORT_TABLE} (
        run_id text,
        created_at_utc timestamptz,
        row_type text,
        filter_used text,
        item_key text,
        open_po_qty double precision,
        ledger_in_qty double precision,
        gap_qty double precision,
        items_in_open_po double precision,
        pod_list text
    );
    ALTER TABLE {SCHEMA}.{REPORT_TABLE} ADD COLUMN IF NOT EXISTS pod_list text;
    ALTER TABLE {SCHEMA}.{REPORT_TABLE} DROP COLUMN IF EXISTS items_in_ledger_in;
    ALTER TABLE {SCHEMA}.{REPORT_TABLE} DROP COLUMN IF EXISTS mismatch_items;
    ALTER TABLE {SCHEMA}.{REPORT_TABLE} DROP COLUMN IF EXISTS open_po_total_qty;
    ALTER TABLE {SCHEMA}.{REPORT_TABLE} DROP COLUMN IF EXISTS ledger_in_total_qty;
    ALTER TABLE {SCHEMA}.{REPORT_TABLE} DROP COLUMN IF EXISTS total_gap_qty;
    """
    with eng.begin() as conn:
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            conn.execute(text(stmt))


def _write_report_to_db(
    eng,
    *,
    filter_used: str,
    pod_item: pd.DataFrame,
    mismatch: pd.DataFrame,
    pod_list_by_item: dict[str, str],
) -> str:
    _ensure_report_schema(eng)

    run_id = str(uuid4())
    created_at = pd.Timestamp.utcnow()

    rows = [
        {
            "run_id": run_id,
            "created_at_utc": created_at,
            "row_type": "summary",
            "filter_used": filter_used,
            "item_key": None,
            "open_po_qty": None,
            "ledger_in_qty": None,
            "gap_qty": None,
            "items_in_open_po": int(len(pod_item)),
            "pod_list": None,
        }
    ]

    for _, r in mismatch.iterrows():
        item_key = str(r["item_key"])
        rows.append(
            {
                "run_id": run_id,
                "created_at_utc": created_at,
                "row_type": "mismatch",
                "filter_used": filter_used,
                "item_key": item_key,
                "open_po_qty": float(r["open_po_qty"]),
                "ledger_in_qty": float(r["ledger_in_qty"]),
                "gap_qty": float(r["gap_qty"]),
                "items_in_open_po": None,
                "pod_list": pod_list_by_item.get(item_key, ""),
            }
        )

    pd.DataFrame(rows).to_sql(
        REPORT_TABLE,
        eng,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    return run_id


def test_open_po_vs_ledger_in_item_qty_summary() -> None:
    eng = get_engine()

    pod = pd.read_sql(
        """
        SELECT "Item" AS item, "Qty(+)" AS qty, "Name" AS vendor_name, "POD#" AS pod_no
        FROM public."Open_Purchase_Orders"
        """,
        eng,
    )
    pod["item_key"] = _norm_key(pod["item"])
    pod["qty"] = pd.to_numeric(pod["qty"], errors="coerce").fillna(0.0)
    pod["vendor_name"] = pod["vendor_name"].fillna("").astype(str).str.strip()
    pod["pod_no"] = pod["pod_no"].fillna("").astype(str).str.strip()
    pod = pod.loc[pod["qty"] > 0].copy()
    pod = pod.loc[~pod["vendor_name"].isin(["CoastIPC, Inc.", "Industrial PC, Inc."])].copy()
    # Align scope with ETL shipping filter by keeping only PODs present in NT Shipping Schedule table.
    allowed_pod = pd.read_sql(
        'SELECT DISTINCT "QB Num" AS pod_no FROM public."NT Shipping Schedule"',
        eng,
    )
    allowed_set = set(allowed_pod["pod_no"].fillna("").astype(str).str.strip())
    pod = pod.loc[pod["pod_no"].isin(allowed_set)].copy()
    pod_item = (
        pod.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "open_po_qty"})
    )
    pod_list_by_item = (
        pod.loc[pod["pod_no"].ne(""), ["item_key", "pod_no"]]
        .drop_duplicates()
        .sort_values(["item_key", "pod_no"])
        .groupby("item_key")["pod_no"]
        .agg(", ".join)
        .to_dict()
    )

    led = pd.read_sql(
        """
        SELECT "Item" AS item, "Delta" AS qty
        FROM public."ledger_analytics"
        WHERE "Kind" = 'IN'
        """,
        eng,
    )
    filter_used = "Kind='IN' + POD# in NT Shipping Schedule (Ship-to filtered)"

    led["item_key"] = _norm_key(led["item"])
    led["qty"] = pd.to_numeric(led["qty"], errors="coerce").fillna(0.0)
    led = led.loc[led["qty"] > 0].copy()
    led_item = (
        led.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ledger_in_qty"})
    )

    cmp = pod_item.merge(led_item, on="item_key", how="outer")
    cmp["open_po_qty"] = pd.to_numeric(cmp["open_po_qty"], errors="coerce").fillna(0.0)
    cmp["ledger_in_qty"] = pd.to_numeric(cmp["ledger_in_qty"], errors="coerce").fillna(0.0)
    cmp["gap_qty"] = cmp["ledger_in_qty"] - cmp["open_po_qty"]

    mismatch = cmp.loc[cmp["gap_qty"].abs() > EPS].sort_values("gap_qty", ascending=False)
    mismatch["pod_list"] = mismatch["item_key"].map(pod_list_by_item).fillna("")

    run_id = _write_report_to_db(
        eng,
        filter_used=filter_used,
        pod_item=pod_item,
        mismatch=mismatch,
        pod_list_by_item=pod_list_by_item,
    )

    print(f"\n[Filter used] {filter_used}")
    print("[Summary]")
    print(pd.DataFrame([{"items_in_open_po": int(len(pod_item))}]).to_string(index=False))
    print("\n[Top mismatches by item]")
    print(_short(mismatch[["item_key", "open_po_qty", "ledger_in_qty", "gap_qty", "pod_list"]]))
    print(f"\n[LOG] Supabase run_id: {run_id} (table: {SCHEMA}.{REPORT_TABLE})")

    assert mismatch.empty, "Open_Purchase_Orders vs ledger inbound qty mismatch by item."
