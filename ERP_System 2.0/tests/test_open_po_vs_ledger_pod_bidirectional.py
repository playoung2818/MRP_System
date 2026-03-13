from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_config import get_engine  # noqa: E402
from core import _norm_key  # noqa: E402


EPS = 1e-9
SCHEMA = "public"
POD_TABLE = "Open_Purchase_Orders"
LEDGER_TABLE = "ledger_analytics"


def _short(df: pd.DataFrame, n: int = 30) -> str:
    if df.empty:
        return "(none)"
    return df.head(n).to_string(index=False)


def _load_open_po(eng) -> pd.DataFrame:
    pod = pd.read_sql(
        f"""
        SELECT
            "Item" AS item,
            "Qty(+)" AS qty,
            "Name" AS vendor_name,
            "POD#" AS pod_no
        FROM public."{POD_TABLE}"
        """,
        eng,
    )
    pod["item_key"] = _norm_key(pod["item"])
    pod["qty"] = pd.to_numeric(pod["qty"], errors="coerce").fillna(0.0)
    pod["vendor_name"] = pod["vendor_name"].fillna("").astype(str).str.strip()
    pod["pod_no"] = pod["pod_no"].fillna("").astype(str).str.strip()
    pod = pod.loc[pod["qty"] > 0].copy()
    return pod


def _load_ledger_pod_in(eng) -> pd.DataFrame:
    led = pd.read_sql(
        f"""
        SELECT
            "Item" AS item,
            "Delta" AS qty,
            "QB Num" AS qb_num,
            "P. O. #" AS po_no
        FROM public."{LEDGER_TABLE}"
        WHERE "Kind" = 'IN'
        """,
        eng,
    )
    led["item_key"] = _norm_key(led["item"])
    led["qty"] = pd.to_numeric(led["qty"], errors="coerce").fillna(0.0)
    led["qb_num"] = led["qb_num"].fillna("").astype(str).str.strip()
    led["po_no"] = led["po_no"].fillna("").astype(str).str.strip()
    led = led.loc[led["qty"] > 0].copy()
    return led


def test_parsed_open_purchase_orders_vs_ledger_pod_bidirectional() -> None:
    eng = get_engine()

    pod = _load_open_po(eng)
    led = _load_ledger_pod_in(eng)

    pod_item = (
        pod.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "open_po_qty"})
    )
    led_item = (
        led.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ledger_pod_qty"})
    )

    pod_list = (
        pod.loc[pod["pod_no"].ne(""), ["item_key", "pod_no"]]
        .drop_duplicates()
        .sort_values(["item_key", "pod_no"])
        .groupby("item_key")["pod_no"]
        .agg(", ".join)
        .to_dict()
    )

    cmp = pod_item.merge(led_item, on="item_key", how="outer")
    cmp["open_po_qty"] = pd.to_numeric(cmp["open_po_qty"], errors="coerce").fillna(0.0)
    cmp["ledger_pod_qty"] = pd.to_numeric(cmp["ledger_pod_qty"], errors="coerce").fillna(0.0)
    cmp["gap_qty"] = cmp["ledger_pod_qty"] - cmp["open_po_qty"]
    cmp["pod_list"] = cmp["item_key"].map(pod_list).fillna("")

    overcount = cmp.loc[cmp["gap_qty"] > EPS].sort_values("gap_qty", ascending=False)
    undercount = cmp.loc[cmp["gap_qty"] < -EPS].sort_values("gap_qty")

    print("\n[Filter used] Open PO uses all rows with Qty(+) > 0")
    print("[Compare] parsed Open_Purchase_Orders vs ledger_analytics Kind='IN'")
    print("\n[Overcount] ledger IN qty > Open PO qty")
    print(_short(overcount[["item_key", "open_po_qty", "ledger_pod_qty", "gap_qty", "pod_list"]]))
    print("\n[Undercount] ledger IN qty < Open PO qty")
    print(_short(undercount[["item_key", "open_po_qty", "ledger_pod_qty", "gap_qty", "pod_list"]]))

    assert overcount.empty, "Serious problem: ledger IN quantity exceeds Open_Purchase_Orders for some items."
    assert undercount.empty, "Serious problem: ledger IN quantity is short vs Open_Purchase_Orders for some items."
