from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from practice.student_tasks import (
    level1_normalize_wo,
    level2_build_pick_flags,
    level3_select_shipping_qty,
    level4_reconcile_open_po_vs_ledger,
    level5_topological_order,
)


def test_level1_normalize_wo() -> None:
    assert level1_normalize_wo("SO-20251769") == "SO-20251769"
    assert level1_normalize_wo("WO02-20251769") == "SO-20251769"
    assert level1_normalize_wo("WO-20251769_Rayhawk") == "SO-20251769"
    assert level1_normalize_wo("20251769") == "SO-20251769"
    assert level1_normalize_wo("ABC") == "ABC"


def test_level2_build_pick_flags() -> None:
    rows = pd.DataFrame(
        [
            {"order_id": "WO02-20251769", "status": "Picked"},
            {"order_id": "WO02-20251769", "status": "No"},
            {"order_id": "SO-20251771", "status": " no "},
            {"order_id": "SO-20251773", "status": "picked"},
            {"order_id": "bad-id", "status": "Picked"},
        ]
    )

    out = level2_build_pick_flags(rows)
    assert set(out.columns) == {"QB Num", "Picked_Flag"}

    got = {r["QB Num"]: bool(r["Picked_Flag"]) for _, r in out.iterrows()}
    assert got["SO-20251769"] is True
    assert got["SO-20251771"] is False
    assert got["SO-20251773"] is True
    assert got["bad-id"] is True


def test_level3_select_shipping_qty() -> None:
    df = pd.DataFrame(
        [
            {"Confirmed Qty": 10, "Qty": 7},
            {"Confirmed Qty": None, "Qty": 3},
            {"Confirmed Qty": "bad", "Qty": "2"},
            {"Qty": 5},
            {"Confirmed Qty": 0, "Qty": 8},
        ]
    )
    s = level3_select_shipping_qty(df)
    assert s.tolist() == [10.0, 0.0, 0.0, 0.0, 0.0]


def test_level4_reconcile_open_po_vs_ledger() -> None:
    po = pd.DataFrame(
        [
            {"Item": "SSD-2TB-TLC5WT-TD", "Qty(+)": 100, "Name": "Applied Intuition, Inc."},
            {"Item": "SSD-2TB-TLC5WT-TD", "Qty(+)": 3, "Name": "CoastIPC, Inc."},  # excluded
            {"Item": "DDR5-32GB-48WT-SM", "Qty(+)": 4, "Name": "RayHawk Technologies"},
            {"Item": "DDR5-32GB-48WT-SM", "Qty(+)": -1, "Name": "RayHawk Technologies"},
        ]
    )
    led = pd.DataFrame(
        [
            {"Item": "SSD-2TB-TLC5WT-TD", "Delta": 102},
            {"Item": "DDR5-32GB-48WT-SM", "Delta": 4},
            {"Item": "CBL-PC-OW3-180CM1", "Delta": 2},
            {"Item": "CBL-PC-OW3-180CM1", "Delta": -1},
        ]
    )

    out = level4_reconcile_open_po_vs_ledger(po, led)
    assert list(out.columns) == ["item_key", "open_po_qty", "ledger_in_qty", "gap_qty"]

    key_rows = {r["item_key"]: r for _, r in out.iterrows()}
    assert "SSD-2TB-TLC5WT-TD" in key_rows
    assert key_rows["SSD-2TB-TLC5WT-TD"]["open_po_qty"] == 100.0
    assert key_rows["SSD-2TB-TLC5WT-TD"]["ledger_in_qty"] == 102.0
    assert key_rows["SSD-2TB-TLC5WT-TD"]["gap_qty"] == 2.0
    assert "CBL-PC-OW3-180CM1" in key_rows  # exists in ledger only
    assert "DDR5-32GB-48WT-SM" not in key_rows  # balanced


def test_level5_topological_order() -> None:
    graph = {
        "structured": ["sales_order", "inventory", "word_files"],
        "ledger": ["structured", "shipping", "pod"],
        "atp": ["ledger"],
        "reports": ["structured", "ledger"],
    }
    order = level5_topological_order(graph)
    pos = {n: i for i, n in enumerate(order)}
    for node, deps in graph.items():
        for dep in deps:
            assert pos[dep] < pos[node], f"{dep} must come before {node}"

    with pytest.raises(ValueError):
        level5_topological_order({"A": ["B"], "B": ["C"], "C": ["A"]})
