from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ledger import expand_nav_preinstalled  # noqa: E402


def _expand_item(item: str) -> pd.DataFrame:
    ship = pd.DataFrame(
        [
            {
                "QB Num": "POD-TEST",
                "Item": item,
                "Description": item,
                "Ship Date": pd.Timestamp("2026-04-01"),
                "Qty(+)": 2,
                "Pre/Bare": "Bare",
            }
        ]
    )
    return expand_nav_preinstalled(ship)


def test_shipping_nuvo_716_variants_split_into_base_and_cassette() -> None:
    cases = {
        "Nuvo-7160GC-PoE": {"Nuvo-716xGC-PoE", "CSM-7160GC"},
        "Nuvo-7162GC-PoE": {"Nuvo-716xGC-PoE", "CSM-7162GC"},
        "Nuvo-7166GC-PoE": {"Nuvo-716xGC-PoE", "CSM-7166GC"},
        "Nuvo-7160GC": {"Nuvo-716xGC", "CSM-7160GC"},
        "Nuvo-7162GC": {"Nuvo-716xGC", "CSM-7162GC"},
        "Nuvo-7166GC": {"Nuvo-716xGC", "CSM-7166GC"},
    }

    for src_item, expected_items in cases.items():
        expanded = _expand_item(src_item)
        actual_items = set(expanded["Item"].tolist())
        assert actual_items == expected_items, f"{src_item} expanded to {actual_items}, expected {expected_items}"
        qty_by_item = expanded.groupby("Item", as_index=False)["Qty(+)"].sum()
        assert qty_by_item["Qty(+)"].eq(2).all(), f"{src_item} expansion did not preserve qty"
