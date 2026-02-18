from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ledger import expand_nav_preinstalled, build_events, build_ledger_from_events  # noqa: E402


FIX = Path(__file__).resolve().parent / "fixtures"


def _load_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(FIX / name)


def _planned_orders_snapshot(item_summary: pd.DataFrame) -> pd.DataFrame:
    df = item_summary.copy()
    df["Min_Projected_NAV"] = pd.to_numeric(df["Min_Projected_NAV"], errors="coerce").fillna(0.0)
    df["Planned_Order_Qty"] = np.ceil(np.maximum(0.0, -df["Min_Projected_NAV"])).astype(int)
    df = df.loc[df["Planned_Order_Qty"] > 0, [
        "Item",
        "Planned_Order_Qty",
        "Min_Projected_NAV",
        "First_Shortage_Date",
        "NAV_at_First_Shortage",
        "Customer_QB_List",
        "On Sales Order",
        "On PO",
    ]].copy()
    df["First_Shortage_Date"] = pd.to_datetime(df["First_Shortage_Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ["Min_Projected_NAV", "NAV_at_First_Shortage", "On Sales Order", "On PO"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.sort_values("Item").reset_index(drop=True)
    return df


def _shortages_snapshot(violations: pd.DataFrame) -> pd.DataFrame:
    cols = ["Date", "Item", "QB Num", "Name", "Projected_NAV"]
    df = violations.loc[:, cols].copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["Projected_NAV"] = pd.to_numeric(df["Projected_NAV"], errors="coerce")
    df = df.sort_values(["Date", "Item", "QB Num"]).reset_index(drop=True)
    return df


def test_ledger_e2e_planned_orders_and_shortages_snapshot() -> None:
    # Fixture catalog for realism/documentation (2-level BOM with shared component contention).
    _ = _load_csv("items.csv")
    _ = _load_csv("bom_2level.csv")

    inventory = _load_csv("inventory.csv")
    so = _load_csv("open_so.csv")
    pod = _load_csv("open_po.csv")
    ship = _load_csv("shipping_schedule.csv")

    nav_exp = expand_nav_preinstalled(ship)
    events = build_events(so, nav_exp, pod)
    _, item_summary, violations = build_ledger_from_events(so, events, inventory)

    actual_planned = _planned_orders_snapshot(item_summary)
    actual_shortages = _shortages_snapshot(violations)

    expected_planned = _load_csv("expected_planned_orders_snapshot.csv")
    expected_shortages = _load_csv("expected_shortages_snapshot.csv")

    expected_planned["Planned_Order_Qty"] = pd.to_numeric(expected_planned["Planned_Order_Qty"], errors="coerce").astype(int)
    for c in ["Min_Projected_NAV", "NAV_at_First_Shortage", "On Sales Order", "On PO"]:
        expected_planned[c] = pd.to_numeric(expected_planned[c], errors="coerce")
    expected_planned = expected_planned.sort_values("Item").reset_index(drop=True)

    expected_shortages["Projected_NAV"] = pd.to_numeric(expected_shortages["Projected_NAV"], errors="coerce")
    expected_shortages = expected_shortages.sort_values(["Date", "Item", "QB Num"]).reset_index(drop=True)

    assert_frame_equal(actual_planned, expected_planned, check_like=False, check_dtype=False)
    assert_frame_equal(actual_shortages, expected_shortages, check_like=False, check_dtype=False)
