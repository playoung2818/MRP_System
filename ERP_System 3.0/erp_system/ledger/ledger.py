from __future__ import annotations

import numpy as np
import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item
from erp_system.runtime.constants import DUMMY_SHIP_DATES, FAR_FUTURE_DATE, SHORTAGE_REPORT_CUTOFF
from erp_system.transform.common import _norm_cols, _norm_key

from .events import _order_events, build_opening_stock


def build_ledger_from_events(
    so: pd.DataFrame,
    events: pd.DataFrame,
    inventory: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    so = _norm_cols(so)
    stock = build_opening_stock(so, inventory)

    events = events.copy()
    events = events.merge(stock, on="Item", how="left")
    events["Opening"] = events["Opening"].fillna(0.0)

    today = pd.Timestamp.today().normalize()
    open_df = pd.DataFrame(
        {
            "Date": [today] * len(stock),
            "Item": stock["Item"].values,
            "Delta": 0.0,
            "Kind": "OPEN",
            "Source": "Snapshot",
            "Item_raw": stock["Item"].values,
            "Opening": stock["Opening"].values,
        }
    )

    ledger = pd.concat([open_df, events], ignore_index=True, sort=False)
    ledger = _order_events(ledger)
    ledger["CumDelta"] = ledger.groupby("Item", sort=False)["Delta"].cumsum()
    ledger["Projected_NAV"] = ledger["Opening"] + ledger["CumDelta"]

    is_out = ledger["Kind"].eq("OUT")
    ledger["NAV_before"] = np.where(is_out, ledger["Projected_NAV"] - ledger["Delta"], np.nan)
    ledger["NAV_after"] = np.where(is_out, ledger["Projected_NAV"], np.nan)

    item_min = ledger.groupby("Item", as_index=False)["Projected_NAV"].min().rename(columns={"Projected_NAV": "Min_Projected_NAV"})
    first_neg = (
        ledger.loc[ledger["Projected_NAV"] < 0]
        .sort_values(["Item", "Date"])
        .groupby("Item", as_index=False)
        .first()[["Item", "Date", "Projected_NAV"]]
        .rename(columns={"Date": "First_Shortage_Date", "Projected_NAV": "NAV_at_First_Shortage"})
    )

    so_for_users = so.copy()
    for col in ["Name", "QB Num", "Qty(-)"]:
        if col not in so_for_users.columns:
            so_for_users[col] = pd.NA
    so_for_users["Item"] = _norm_key(so_for_users["Item"])
    so_for_users["Name"] = so_for_users["Name"].fillna("").astype(str).str.strip()
    so_for_users["QB Num"] = so_for_users["QB Num"].fillna("").astype(str).str.strip()
    so_for_users["Qty(-)"] = pd.to_numeric(so_for_users["Qty(-)"], errors="coerce").fillna(0.0)

    item_users = so_for_users.loc[so_for_users["Qty(-)"] > 0, ["Item", "Name", "QB Num"]].copy()
    item_users = item_users.loc[item_users["Name"].ne("") | item_users["QB Num"].ne("")]
    item_users["Customer_QB"] = np.where(
        item_users["Name"].ne("") & item_users["QB Num"].ne(""),
        item_users["Name"] + " (" + item_users["QB Num"] + ")",
        np.where(item_users["Name"].ne(""), item_users["Name"], item_users["QB Num"]),
    )
    if item_users.empty:
        item_users = pd.DataFrame(columns=["Item", "Customer_QB_List"])
    else:
        item_users = (
            item_users.sort_values(["Item", "QB Num", "Name"])
            .drop_duplicates(subset=["Item", "Customer_QB"])
            .groupby("Item", as_index=False)["Customer_QB"]
            .agg(", ".join)
            .rename(columns={"Customer_QB": "Customer_QB_List"})
        )

    inv_cols = pd.DataFrame(columns=["Item", "On Sales Order", "On PO"])
    if inventory is not None and not inventory.empty:
        inv = inventory.copy()
        item_col = "Part_Number" if "Part_Number" in inv.columns else ("Item" if "Item" in inv.columns else None)
        if item_col is not None:
            inv["Item"] = _norm_key(inv[item_col])
            for c in ["On Sales Order", "On PO"]:
                if c not in inv.columns:
                    inv[c] = 0.0
                inv[c] = pd.to_numeric(inv[c], errors="coerce").fillna(0.0)
            inv_cols = inv[["Item", "On Sales Order", "On PO"]].groupby("Item", as_index=False)[["On Sales Order", "On PO"]].sum()

    item_summary = stock.merge(item_min, on="Item", how="outer").merge(first_neg, on="Item", how="left").merge(item_users, on="Item", how="left").merge(inv_cols, on="Item", how="left")
    item_summary["On Sales Order"] = pd.to_numeric(item_summary["On Sales Order"], errors="coerce").fillna(0.0)
    item_summary["On PO"] = pd.to_numeric(item_summary["On PO"], errors="coerce").fillna(0.0)
    item_summary["OK"] = item_summary["Min_Projected_NAV"].fillna(0) >= 0

    mask = (
        (ledger["Projected_NAV"] < 0)
        & ledger["Date"].notna()
        & (ledger["Date"] != FAR_FUTURE_DATE)
        & ledger["Kind"].eq("OUT")
        & ledger["Source"].eq("SO")
        & ~ledger["Item"].fillna("").str.startswith("Total ")
        & (ledger["Date"] < SHORTAGE_REPORT_CUTOFF)
    )
    violations = ledger.loc[mask].sort_values(by="Date").copy()
    ledger.sort_values(["Item", "Date", "Kind"], inplace=True, kind="mergesort")
    item_summary.sort_values(["OK", "Min_Projected_NAV"], ascending=[True, True], inplace=True)
    return ledger, item_summary, violations


def earliest_atp_by_projected_nav(
    ledger: pd.DataFrame,
    item: str,
    qty: float,
    from_date: pd.Timestamp | None = None,
) -> pd.Timestamp | None:
    if ledger is None or ledger.empty:
        return None
    from_date = pd.Timestamp.today().normalize() if from_date is None else pd.to_datetime(from_date).normalize()
    qty_val = pd.to_numeric(qty, errors="coerce")
    if pd.isna(qty_val):
        return None
    qty_val = int(qty_val)
    if not {"Item", "Date", "Projected_NAV"}.issubset(ledger.columns):
        return None
    df = ledger.loc[ledger["Item"].astype(str) == str(item)].copy()
    if df.empty:
        return None
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.loc[df["Date"].notna()]
    if df.empty:
        return None
    df = df.loc[~df["Date"].isin(DUMMY_SHIP_DATES)]
    if df.empty:
        return None
    df["Projected_NAV"] = pd.to_numeric(df["Projected_NAV"], errors="coerce")
    df = df.loc[df["Projected_NAV"].notna()]
    if df.empty:
        return None
    df = df.loc[df["Date"] >= from_date].sort_values("Date")
    if df.empty:
        return None
    candidates = df.loc[df["Projected_NAV"] >= qty_val, "Date"]
    return None if candidates.empty else candidates.min()


__all__ = ["build_ledger_from_events", "earliest_atp_by_projected_nav"]
