from __future__ import annotations

import numpy as np
import pandas as pd

from erp_system.runtime.constants import PLACEHOLDER_DATE
from erp_system.transform.common import _norm_cols

from .events import _order_events, build_opening_stock


def build_ledger_from_events(
    so: pd.DataFrame,
    events: pd.DataFrame,
    inventory: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    so = _norm_cols(so)
    stock = build_opening_stock(so, inventory)

    events = events.copy()
    events = events.merge(stock, on="Item", how="left")
    events["Opening"] = events["Opening"].fillna(0.0)

    today = pd.Timestamp.today().normalize()
    open_df = pd.DataFrame(
        {
            "Date": today,
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

    mask = (
        (ledger["Projected_NAV"] < 0)
        & ledger["Date"].notna()
        & ledger["Date"].ne(PLACEHOLDER_DATE)
        & ledger["Kind"].eq("OUT")
        & ledger["Source"].eq("SO")
        & ~ledger["Item"].fillna("").str.startswith("Total ")
    )
    violations = ledger.loc[mask].sort_values(by="Date").copy()
    ledger.sort_values(["Item", "Date", "Kind"], inplace=True, kind="mergesort")
    return ledger, violations


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
    df = df.loc[df["Date"].ne(PLACEHOLDER_DATE)]
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
