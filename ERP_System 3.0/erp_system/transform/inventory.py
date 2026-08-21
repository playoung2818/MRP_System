from __future__ import annotations

import numpy as np
import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item

from .sales_order import normalize_wo_number


WIP_SITE = "WH01S-NTA"


def build_wip_lookup(so_full: pd.DataFrame, word_files_df: pd.DataFrame, site: str = WIP_SITE) -> pd.DataFrame:
    """Single source of truth for WIP: qty that's been picked (word file created) but is
    still on an open sales order (i.e. not yet shipped), scoped to `site`. Computed once
    here, before inventory_status is built; everything downstream (inventory_status,
    wo_structured) just looks the number up by Part_Number instead of recomputing it.
    """
    word_pick = word_files_df.copy()
    word_pick["WO_Number"] = word_pick["WO_Number"].astype(str).apply(normalize_wo_number)
    word_pick["Picked_Flag"] = word_pick["status"].astype(str).str.strip().eq("Picked")
    picked_flags = word_pick.groupby("WO_Number", as_index=False)["Picked_Flag"].max()

    sales = so_full.copy()
    sales["WO_Number"] = sales["QB Num"].astype(str).apply(normalize_wo_number)
    sales["QB Num"] = sales["WO_Number"]
    sales = sales.merge(picked_flags, on="WO_Number", how="left")
    sales["Picked_Flag"] = sales["Picked_Flag"].astype("boolean").fillna(False)
    sales["Picked"] = np.where(sales["Picked_Flag"], "Picked", "No")
    partial_col = sales["partial"] if "partial" in sales.columns else False
    partial_col = pd.Series(partial_col, index=sales.index).fillna(False)
    mask_partial = sales["Picked_Flag"] & partial_col
    sales.loc[mask_partial, "Picked"] = "Partial"

    if "Inventory Site" in sales.columns:
        sales = sales[sales["Inventory Site"].astype(str).str.strip() == site]

    picked_lines = sales.loc[sales["Picked"].eq("Picked"), ["Item", "QB Num", "Qty(-)"]].copy()
    if picked_lines.empty:
        return pd.DataFrame(columns=["Part_Number", "WIP", "WIP_Qty"])

    wip_qty = (
        picked_lines.groupby("Item", as_index=False)["Qty(-)"].sum().rename(
            columns={"Item": "Part_Number", "Qty(-)": "WIP_Qty"}
        )
    )
    wip_list = (
        picked_lines.groupby("Item")["QB Num"]
        .apply(lambda s: ", ".join(pd.unique(s.dropna().astype(str))))
        .reset_index()
        .rename(columns={"Item": "Part_Number", "QB Num": "WIP"})
    )

    wip = wip_qty.merge(wip_list, on="Part_Number", how="outer")
    wip["Part_Number"] = wip["Part_Number"].astype(str).str.strip().map(normalize_item)
    wip["WIP_Qty"] = pd.to_numeric(wip["WIP_Qty"], errors="coerce").fillna(0)
    wip["WIP"] = wip["WIP"].fillna("")
    return wip


def transform_inventory(inventory_df: pd.DataFrame, wip_lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    inv = inventory_df.copy()
    inv = inv.rename(columns={"Unnamed: 0": "Part_Number"})
    inv["Part_Number"] = inv["Part_Number"].astype(str).str.strip().map(normalize_item)
    for c in ["On Hand", "On Sales Order", "On PO", "Available", "On Hand - WIP", "WIP_Qty"]:
        if c in inv.columns:
            inv[c] = pd.to_numeric(inv[c], errors="coerce").fillna(0)

    if wip_lookup is not None and not wip_lookup.empty:
        wip = wip_lookup.copy()
        if "Part_Number" not in wip.columns and "Item" in wip.columns:
            wip["Part_Number"] = wip["Item"]
        if "Part_Number" in wip.columns:
            wip["Part_Number"] = wip["Part_Number"].astype(str).str.strip().map(normalize_item)
            keep_cols = [c for c in ["Part_Number", "WIP", "WIP_Qty", "On Hand - WIP"] if c in wip.columns]
            wip = wip.loc[:, keep_cols].drop_duplicates(subset=["Part_Number"])
            inv = inv.merge(wip, on="Part_Number", how="left", suffixes=("", "_src"))
            if "WIP_src" in inv.columns:
                if "WIP" not in inv.columns:
                    inv["WIP"] = pd.NA
                inv["WIP"] = inv["WIP"].combine_first(inv["WIP_src"])
                inv.drop(columns=["WIP_src"], inplace=True)
            if "WIP_Qty_src" in inv.columns:
                if "WIP_Qty" not in inv.columns:
                    inv["WIP_Qty"] = pd.NA
                inv["WIP_Qty"] = inv["WIP_Qty"].combine_first(inv["WIP_Qty_src"])
                inv.drop(columns=["WIP_Qty_src"], inplace=True)
            if "On Hand - WIP_src" in inv.columns:
                inv["On Hand - WIP"] = inv["On Hand - WIP_src"].combine_first(inv.get("On Hand - WIP"))
                inv.drop(columns=["On Hand - WIP_src"], inplace=True)

    if "WIP" not in inv.columns:
        inv["WIP"] = ""
    inv["WIP"] = inv["WIP"].fillna("")
    if "WIP_Qty" not in inv.columns:
        inv["WIP_Qty"] = 0
    inv["WIP_Qty"] = pd.to_numeric(inv["WIP_Qty"], errors="coerce").fillna(0)
    # Always derive this from On Hand - WIP_Qty. The old code defaulted a missing
    # "On Hand - WIP" straight to On Hand, which made it a real (non-NaN) value before
    # the fillna(On Hand - WIP_Qty) below ever ran — so that subtraction was dead code
    # and WIP_Qty was silently ignored here.
    inv["On Hand - WIP"] = inv["On Hand"] - inv["WIP_Qty"]
    return inv


__all__ = ["build_wip_lookup", "transform_inventory", "WIP_SITE"]
