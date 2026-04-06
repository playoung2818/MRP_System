from __future__ import annotations

import numpy as np
import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item

from .common import _norm_key
from .sales_order import normalize_wo_number


def build_wip_lookup(so_full: pd.DataFrame, word_files_df: pd.DataFrame) -> pd.DataFrame:
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
    if "On Hand - WIP" not in inv.columns:
        inv["On Hand - WIP"] = inv.get("On Hand", 0)
    inv["On Hand - WIP"] = pd.to_numeric(inv["On Hand - WIP"], errors="coerce")
    inv["On Hand - WIP"] = inv["On Hand - WIP"].fillna(inv["On Hand"] - inv["WIP_Qty"])
    return inv


def add_onhand_minus_wip(inv: pd.DataFrame, structured: pd.DataFrame) -> pd.DataFrame:
    out = inv.copy()
    if "On Hand" not in out.columns:
        out["On Hand"] = 0
    out["On Hand"] = pd.to_numeric(out["On Hand"], errors="coerce").fillna(0.0)

    out["__ITEM_KEY__"] = _norm_key(out.get("Item", pd.Series(pd.NA, index=out.index)))
    st = structured.copy()
    st["__ITEM_KEY__"] = _norm_key(st.get("Item", pd.Series(pd.NA, index=st.index)))

    if "Assigned Q'ty" in st.columns:
        st["Assigned Q'ty"] = pd.to_numeric(st["Assigned Q'ty"], errors="coerce").fillna(0.0)
        wip = (
            st.loc[st["Assigned Q'ty"].ne(0), ["__ITEM_KEY__", "Assigned Q'ty"]]
            .groupby("__ITEM_KEY__", as_index=False)["Assigned Q'ty"]
            .sum()
            .rename(columns={"Assigned Q'ty": "WIP_Qty"})
        )
    else:
        wip = pd.DataFrame({"__ITEM_KEY__": out["__ITEM_KEY__"].unique(), "WIP_Qty": 0.0})

    out = out.merge(wip, on="__ITEM_KEY__", how="left", suffixes=("", "_calc"))
    if "WIP_Qty" not in out.columns:
        out["WIP_Qty"] = 0
    out["WIP_Qty"] = pd.to_numeric(out["WIP_Qty"], errors="coerce")
    out["WIP_Qty"] = out["WIP_Qty"].fillna(out.get("WIP_Qty_calc", 0)).fillna(0.0)
    if "WIP_Qty_calc" in out.columns:
        out.drop(columns=["WIP_Qty_calc"], inplace=True)

    out["On Hand - WIP"] = out["On Hand"] - out["WIP_Qty"]
    out.drop(columns=["__ITEM_KEY__"], inplace=True)
    return out


__all__ = ["add_onhand_minus_wip", "build_wip_lookup", "transform_inventory"]
