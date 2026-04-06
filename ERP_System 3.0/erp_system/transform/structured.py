from __future__ import annotations

import numpy as np
import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item
from erp_system.runtime.constants import FAR_FUTURE_DATE, UNASSIGNED_LT_DATE
from erp_system.runtime.policies import EXCLUDED_PREINSTALLED_PO_VENDORS

from .sales_order import normalize_wo_number


def reorder_df_out_by_output(output_df: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    ref = output_df.copy()
    ref["__pos_out"] = ref.groupby("QB Num").cumcount()
    ref["__occ"] = ref.groupby(["QB Num", "Item"]).cumcount()
    ref_key = ref[["QB Num", "Item", "__occ", "__pos_out"]]

    tgt = df_out.copy()
    tgt["__occ"] = tgt.groupby(["QB Num", "Item"]).cumcount()

    merged = tgt.merge(ref_key, on=["QB Num", "Item", "__occ"], how="left")
    merged["__fallback"] = merged.groupby("QB Num").cumcount()
    merged["__pos_out"] = merged["__pos_out"].fillna(np.inf)
    return (
        merged.sort_values(["QB Num", "__pos_out", "__fallback"])
        .drop(columns=["__occ", "__pos_out", "__fallback"])
        .reset_index(drop=True)
    )


def build_structured_df(
    df_sales_order: pd.DataFrame,
    word_files_df: pd.DataFrame,
    inventory_df: pd.DataFrame,
    pdf_orders_df: pd.DataFrame,
    df_pod: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    needed_cols = {
        "Order Date": "SO Entry Date",
        "Name": "Customer",
        "P. O. #": "Customer PO",
        "QB Num": "QB Num",
        "Item": "Item",
        "Qty(-)": "Qty",
        "Ship Date": "Lead Time",
    }
    for src in list(needed_cols.keys()):
        if src not in df_sales_order.columns:
            df_sales_order[src] = "" if src not in ("Qty(-)",) else 0

    df_out = df_sales_order.rename(columns=needed_cols)[list(needed_cols.values())].copy()
    df_out["WO"] = ""
    for alt in ["WO", "WO_Number", "NTA Order ID", "SO Number"]:
        if alt in df_sales_order.columns:
            df_out["WO"] = df_sales_order[alt].astype(str).apply(normalize_wo_number)
            break
    df_out = df_out.sort_values(["QB Num", "Item"]).reset_index(drop=True)

    pdf_ref = pdf_orders_df.rename(columns={"WO": "QB Num", "Product Number": "Item"})
    final_sales_order = reorder_df_out_by_output(pdf_ref, df_out)
    final_sales_order["Item"] = final_sales_order["Item"].map(normalize_item)
    final_sales_order = final_sales_order.loc[:, ~final_sales_order.columns.duplicated()]

    word_pick = word_files_df.copy()
    word_pick["WO_Number"] = word_pick["WO_Number"].astype(str).apply(normalize_wo_number)
    word_pick["Picked_Flag"] = word_pick["status"].astype(str).str.strip().eq("Picked")
    word_pick = word_pick.groupby("WO_Number", as_index=False)["Picked_Flag"].max()

    df_order_picked = (
        final_sales_order.merge(word_pick, left_on="QB Num", right_on="WO_Number", how="left").drop(columns=["WO_Number"])
    )
    df_order_picked["Picked_Flag"] = df_order_picked["Picked_Flag"].astype("boolean").fillna(False)

    partial_map = (
        df_sales_order.groupby(["QB Num", "Item"], as_index=False)["partial"]
        .any()
        .rename(columns={"partial": "partial_flag"})
    )
    df_order_picked = df_order_picked.merge(partial_map, on=["QB Num", "Item"], how="left")
    df_order_picked["partial"] = df_order_picked["partial_flag"].fillna(False).astype(bool)
    df_order_picked.drop(columns=["partial_flag"], inplace=True)

    df_order_picked["Picked"] = np.where(df_order_picked["Picked_Flag"], "Picked", "No")
    mask_partial = df_order_picked["Picked_Flag"] & df_order_picked["partial"]
    df_order_picked.loc[mask_partial, "Picked"] = "Partial"

    picked_parts = (
        df_order_picked.loc[df_order_picked["Picked"].eq("Picked")]
        .groupby("Item", as_index=False)["Qty"]
        .sum()
        .rename(columns={"Item": "Part_Number", "Qty": "Picked_Qty"})
    )

    inv_plus = inventory_df.merge(picked_parts, on="Part_Number", how="left")
    for c in ["On Hand", "On Sales Order", "On PO", "Picked_Qty", "Reorder Pt (Min)", "Sales/Week", "Available"]:
        if c in inv_plus.columns:
            inv_plus[c] = pd.to_numeric(inv_plus[c], errors="coerce").fillna(0)

    structured_df = df_order_picked.merge(inv_plus, how="left", left_on="Item", right_on="Part_Number")
    structured_df["Qty"] = pd.to_numeric(structured_df["Qty"], errors="coerce")
    structured_df = structured_df.dropna(subset=["Qty"])

    structured_df["Lead Time"] = pd.to_datetime(structured_df["Lead Time"], errors="coerce").dt.floor("D")
    mask_july4 = structured_df["Lead Time"].dt.month.eq(7) & structured_df["Lead Time"].dt.day.eq(4)
    mask_dec31 = structured_df["Lead Time"].dt.month.eq(12) & structured_df["Lead Time"].dt.day.eq(31)
    structured_df.loc[mask_july4, "Lead Time"] = UNASSIGNED_LT_DATE
    structured_df.loc[mask_dec31, "Lead Time"] = FAR_FUTURE_DATE

    not_dummy = ~((structured_df["Lead Time"] == UNASSIGNED_LT_DATE) | (structured_df["Lead Time"] == FAR_FUTURE_DATE))
    structured_df["Assigned Q'ty"] = structured_df["Qty"].where(not_dummy, 0).groupby(structured_df["Item"]).transform("sum")

    structured_df["Picked_Qty"] = pd.to_numeric(structured_df.get("Picked_Qty", 0), errors="coerce").fillna(0)
    structured_df["On Hand"] = pd.to_numeric(structured_df.get("On Hand", 0), errors="coerce").fillna(0)
    structured_df["On Hand - WIP"] = (structured_df["On Hand"] - structured_df["Picked_Qty"]).clip(lower=0)

    filtered = df_pod[~df_pod["Name"].isin(EXCLUDED_PREINSTALLED_PO_VENDORS)]
    result = filtered.groupby("Item", as_index=False)["Qty(+)"].sum()
    lookup = result[["Item", "Qty(+)"]].drop_duplicates(subset=["Item"]).set_index("Item")["Qty(+)"]
    structured_df["Pre-installed PO"] = structured_df["Item"].map(lookup).fillna(0)

    structured_df["Available"] = pd.to_numeric(structured_df.get("Available", 0), errors="coerce").fillna(0)
    structured_df["On PO"] = pd.to_numeric(structured_df.get("On PO", 0), errors="coerce").fillna(0)
    structured_df["Reorder Pt (Min)"] = pd.to_numeric(structured_df.get("Reorder Pt (Min)", 0), errors="coerce").fillna(0)
    structured_df["Sales/Week"] = pd.to_numeric(structured_df.get("Sales/Week", 0), errors="coerce").fillna(0)

    structured_df["Available + Pre-installed PO"] = structured_df["Available"] + structured_df["Pre-installed PO"]
    structured_df["Available + On PO"] = structured_df["Available"] + structured_df["On PO"]
    structured_df["Recommended Restock Qty"] = np.ceil(
        np.maximum(0, (4 * structured_df["Sales/Week"]) - structured_df["Available"] - structured_df["On PO"])
    ).astype(int)

    structured_df["Component_Status"] = np.select(
        [
            (structured_df["Available"] >= 0) & (structured_df["On Hand"] > 0),
            (structured_df["Available"] + structured_df["On PO"] >= 0),
        ],
        ["Available", "Waiting"],
        default="Shortage",
    )

    structured_df["Qty(+)"] = "0"
    structured_df["Pre/Bare"] = "Out"
    structured_df.rename(
        columns={
            "SO Entry Date": "Order Date",
            "Customer": "Name",
            "Lead Time": "Ship Date",
            "Customer PO": "P. O. #",
            "Qty": "Qty(-)",
            "SO Status": "SO_Status",
        },
        inplace=True,
    )

    for col in ["Order Date", "Ship Date"]:
        if col in structured_df.columns:
            structured_df[col] = pd.to_datetime(structured_df[col], errors="coerce").dt.strftime("%m/%d/%Y")

    return structured_df, final_sales_order


def prepare_erp_view(structured: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "Order Date",
        "Name",
        "QB Num",
        "Item",
        "Qty(-)",
        "Available",
        "Available + On PO",
        "Sales/Week",
        "Recommended Restock Qty",
        "Available + Pre-installed PO",
        "On Hand - WIP",
        "Assigned Q'ty",
        "On Hand",
        "On Sales Order",
        "On PO",
        "Component_Status",
        "P. O. #",
        "Ship Date",
    ]
    df = structured.copy()
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    erp_df = df[cols].copy()
    erp_df["Ship Date"] = pd.to_datetime(erp_df["Ship Date"], errors="coerce")
    mask = (
        (erp_df["Ship Date"].dt.month.eq(7) & erp_df["Ship Date"].dt.day.eq(4))
        | (erp_df["Ship Date"].dt.month.eq(12) & erp_df["Ship Date"].dt.day.eq(31))
    )
    erp_df["AssignedFlag"] = ~mask
    erp_df["Ship Date"] = erp_df["Ship Date"].dt.strftime("%m/%d/%Y")
    return erp_df


__all__ = ["build_structured_df", "prepare_erp_view", "reorder_df_out_by_output"]
