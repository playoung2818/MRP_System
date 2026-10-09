from __future__ import annotations

import numpy as np
import pandas as pd

from mrp_system.normalize.mrp_normalize import normalize_item
from mrp_system.runtime.constants import PLACEHOLDER_DATE
from mrp_system.runtime.policies import EXCLUDED_PREINSTALLED_PO_VENDORS

from .sales_order import normalize_wo_number
from .item_order import reorder_so_items


def reorder_df_out_by_output(output_df: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    """Match reference order by canonical item, preserving repeated occurrences."""
    return reorder_so_items(df_out, reference=output_df)


def build_structured_df(
    df_sales_order: pd.DataFrame,
    mes_quantities: pd.DataFrame,
    inventory_df: pd.DataFrame,
    pdf_orders_df: pd.DataFrame,
    df_pod: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    terms_col = next((col for col in ("Terms", "Term", "term") if col in df_sales_order.columns), "Terms")
    needed_cols = {
        "Order Date": "SO Entry Date",
        "Name": "Customer",
        "P. O. #": "Customer PO",
        "QB Num": "QB Num",
        terms_col: "Terms",
        "Item": "Item",
        "Qty(-)": "Qty",
        "Ship Date": "Lead Time",
        "Inventory Site": "Inventory Site",
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
    pdf_ref = pdf_orders_df.rename(columns={"WO": "QB Num", "Product Number": "Item"})
    final_sales_order = reorder_df_out_by_output(pdf_ref, df_out)
    final_sales_order["Item"] = final_sales_order["Item"].map(normalize_item)
    final_sales_order = final_sales_order.loc[:, ~final_sales_order.columns.duplicated()]

    from .inventory import match_mes_quantities
    picked = match_mes_quantities(df_sales_order, mes_quantities)
    picked = picked[['QB Num', 'Item', 'Inventory Site', 'WIP_Qty', 'Qty(-)', 'partial']]
    final_sales_order['QB Num'] = final_sales_order['QB Num'].astype(str).map(normalize_wo_number)
    df_order_picked = final_sales_order.merge(picked, on=['QB Num', 'Item', 'Inventory Site'], how='left')
    qty = df_order_picked['WIP_Qty'].fillna(0)
    df_order_picked['Picked_Flag'] = qty.gt(0)
    df_order_picked['partial'] = df_order_picked['partial'].astype('boolean').fillna(False).astype(bool)
    partially_picked = qty.gt(0) & qty.lt(df_order_picked['Qty(-)'])
    df_order_picked['Picked'] = np.where(qty.le(0), 'No', np.where(partially_picked, 'Partial', 'Picked'))
    df_order_picked.drop(columns=['WIP_Qty', 'Qty(-)'], inplace=True)

    inv_plus = inventory_df.copy()
    for c in ["On Hand", "On Sales Order", "On PO", "Reorder Pt (Min)", "Sales/Week", "Available"]:
        if c in inv_plus.columns:
            inv_plus[c] = pd.to_numeric(inv_plus[c], errors="coerce").fillna(0)

    structured_df = df_order_picked.merge(inv_plus, how="left", left_on="Item", right_on="Part_Number")
    structured_df["Qty"] = pd.to_numeric(structured_df["Qty"], errors="coerce")
    structured_df = structured_df.dropna(subset=["Qty"])

    if "Inventory Site" in structured_df.columns:
        structured_df = structured_df[structured_df["Inventory Site"].astype(str).str.strip() == "WH01S-NTA"].copy()

    structured_df["Lead Time"] = pd.to_datetime(structured_df["Lead Time"], errors="coerce").dt.floor("D")
    mask_july4 = structured_df["Lead Time"].dt.month.eq(7) & structured_df["Lead Time"].dt.day.eq(4)
    mask_dec31 = structured_df["Lead Time"].dt.month.eq(12) & structured_df["Lead Time"].dt.day.eq(31)
    structured_df.loc[mask_july4 | mask_dec31, "Lead Time"] = PLACEHOLDER_DATE

    not_dummy = structured_df["Lead Time"] != PLACEHOLDER_DATE
    structured_df["Assigned Q'ty"] = structured_df["Qty"].where(not_dummy, 0).groupby(structured_df["Item"]).transform("sum")

    # WIP (picked, not yet shipped) is computed once in build_wip_lookup(), already scoped to
    # WH01S-NTA, and merged in above via inv_plus as "WIP_Qty" — just look it up here rather
    # than recomputing it from df_order_picked a second time with its own site filter.
    structured_df["Picked_Qty"] = pd.to_numeric(structured_df.get("WIP_Qty", 0), errors="coerce").fillna(0)
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


def prepare_mrp_view(structured: pd.DataFrame) -> pd.DataFrame:
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
    mrp_df = df[cols].copy()
    mrp_df["Ship Date"] = pd.to_datetime(mrp_df["Ship Date"], errors="coerce")
    mask = (
        (mrp_df["Ship Date"].dt.month.eq(7) & mrp_df["Ship Date"].dt.day.eq(4))
        | (mrp_df["Ship Date"].dt.month.eq(12) & mrp_df["Ship Date"].dt.day.eq(31))
    )
    mrp_df["AssignedFlag"] = ~mask
    mrp_df["Ship Date"] = mrp_df["Ship Date"].dt.strftime("%m/%d/%Y")
    return mrp_df


__all__ = ["build_structured_df", "prepare_mrp_view", "reorder_df_out_by_output"]
