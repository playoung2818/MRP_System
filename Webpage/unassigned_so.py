"""Read-only browser report for sales orders without assigned lead time."""
import pandas as pd

COLUMNS = [
    "Order Date", "Name", "QB Num", "Item", "Qty(-)", "Available",
    "Available + On PO", "Sales/Week", "Recommended Restock Qty",
    "Assigned Q'ty", "On Hand", "On Sales Order", "On PO", "Component_Status",
]
NUMERIC_COLUMNS = COLUMNS[4:13]


def unassigned_orders(source: pd.DataFrame | None) -> pd.DataFrame:
    if source is None or not {"Ship Date", "QB Num"}.issubset(source.columns):
        return pd.DataFrame(columns=COLUMNS)
    dates = pd.to_datetime(source["Ship Date"], errors="coerce")
    keys = source["QB Num"].fillna("").astype(str).str.strip()
    mask = ((dates.dt.month.eq(7) & dates.dt.day.eq(4))
            | (dates.dt.month.eq(12) & dates.dt.day.eq(31)))
    result = source.loc[mask & keys.ne("")].copy()
    result["QB Num"] = keys.loc[result.index]
    return result


def report_rows(source: pd.DataFrame | None, query: str = "") -> tuple[int, list[dict]]:
    orders = unassigned_orders(source).reindex(columns=COLUMNS)
    if query:
        matches = pd.Series(False, index=orders.index)
        for col in ("QB Num", "Name", "Item"):
            matches |= orders[col].fillna("").astype(str).str.contains(query, case=False, regex=False)
        orders = orders.loc[matches]
    count = int(orders["QB Num"].nunique())
    # Group SOs together while retaining the original component order.
    orders = orders.sort_values("QB Num", kind="stable")
    numeric = orders[NUMERIC_COLUMNS].apply(pd.to_numeric, errors="coerce")
    highlights = numeric["Sales/Week"].fillna(0).gt(numeric["Available + On PO"].fillna(0))
    orders["Order Date"] = pd.to_datetime(orders["Order Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    rows = []
    last_key = None
    band = False
    for position, (_, row) in enumerate(orders.iterrows()):
        if row["QB Num"] != last_key:
            band = not band
            last_key = row["QB Num"]
        cells = {}
        for col in COLUMNS:
            value = row[col]
            if pd.isna(value):
                cells[col] = ""
            elif col in NUMERIC_COLUMNS:
                number = numeric.iloc[position][col]
                cells[col] = "" if pd.isna(number) else f"{number:,.2f}".rstrip("0").rstrip(".")
            else:
                cells[col] = str(value)
        rows.append({"cells": cells, "band": band,
                     "shortage": cells["Component_Status"] == "Shortage",
                     "low_cover": bool(highlights.iloc[position])})
    return count, rows
