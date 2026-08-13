from __future__ import annotations

import re

import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item


def normalize_wo_number(wo: str) -> str:
    match = re.search(r"\b(20\d{6})\b", str(wo))
    return f"SO-{match.group(1)}" if match else str(wo)


def transform_sales_order(df_sales_order: pd.DataFrame) -> pd.DataFrame:
    df = df_sales_order.copy()
    df["partial"] = df["Qty"] != df["Backordered"]
    df = df.drop(columns=["Qty", "Item"], errors="ignore")
    df = df.rename(
        columns={"Unnamed: 0": "Item", "Num": "QB Num", "Backordered": "Qty(-)", "Date": "Order Date"}
    )
    df["Item"] = df["Item"].ffill().astype(str).str.strip()
    df = df[~df["Item"].str.startswith("total", na=False)]
    df = df[~df["Item"].str.lower().isin(["forwarding charge", "tariff (estimation)"])]
    if "Inventory Site" in df.columns:
        df["Inventory Site"] = df["Inventory Site"].astype(str).str.strip()
    # Non-WH01S-NTA rows are kept here (not dropped) so the Google Sheet export can show
    # every site; build_structured_df() re-applies the WH01S-NTA scope for ledger/ATP/
    # assignment calculations so those numbers are unaffected.
    df["Item"] = df["Item"].map(normalize_item)
    return df


__all__ = ["normalize_wo_number", "transform_sales_order"]
