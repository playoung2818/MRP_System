from __future__ import annotations

import re

import pandas as pd

from mrp_system.normalize.mrp_normalize import normalize_item


def normalize_wo_number(wo: str) -> str:
    match = re.search(r"\b(20\d{6})\b", str(wo))
    return f"SO-{match.group(1)}" if match else str(wo)


def transform_sales_order(df_sales_order: pd.DataFrame) -> pd.DataFrame:
    df = df_sales_order.copy()
    # Remove subtotal rows before forward-filling item headers or parsing qty.
    labels = df['Unnamed: 0'].fillna('').astype(str).str.strip()
    df = df[~labels.str.match(r'(?i)^total(?:\s|$)')].copy()
    df['Unnamed: 0'] = df['Unnamed: 0'].ffill()
    numbers = df['Num'].fillna('').astype(str).str.strip()
    df = df[~numbers.str.casefold().isin(['', 'nan', 'none', 'null'])].copy()
    df['Num'] = numbers.loc[df.index]
    df["partial"] = df["Qty"] != df["Backordered"]
    df['Shipped Qty'] = (pd.to_numeric(df['Qty'], errors='raise') - pd.to_numeric(df['Backordered'], errors='raise')).clip(lower=0)
    df = df.drop(columns=["Qty", "Item"], errors="ignore")
    df = df.rename(
        columns={"Unnamed: 0": "Item", "Num": "QB Num", "Backordered": "Qty(-)", "Date": "Order Date"}
    )
    df["Item"] = df["Item"].ffill().astype(str).str.strip()
    df = df[~df['Item'].str.casefold().isin(['', 'nan', 'none', 'null'])]
    df = df[~df["Item"].str.lower().isin(["forwarding charge", "tariff (estimation)"])]
    if "Inventory Site" in df.columns:
        df["Inventory Site"] = df["Inventory Site"].astype(str).str.strip()
    # Non-WH01S-NTA rows are kept here (not dropped) so the Google Sheet export can show
    # every site; build_structured_df() re-applies the WH01S-NTA scope for ledger/ATP/
    # assignment calculations so those numbers are unaffected.
    df["Item"] = df["Item"].map(normalize_item)
    return df


__all__ = ["normalize_wo_number", "transform_sales_order"]
