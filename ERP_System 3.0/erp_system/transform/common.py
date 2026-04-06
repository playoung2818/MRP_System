from __future__ import annotations

import pandas as pd


def enforce_column_order(df: pd.DataFrame, order: list[str]) -> pd.DataFrame:
    front = [c for c in order if c in df.columns]
    back = [c for c in df.columns if c not in front]
    return df.loc[:, front + back]


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ("Ship Date", "Order Date", "Arrive Date", "Date"):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    if "Item" in df.columns:
        df["Item"] = df["Item"].astype(str).str.strip()
    for c in ("Qty(+)", "Qty(-)", "On Hand", "On Hand - WIP", "Available", "On Sales Order", "On PO"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


def _norm_key(s: pd.Series) -> pd.Series:
    s = s.astype("string")
    return s.str.strip().str.upper()


__all__ = ["_norm_cols", "_norm_key", "enforce_column_order"]
