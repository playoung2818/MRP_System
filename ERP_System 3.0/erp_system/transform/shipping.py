from __future__ import annotations

import re

import numpy as np
import pandas as pd

from erp_system.runtime.constants import UNASSIGNED_LT_DATE
from erp_system.runtime.policies import (
    PREINSTALL_EXCLUDED_PREFIXES,
    PREINSTALL_MODEL_EXCLUSIONS,
    PREINSTALL_MODEL_PREFIXES,
)


def transform_shipping(df_shipping_schedule: pd.DataFrame) -> pd.DataFrame:
    def _norm_shipto(val: str) -> str:
        return re.sub(r"[^A-Za-z0-9]", "", str(val)).upper()

    target_shipto = {
        _norm_shipto("Neousys Technology America, Inc."),
        _norm_shipto("Neousys Technology America Inc."),
    }

    df = df_shipping_schedule.copy()
    if "Ship to" in df.columns:
        df["__shipto_key"] = df["Ship to"].apply(_norm_shipto)
        df = df[df["__shipto_key"].isin(target_shipto)].copy()
        df.drop(columns=["__shipto_key"], inplace=True, errors="ignore")
    else:
        return pd.DataFrame(columns=["SO NO.", "QB Num", "Item", "Description", "Ship Date", "Qty(+)", "Pre/Bare"])

    need = ["SO NO.", "Customer PO No.", "Model Name", "Ship Date", "Order Qty", "Confirmed Qty", "Description", "Reference"]
    for c in need:
        if c not in df.columns:
            df[c] = np.nan

    ship = df.loc[:, need].copy()
    ship.rename(columns={"Customer PO No.": "QB Num", "Model Name": "Item", "Confirmed Qty": "Qty(+)"}, inplace=True)

    ship["QB Num"] = ship["QB Num"].astype(str).str.split("(").str[0].str.strip()
    ship["Item"] = ship["Item"].astype(str).str.strip()
    ship["Description"] = ship["Description"].astype(str)

    ship_date_raw = ship["Ship Date"].astype("string").str.strip()
    tbc_mask = ship_date_raw.str.upper().eq("TBC")
    ship["Ship Date"] = pd.to_datetime(ship["Ship Date"], errors="coerce")
    ship.loc[tbc_mask, "Ship Date"] = UNASSIGNED_LT_DATE

    ship["Qty(+)"] = pd.to_numeric(ship["Qty(+)"], errors="coerce").fillna(0)
    ship["Order Qty"] = pd.to_numeric(ship["Order Qty"], errors="coerce").fillna(0)
    fallback_mask = tbc_mask & ship["Qty(+)"].eq(0)
    ship.loc[fallback_mask, "Qty(+)"] = ship.loc[fallback_mask, "Order Qty"]
    ship["Qty(+)"] = ship["Qty(+)"].astype(int)

    model_key = ship["Item"].astype(str).str.upper().str.strip()
    model_ok = (
        model_key.str.startswith(PREINSTALL_MODEL_PREFIXES, na=False)
        & ~model_key.str.startswith(PREINSTALL_EXCLUDED_PREFIXES)
        & ~model_key.isin(PREINSTALL_MODEL_EXCLUSIONS)
    )
    including_ok = ship["Description"].str.contains(r"[ï¼Œ,]\s*including\b", case=False, na=False)
    ship["Pre/Bare"] = np.where(model_ok & including_ok, "Pre", "Bare")

    desired = ["SO NO.", "QB Num", "Item", "Description", "Ship Date", "Qty(+)", "Pre/Bare"]
    ship = ship.reindex(columns=[c for c in desired if c in ship.columns] + [c for c in ship.columns if c not in desired])
    return ship


__all__ = ["transform_shipping"]
