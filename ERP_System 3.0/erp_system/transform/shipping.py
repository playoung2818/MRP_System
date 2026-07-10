from __future__ import annotations

import re

import numpy as np
import pandas as pd

from erp_system.runtime.constants import UNASSIGNED_LT_DATE
from erp_system.runtime.policies import PREINSTALL_MODEL_PREFIXES


# Shipping model names that represent a fixed group of inventory items.
# Keys are matched case-insensitively after surrounding whitespace is removed.
SHIPPING_MODEL_GROUP_MAPPINGS: dict[str, tuple[tuple[str, float], ...]] = {
    "NRU-161V-AWP-JON16-RC01": (
        ("NRU-161V-AWP", 1.0),
        ("GC-Jetson-NX16G-Orin-Nvidia", 1.0),
        ("M.242-SSD-256GB-P34-TLC5WT-TD1", 1.0),
    ),
    "FLYC-300-JON16-IN01": (
        ("FLYC-300-EC-JON16-NS", 1.0),
        ("M.230-SSD-1TB-PCIe4-TLC-TD", 1.0),
    ),
    "SEMIL-1748GC-10G-L4-EL06": (
        ("SEMIL-1748GC-10G-L4-BSK(EA)", 1.0),
        ("E-2278GE", 1.0),
        ("DDR4-32GB-ECC26WT-DL", 1.0),
        ("M.280-SSD-2TB-PCIe44-TLC5ET-TD1", 1.0),
        ("Cbl-W5M-M12A5M-40CM-PK-CANFD-TP", 4.0),
        ("Cbl-W20F-M12A10F-40CM-IK-COM", 1.0),
        ("DtC-M12M-WP", 4.0),
        ("DtC-M12-WP", 1.0),
        ("mPCIe-CAN-IPEH-4047", 1.0),
        ("mPCIe-COM-2RS232-X203", 1.0)
    ),
}


def get_shipping_model_group(model_name: object) -> tuple[tuple[str, float], ...] | None:
    """Return the fixed item group configured for a shipping model name."""
    key = str(model_name).strip().upper()
    for model, items in SHIPPING_MODEL_GROUP_MAPPINGS.items():
        if model.upper() == key:
            return items
    return None


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
    model_ok = model_key.str.startswith(PREINSTALL_MODEL_PREFIXES, na=False)
    including_ok = ship["Description"].str.contains(r"[,\uFF0C]\s*including\b", case=False, na=False)
    ship["Pre/Bare"] = np.where(model_ok & including_ok, "Pre", "Bare")

    desired = ["SO NO.", "QB Num", "Item", "Description", "Ship Date", "Qty(+)", "Pre/Bare"]
    ship = ship.reindex(columns=[c for c in desired if c in ship.columns] + [c for c in ship.columns if c not in desired])
    return ship


__all__ = ["SHIPPING_MODEL_GROUP_MAPPINGS", "get_shipping_model_group", "transform_shipping"]
