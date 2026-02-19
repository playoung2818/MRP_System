from __future__ import annotations

import re
from typing import Any

import pandas as pd

# MAP from QB to SAP
ITEM_MAPPINGS: dict[str, str] = {
    "AccsyBx-Cardholder-10108GC-5080": "AccsyBx-Cardholder-10108GC-5080_70_70Ti",
    "AccsyBx-Cardholder-10208GC-5080": "AccsyBx-Cardholder-10208GC-5080_70_70Ti",
    "AccsyBx-Cardholder-9160GC-2000E": "AccsyBx-Cardholder-9160GC-2000EAda",
    "Cbl-M12A5F-OT2-B-Red-Fuse-100CM": "Cbl-M12A5F-OT2-Black-Red-Fuse-100CM",
    "Cblkit-FP-NRU-230V-AWP_NRU-240S": "Cblkit-FP-NRU-230V-AWP_NRU-240S-AWP",
    "E-mPCIe-BTWifi-WT-6218_Mod_40CM": "Extnd-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM_kits",
    "E-mPCIe-GPS-M800_Mod_40CM": "Extnd-mPCIeHS_GPS-M800_Mod_Cbl-40CM_kits",
    "FPnl-3Ant-NRU-170-PPC series": "FPnl-3Ant-NRU-170-PPCseries",
    "M.242-SSD-128GB-PCIe34-TLC5WT-T": "M.242-SSD-128GB-PCIe34-TLC5WT-TD",
    "M.242-SSD-256GB-PCIe34-TLC5WT-T": "M.242-SSD-256GB-PCIe34-TLC5WT-TD",
    "M.280-SSD-256GB-PCIe44-TLC5WT-T": "M.280-SSD-256GB-PCIe44-TLC5WT-TD",
    "M.280-SSD-4TB-PCIe4-TLCWT5NH-IK": "M.280-SSD-4TB-PCIe4-TLC5WT-NH-IK",
    "M.280-SSD-512GB-PCIe44-TLC5WT-T": "M.280-SSD-512GB-PCIe44-TLC5WT-TD",
}

# MAP from SAP to QB
PATTERN_MAPPINGS = [
    (
        re.compile(
            r"^GC[-_ ]?AGXORIN64G[-_ ]?(?:JETPACK|JP)\s*[\d\.]*(?:[_ -].*)?$",
            re.IGNORECASE,
        ),
        "GC-JETSON-AGX64GB-ORIN-NVIDIA",
    ),
    (
        re.compile(
            r"^GC[-_ ]?AGXORIN\s*IND\.?\s*64G[-_ ]?(?:JETPACK|JP)\s*[\d\.]*(?:[_ -].*)?$",
            re.IGNORECASE,
        ),
        "GC-JETSON-AGX64GB-ORIN-INDUSTRIAL-NVIDIA",
    ),
    (
        re.compile(
            r"^GC[-_ ]?ORINNX16G[-_ ]?(?:JETPACK|JP)\s*[\d\.]*(?:[_ -].*)?$",
            re.IGNORECASE,
        ),
        "GC-JETSON-NX16G-ORIN-NVIDIA",
    ),
    (
        re.compile(
            r"^GC-Jetson-AGX64GB-Orin-Nvidia(?:[- ]?JetPack[-_ ]?[\d\\.]+)?$",
            re.IGNORECASE,
        ),
        "GC-Jetson-AGX64GB-Orin-Nvidia",
    ),
    (
        re.compile(
            r"^GC-Jetson-AGX32GB-Orin-Nvidia(?:[- ]?JetPack[-_ ]?[\d\\.]+)?$",
            re.IGNORECASE,
        ),
        "GC-Jetson-AGX32GB-Orin-Nvidia",
    ),
    (
        re.compile(
            r"^GC-Jetson-NX16G-Orin-Nvidia(?:[- ]?JetPack[-_ ]?[\d\\.]+)?$",
            re.IGNORECASE,
        ),
        "GC-Jetson-NX16G-Orin-Nvidia",
    ),
]


def normalize_item(value: Any) -> Any:
    """
    Normalize a single item name/identifier:
    1) Preserve missing values.
    2) Strip whitespace and apply direct ITEM_MAPPINGS.
    3) Apply regex patterns (Jetson JetPack variants) for canonical names.
    """
    if value is None:
        return value
    try:
        if pd.isna(value):
            return value
    except Exception:
        # Fallback if the object is not pandas-aware
        pass

    name = str(value).strip()
    if not name:
        return name

    direct = ITEM_MAPPINGS.get(name)
    if direct:
        return direct

    for pattern, replacement in PATTERN_MAPPINGS:
        if pattern.match(name):
            return ITEM_MAPPINGS.get(replacement, replacement)

    return name


def normalize_series(series: pd.Series) -> pd.Series:
    """Vectorized helper to normalize a pandas Series of item names."""
    return series.apply(normalize_item)


__all__ = ["normalize_item", "normalize_series", "ITEM_MAPPINGS", "PATTERN_MAPPINGS"]
