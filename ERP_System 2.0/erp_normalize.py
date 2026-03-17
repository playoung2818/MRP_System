from __future__ import annotations

import re
from typing import Any

import pandas as pd

# Direct canonical-name mappings used across all ingestion sources
# (POD memo parsing, shipping expansion, SO/item normalization paths).
ITEM_MAPPINGS: dict[str, str] = {
    "AccsyBx-Cardholder-10108GC-5080": "AccsyBx-Cardholder-10108GC-5080_70_70Ti",
    "AccsyBx-Cardholder-10208GC-5080": "AccsyBx-Cardholder-10208GC-5080_70_70Ti",
    "AccsyBx-Cardholder-9160GC-2000E": "AccsyBx-Cardholder-9160GC-2000EAda",
    "Cbl-M12A5F-OT2-B-Red-Fuse-100CM": "Cbl-M12A5F-OT2-Black-Red-Fuse-100CM",
    "Cblkit-FP-NRU-230V-AWP_NRU-240S": "Cblkit-FP-NRU-230V-AWP_NRU-240S-AWP",
    "E-mPCIe-BTWifi-WT-6218_Mod_40CM": "Extnd-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM_kits",
    "E-mPCIe-GPS-M800_Mod_40CM": "Extnd-mPCIeHS_GPS-M800_Mod_Cbl-40CM_kits",
    "E-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM": "Extnd-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM_kits",
    "M.2 Key B_LTE_Telit FN990A40_15cm": "M.2 Key B_LTE_Telit FN990A40_15",
    "M.2 KEY B_LTE_TELIT FN990A40_15CM": "M.2 Key B_LTE_Telit FN990A40_15",
    "FPnl-3Ant-NRU-160-AWP series": "FPnl-3Ant-of NRU-160-AWP series",
    "FPnl-3Ant-of": "FPnl-3Ant-of NRU-160-AWP series",
    "mPCIeHS_BTWifi_Emwicon WMX6218_40cm": "Extnd-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM_kits",
    "mPCIeHS_BTWifi_Emwicon WMX6218_15cm": "mPCIeHS_BTWifi_WMX6218_15cm",
    "M.242-SSD-128GB-PCIe34-TLC5WT-T": "M.242-SSD-128GB-PCIe34-TLC5WT-TD",
    "M.242-SSD-128G-PCIe34-TLC5WT-TD": "M.242-SSD-128GB-PCIe34-TLC5WT-TD",
    "M.242-SSD-256GB-PCIe34-TLC5WT-T": "M.242-SSD-256GB-PCIe34-TLC5WT-TD",
    "M.242-SSD-256G-PCIe34-TLC5WT-TD": "M.242-SSD-256GB-PCIe34-TLC5WT-TD",
    "M.242-SSD-512GB-PCIe34-TLC5WT-T": "M.242-SSD-512GB-PCIE34-TLC5WT-TD",
    "M.280-SSD-128G-SATA-TLC5WT-TD": "M.280-SSD-128GB-SATA-TLC5WT-TD",
    "M.280-SSD-256GB-PCIe44-TLC5WT-T": "M.280-SSD-256GB-PCIe44-TLC5WT-TD",
    "M.280-SSD-1TB-SATA-TLC5-P N": "M.280-SSD-1TB-SATA-TLC5-PN",
    "M.280-SSD-2TB- PCIe44-TLC5WT-TD": "M.280-SSD-2TB-PCIE44-TLC5WT-TD",
    "M.280-SSD-4TB-PCIe4-TLCWT5NH-IK": "M.280-SSD-4TB-PCIe4-TLC5WT-NH-IK",
    "M.280-SSD-512GB-PCIe44-TLC5WT-T": "M.280-SSD-512GB-PCIe44-TLC5WT-TD",
    "PA-280W-CW6P-2P-1": "PA-280W-CW6P-2P",
    "GC-J-A64GB-O-Industrial-Nvidia": "GC-JETSON-AGX64GB-ORIN-INDUSTRIAL-NVIDIA",
    "AccsyBx-FPnl_3Ant-Cbl-NRU-170-PPC series": "AccsyBx-FPnl_3Ant-Cbl-NRU170PPC"
}

# Pattern-based canonical mappings (e.g., JetPack/JP variants).
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
    (
        re.compile(
            r"^GC[-_ ]?ORINNX8G[-_ ]?(?:JETPACK|JP)\s*[\d\.]*(?:[_ -].*)?$",
            re.IGNORECASE,
        ),
        "GC-JETSON-NX8G-ORIN-NVIDIA",
    ),
]

POD_SITE: dict[str, str] = {
    "POD-251229": "WH02D-NTA",
    "POD-251518": "WH01X-NTA",
    "POD-251590": "Drop Ship",
    "POD-251593": "Drop Ship",
    "POD-251594": "Drop Ship",
    "POD-251705": "Drop Ship",
    "POD-251728": "Drop Ship",
    "POD-251759": "Drop Ship",
    "POD-260006": "Drop Ship",
    "POD-260015": "Drop Ship",
    "POD-260016": "Drop Ship",
    "POD-260017": "Drop Ship",
    "POD-260031": "Drop Ship",
    "POD-260046": "Drop Ship",
    "POD-260087": "Drop Ship",
    "POD-260096": "WH01X-NTA",
    "POD-260106": "Drop Ship",
    "POD-260107": "Drop Ship",
    "POD-260108": "Drop Ship",
    "POD-260109": "Drop Ship",
    "POD-260119": "WH01X-NTA",
    "POD-260120": "WH01X-NTA",
    "POD-260121": "WH01X-NTA",
    "POD-260137": "Drop Ship",
    "POD-260144": "Drop Ship",
    "POD-260145": "Drop Ship",
    "POD-260158": "Drop Ship",
    "POD-260161": "Drop Ship",
    "POD-260162": "Drop Ship",
    "POD-260163": "Drop Ship",
    "POD-260171": "Drop Ship",
    "POD-260182": "WH01X-NTA",
    "POD-260202": "Drop Ship",
    "POD-260208": "Drop Ship",
    "POD-260237": "WH10Parts- NTA",
    "POD-260250": "Drop Ship",
    "POD-260261": "WH01X-NTA",
    "POD-260267": "Drop Ship",
    "POD-260268": "Drop Ship",
    "POD-260269": "WH01X-NTA",
    "POD-260273": "Drop Ship",
    "POD-260275": "Drop Ship",
    "POD-260276": "Drop Ship",
    "POD-260282": "WH01DK-NTA",
    "POD-260284": "Drop Ship",
    "POD-260285": "WH01X-NTA",
    "POD-260286": "WH01X-NTA",
    "POD-260287": "WH01X-NTA",
    "POD-260288": "WH01X-NTA",
    "POD-260289": "WH01X-NTA",
    "POD-260290": "WH01X-NTA",
    "POD-260291": "WH01X-NTA",
    "POD-260313": "Drop Ship",
    "POD-260314": "Drop Ship",
    "POD-260315": "Drop Ship",
    "POD-260316": "Drop Ship",
    "POD-260317": "Drop Ship",
    "POD-260318": "Drop Ship",
    "POD-260322": "WH01X-NTA",
    "POD-260325": "WH01X-NTA",
    "POD-260326": "WH01X-NTA",
    "POD-260328": "Drop Ship",
    "POD-260329": "Drop Ship",
    "POD-260342": "Drop Ship",
    "POD-260346": "Drop Ship",
    "POD-260350": "Drop Ship",
    "POD-260351": "Drop Ship",
    "POD-260352": "Drop Ship",
    "POD-260353": "Drop Ship",
    "POD-260359": "Drop Ship",
    "POD-260360": "Drop Ship",
    "POD-260361": "Drop Ship",
    "POD-260362": "Drop Ship",
    "POD-260371": "Drop Ship",
    "POD-260379": "Drop Ship",
}


def detect_pod_site(df_pod: pd.DataFrame, *, include_site: str = "WH01S-NTA") -> dict[str, str]:
    """
    Return POD -> Inventory Site mappings for POD rows whose Inventory Site differs
    from the included/default site.

    Expected POD export columns:
    - Inventory Site
    - POD# or Num/QB Num
    """
    if df_pod is None or df_pod.empty or "Inventory Site" not in df_pod.columns:
        return {}

    pod = df_pod.copy()
    if "POD#" not in pod.columns:
        if "Num" in pod.columns:
            pod["POD#"] = pod["Num"]
        elif "QB Num" in pod.columns:
            pod["POD#"] = pod["QB Num"]
        else:
            return {}

    pod["POD#"] = pod["POD#"].fillna("").astype(str).str.split("(", expand=True)[0].str.strip()
    pod["Inventory Site"] = pod["Inventory Site"].fillna("").astype(str).str.strip()

    pod = pod.loc[
        pod["POD#"].ne("")
        & pod["Inventory Site"].ne("")
        & pod["Inventory Site"].ne(include_site)
    , ["POD#", "Inventory Site"]].drop_duplicates(subset=["POD#"], keep="last")

    return dict(zip(pod["POD#"], pod["Inventory Site"]))


def format_pod_site_entries(site_map: dict[str, str]) -> str:
    """
    Format POD site mappings as dictionary lines matching POD_SITE style.
    """
    if not site_map:
        return ""
    lines = [f'    "{pod_no}": "{site}",' for pod_no, site in sorted(site_map.items())]
    return "\n".join(lines)


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


__all__ = [
    "normalize_item",
    "normalize_series",
    "ITEM_MAPPINGS",
    "PATTERN_MAPPINGS",
    "POD_SITE",
    "detect_pod_site",
    "format_pod_site_entries",
]
