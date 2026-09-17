from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd

# Direct canonical-name mappings used across all ingestion sources
# (POD memo parsing, shipping expansion, SO/item normalization paths).
# Item Names been shortened because they exceed the maximum length allowed on QB, now expand them
ITEM_MAPPINGS: dict[str, str] = {
    "AccsyBx-Cardholder-10108GC-5080": "AccsyBx-Cardholder-10108GC-5080_70_70Ti",
    "AccsyBx-Cardholder-10208GC-5080": "AccsyBx-Cardholder-10208GC-5080_70_70Ti",
    "AccsyBx-Cardholder-9160GC-2000E": "AccsyBx-Cardholder-9160GC-2000EAda",
    "Cbl-M12A5F-OT2-B-Red-Fuse-100CM": "Cbl-M12A5F-OT2-Black-Red-Fuse-100CM",
    "Cblkit-FP-NRU-230V-AWP_NRU-240S": "Cblkit-FP-NRU-230V-AWP_NRU-240S-AWP",
    "E-mPCIe-BTWifi-WT-6218_Mod_40CM": "Extnd-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM_kits",
    "E-mPCIe-GPS-M800_Mod_40CM": "Extnd-mPCIeHS_GPS-M800_Mod_Cbl-40CM_kits",
    "E-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM": "Extnd-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-40CM_kits",
    "E-mPCIeHS_GPS-M800_Mod_Cbl-40CM": "Extnd-mPCIeHS_GPS-M800_Mod_Cbl-40CM_kits",
    "E-mPCIeHS-BTWifi-WT-6218_Mod_Cbl-15CM": "E-mPCIe-BTWifi-WT-6218_Mod_15CM",
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
    "M.280-SSD-1TB-SATA-TLC5-P N": "M.280-SSD-1TB-SATA-TLC5-PN", ## Taipei SAP Description typo
    "M.280-SSD-2TB- PCIe44-TLC5WT-TD": "M.280-SSD-2TB-PCIE44-TLC5WT-TD",
    "M.280-SSD-4TB-PCIe4-TLCWT5NH-IK": "M.280-SSD-4TB-PCIe4-TLC5WT-NH-IK",
    "M.280-SSD-512GB-PCIe44-TLC5WT-T": "M.280-SSD-512GB-PCIe44-TLC5WT-TD",
    "M.280-SSD-256GB-P44-TLC5WT-TD2": "M.280-SSD-256GB-PCIe44-TLC5WT-TD2",
    "M.242-SSD-256GB-P34-TLC5WT-TD1": "M.242-SSD-256GB-PCIe34-TLC5WT-TD1",
    "M.280-SSD-512GB-P44-TLC5WT-TD2": "M.280-SSD-512GB-PCIe44-TLC5WT-TD2",
    "PA-280W-CW6P-2P-1": "PA-280W-CW6P-2P",
    "GC-J-A64GB-O-Industrial-Nvidia": "GC-JETSON-AGX64GB-ORIN-INDUSTRIAL-NVIDIA",
    "GC-AGXOrin Ind. 64G-JP 6.0_NRU-230/240S": "GC-JETSON-AGX64GB-ORIN-INDUSTRIAL-NVIDIA",
    "AccsyBx-FPnl_3Ant-Cbl-NRU-170-PPC series": "AccsyBx-FPnl_3Ant-Cbl-NRU170PPC",
    "AccsyBx-Cardholder-10109GC-508070TVentus": "AccsyBx-Cardholder-10109GC-5080",
    "AccsyBx-RPnl_3Ant-Cbl-POC-766AW": "AccsyBx-RPnl_3Ant-Cbl-POC-766AWP",
    "Cbl-2W5M-M12A8F-40CM-PK-CANFD-T": "Cbl-2W5M-M12A8F-40CM-PK-CANFD-TP",
    "RPnl-2LTE_2Wifi-SEMIL17": "Pnl-2LTE2Wifi-SEMIL17",
    "NRU-240S-AWP-BAG": "NRU-240S-AWP-BAG(EA)",
    "AccsyBx-Cardholder-960GC-RTX Pro 2000":"AccsyBx-CH-960GC-RTX Pro 2000",
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
            r"^GC[-_ ]?JETSON[-_ ]?AGX64GB[-_ ]?ORIN[-_ ]?INDUSTRIAL[-_ ]?NVIDIA(?:[-_ ]?(?:JET\s*PACK|JET\s*PAC\s*K|JP)\s*[-_ ]*[\d\.]+)?(?:[_ -].*)?$",
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
    "POD-260290": "WH01X-NTA",
    "POD-260291": "WH01X-NTA",
    "POD-260338": "Drop Ship",
    "POD-260339": "Drop Ship",
    "POD-260340": "Drop Ship",
    "POD-260509": "Drop Ship",
    "POD-260798": "Drop Ship",
    "POD-260887": "WH01X-NTA",
    "POD-260888": "WH01X-NTA",
    "POD-260889": "WH01X-NTA",
    "POD-260981": "WH01X-NTA",
    "POD-261057": "Drop Ship",
    "POD-261068": "WH01X-NTA",
    "POD-261069": "WH01X-NTA",
    "POD-261070": "WH01X-NTA",
    "POD-261073": "WH01X-NTA",
    "POD-261171": "Drop Ship",
    "POD-261193": "Drop Ship",
    "POD-261195": "Drop Ship",
    "POD-261196": "Drop Ship",
    "POD-261208": "Drop Ship",
    "POD-261216": "Drop Ship",
    "POD-261226": "Drop Ship",
    "POD-261254": "Drop Ship",
    "POD-261255": "Drop Ship",
    "POD-261259": "Drop Ship",
    "POD-261260": "Drop Ship",
    "POD-261267": "Drop Ship",
    "POD-261271": "Drop Ship",
    "POD-261280": "Drop Ship",
    "POD-261281": "Drop Ship",
    "POD-261286": "Drop Ship",
    "POD-261287": "Drop Ship",
    "POD-261302": "Drop Ship",
    "POD-261330": "Drop Ship",
    "POD-261341": "Drop Ship",
    "POD-261344": "Drop Ship",
    "POD-261345": "Drop Ship",
    "POD-261349": "Drop Ship",
    "POD-261354": "Drop Ship",
    "POD-261355": "Drop Ship",
    "POD-261369": "Drop Ship",
    "POD-261378": "Drop Ship",
    "POD-261380": "Drop Ship",
    "POD-261381": "Drop Ship",
    "POD-261392": "Drop Ship",
    "POD-261393": "Drop Ship",
    "POD-261417": "WH01DK-NTA",
    "POD-261423": "Drop Ship",
    "POD-261424": "Drop Ship",
    "POD-261429": "Drop Ship",
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
        & pod["Inventory Site"].ne(include_site),
        ["POD#", "Inventory Site"],
    ].drop_duplicates(subset=["POD#"], keep="last")

    return dict(zip(pod["POD#"], pod["Inventory Site"]))


def format_pod_site_entries(site_map: dict[str, str]) -> str:
    """
    Format POD site mappings as dictionary lines matching POD_SITE style.
    """
    if not site_map:
        return ""
    lines = [f'    "{pod_no}": "{site}",' for pod_no, site in sorted(site_map.items())]
    return "\n".join(lines)


_POD_SITE_BLOCK_RE = re.compile(
    r"POD_SITE: dict\[str, str\] = \{\n.*?\n\}",
    re.DOTALL,
)


def _build_pod_site_block(site_map: dict[str, str]) -> str:
    lines = format_pod_site_entries(site_map)
    if lines:
        return f"POD_SITE: dict[str, str] = {{\n{lines}\n}}"
    return "POD_SITE: dict[str, str] = {\n}"


def refresh_pod_site(df_pod: pd.DataFrame) -> dict[str, str]:
    """
    Recompute POD_SITE from a raw POD dataframe (same shape as detect_pod_site
    expects) and rewrite the POD_SITE block in this file on disk.

    This edits the source file in place, so it takes effect starting the next
    process run -- the POD_SITE already imported into memory for the run that
    calls this is unchanged.
    """
    site_map = detect_pod_site(df_pod)

    target = Path(__file__).resolve()
    text = target.read_text(encoding="utf-8")
    new_block = _build_pod_site_block(site_map)
    updated_text, count = _POD_SITE_BLOCK_RE.subn(new_block, text, count=1)
    if count != 1:
        raise RuntimeError("Could not find POD_SITE block in erp_normalize.py")
    target.write_text(updated_text, encoding="utf-8")
    return site_map


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
    "refresh_pod_site",
]
