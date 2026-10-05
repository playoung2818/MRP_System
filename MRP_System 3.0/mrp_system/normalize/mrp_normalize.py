from __future__ import annotations

import json
import os
import re
import tempfile
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

# Generated runtime data, never Python source. Override for deployed installs.
POD_SITE_PATH = Path(os.environ.get("POD_SITE_PATH") or (
    Path(__file__).resolve().parents[2] / "data" / "pod_site.json"
))


def load_pod_site(path: Path | None = None) -> dict[str, str]:
    """Load the last snapshot; a fresh installation starts with an empty map."""
    target = Path(path) if path is not None else POD_SITE_PATH
    try:
        with target.open(encoding="utf-8") as handle:
            site_map = json.load(handle)
    except FileNotFoundError:
        return {}
    if not isinstance(site_map, dict) or any(
        not isinstance(key, str) or not isinstance(value, str)
        for key, value in site_map.items()
    ):
        raise ValueError(f"POD site snapshot must be a JSON object of strings: {target}")
    return site_map


# Keep this dictionary's identity stable for consumers importing POD_SITE.
POD_SITE: dict[str, str] = load_pod_site()


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
    Format POD site mappings for display (not for source-file persistence).
    """
    if not site_map:
        return ""
    lines = [f'    "{pod_no}": "{site}",' for pod_no, site in sorted(site_map.items())]
    return "\n".join(lines)


def refresh_pod_site(df_pod: pd.DataFrame) -> dict[str, str]:
    """Persist detected sites atomically and apply them to the current ETL run.

    The existing POD_SITE object is mutated so imports in ledger.events see
    fresh exclusions immediately. A failed write leaves the previous snapshot
    and in-memory mapping intact.
    """
    site_map = detect_pod_site(df_pod)
    target = POD_SITE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=target.parent,
            prefix=f".{target.name}.", suffix=".tmp", delete=False,
        ) as handle:
            temporary = Path(handle.name)
            json.dump(site_map, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    POD_SITE.clear()
    POD_SITE.update(site_map)
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


__all__ = [
    "normalize_item",
    "ITEM_MAPPINGS",
    "PATTERN_MAPPINGS",
    "POD_SITE",
    "POD_SITE_PATH",
    "load_pod_site",
    "detect_pod_site",
    "format_pod_site_entries",
    "refresh_pod_site",
]
