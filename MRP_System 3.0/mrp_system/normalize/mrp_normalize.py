from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

from .part_aliases import canonical_part_for, refresh_part_number_aliases

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
    2) Apply database alias_part -> canonical_part mappings.
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

    direct = canonical_part_for(name)
    if direct:
        return direct

    for pattern, replacement in PATTERN_MAPPINGS:
        if pattern.match(name):
            return canonical_part_for(replacement) or replacement

    return name


__all__ = [
    "normalize_item",
    "refresh_part_number_aliases",
    "PATTERN_MAPPINGS",
    "POD_SITE",
    "POD_SITE_PATH",
    "load_pod_site",
    "detect_pod_site",
    "format_pod_site_entries",
    "refresh_pod_site",
]
