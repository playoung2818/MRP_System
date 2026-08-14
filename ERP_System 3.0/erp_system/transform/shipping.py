from __future__ import annotations

import re

import numpy as np
import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item
from erp_system.runtime.constants import PLACEHOLDER_DATE
from erp_system.runtime.policies import PREINSTALL_KEEP_MODEL_SKIP_FIRST_COMPONENT_PREFIXES, PREINSTALL_MODEL_PREFIXES


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
    "RGS-8805GC-7543P": (
        ("RGS-8805GC", 1.0),
        ("7543P", 1.0),
        ("M.280-SSD-2TB-PCIe44-TLC5ET-TD1", 1.0)
    ),
    "Nuvo-9006LP-AUT-KS-CF1": (
        ("Nuvo-9006LP-AUT-KS", 1.0),
        ("i9-13900", 1.0),
        ("DDR5-16GB-56-SM", 2.0),
        ("M.280-SSD-1TB-PCIe44-TLC5-PN", 1.0),
        ("M.230-10BASET1S-KS", 1.0),
        ("MezIO-AE304-KS(EA)", 1.0)
    ),
    "Nuvo-9006LP-AUT-KS-CF2": (
        ("Nuvo-9006LP-AUT-KS", 1.0),
        ("i9-13900", 1.0),
        ("DDR5-16GB-56-SM", 2.0),
        ("M.280-SSD-1TB-PCIe44-TLC5-PN", 1.0),
        ("MezIO-AE304-KS(EA)", 1.0)
    ),
}

# Shipping model names whose core inventory items are fixed, while additional
# peripherals must still be parsed from the shipping description.
SHIPPING_MODEL_CORE_MAPPINGS: dict[str, tuple[tuple[str, float], ...]] = {
    "NRU-161V-AWP-JON16-NS": (
        ("NRU-161V-AWP", 1.0),
        ("GC-Jetson-NX16G-Orin-Nvidia", 1.0),
    ),
    "NRU-172S-PPC-JON16-NS" : (
        ("NRU-172S-PPC", 1.0),
        ("GC-Jetson-NX16G-Orin-Nvidia", 1.0),
    ),
    "NRU-171V-PPC-JON16-NS" : (
        ("NRU-171V-PPC", 1.0),
        ("GC-Jetson-NX16G-Orin-Nvidia", 1.0),
    ),
}

# Shipping model names that are a fixed 1:1 split into exactly two component
# items, with no description parsing involved (unlike SHIPPING_MODEL_CORE_MAPPINGS).
NUVO_716_VARIANT_SPLITS: dict[str, tuple[str, str]] = {
    "NUVO-7160GC-POE": ("Nuvo-716xGC-PoE", "CSM-7160GC"),
    "NUVO-7162GC-POE": ("Nuvo-716xGC-PoE", "CSM-7162GC"),
    "NUVO-7166GC-POE": ("Nuvo-716xGC-PoE", "CSM-7166GC"),
    "NUVO-7168GC-POE": ("Nuvo-716xGC-PoE", "CSM-7168GC"),
    "NUVO-7160GC": ("Nuvo-716xGC", "CSM-7160GC"),
    "NUVO-7162GC": ("Nuvo-716xGC", "CSM-7162GC"),
    "NUVO-7166GC": ("Nuvo-716xGC", "CSM-7166GC"),
    "NUVO-7168GC": ("Nuvo-716xGC", "CSM-7168GC"),
}

INCL_SPLIT = re.compile(r"\bincluding\b", re.IGNORECASE)
QTYX_RE = re.compile(r"^\s*(\d+)\s*x\s*(.+)\s*$", re.IGNORECASE) # Detect quantity prefixes like: "2 x SSD-512GB-TLC5WT-TD1"
ITEM_AND_SPLIT = re.compile(r"\s+\band\b\s+(?=(?:[A-Z0-9]+[.-]){1,}[A-Z0-9])", re.IGNORECASE) # Detect an _and_ that is probably joining two item codes


def get_shipping_model_group(model_name: object) -> tuple[tuple[str, float], ...] | None:
    """Return the fixed item group configured for a shipping model name."""
    key = str(model_name).strip().upper()
    for model, items in SHIPPING_MODEL_GROUP_MAPPINGS.items():
        if model.upper() == key:
            return items
    return None


def get_shipping_model_core_group(model_name: object) -> tuple[tuple[str, float], ...] | None:
    """Return core items while allowing description peripherals to be added."""
    key = str(model_name).strip().upper()
    for model, items in SHIPPING_MODEL_CORE_MAPPINGS.items():
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
    ship.loc[tbc_mask, "Ship Date"] = PLACEHOLDER_DATE

    ship["Qty(+)"] = pd.to_numeric(ship["Qty(+)"], errors="coerce").fillna(0)
    ship["Order Qty"] = pd.to_numeric(ship["Order Qty"], errors="coerce").fillna(0)
    fallback_mask = tbc_mask & ship["Qty(+)"].eq(0)
    ship.loc[fallback_mask, "Qty(+)"] = ship.loc[fallback_mask, "Order Qty"]
    ship["Qty(+)"] = ship["Qty(+)"].astype(int)

    model_key = ship["Item"].astype(str).str.upper().str.strip()
    model_ok = model_key.str.startswith(PREINSTALL_MODEL_PREFIXES, na=False)
    including_ok = ship["Description"].str.contains(r"[,，]\s*including\b", case=False, na=False)
    ship["Pre/Bare"] = np.where(model_ok & including_ok, "Pre", "Bare")

    desired = ["SO NO.", "QB Num", "Item", "Description", "Ship Date", "Qty(+)", "Pre/Bare"]
    ship = ship.reindex(columns=[c for c in desired if c in ship.columns] + [c for c in ship.columns if c not in desired])
    return ship


def clean_space(s: str) -> str:
    if not isinstance(s, str):
        return ""
    cleaned = s.replace("_x000D_", " ")
    cleaned = cleaned.replace("\r", " ").replace("\n", " ")
    cleaned = cleaned.replace(" ", " ").replace("　", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def parse_description(desc: str) -> tuple[str, list[str]]:
    s = clean_space(desc)
    parts = INCL_SPLIT.split(s, maxsplit=1)
    parent = clean_space(parts[0].split(",")[0])
    comps = []
    if len(parts) > 1:
        comma_tokens = [clean_space(x) for x in parts[1].split(",") if clean_space(x)]
        for token in comma_tokens:
            comps.extend(clean_space(x) for x in ITEM_AND_SPLIT.split(token) if clean_space(x))
    return parent, comps


def parse_component_token(token: str) -> tuple[str, float]:
    m = QTYX_RE.match(token)
    if m:
        return clean_space(m.group(2)), float(m.group(1))
    return clean_space(token), 1.0


def split_nuvo_716_variant_item(item: str) -> list[str] | None:
    normalized = clean_space(item).upper()
    split_items = NUVO_716_VARIANT_SPLITS.get(normalized)
    return list(split_items) if split_items else None


def keep_model_skip_first_component(item: str) -> bool:
    normalized = clean_space(item).upper()
    return normalized.startswith(PREINSTALL_KEEP_MODEL_SKIP_FIRST_COMPONENT_PREFIXES)


def _split_special_shipping_variants(sap: pd.DataFrame, *, include_configured_groups: bool = True) -> pd.DataFrame:
    if sap.empty or "Item" not in sap.columns:
        return sap.copy()
    ## Combine 716X Mapping and configured shipping model groups Mapping
    def _group_items(value: object) -> list[tuple[str, float]] | tuple[tuple[str, float], ...] | None:
        if include_configured_groups:
            configured_group = get_shipping_model_group(value)
            if configured_group is not None:
                return configured_group
        variant_items = split_nuvo_716_variant_item(str(value))
        if variant_items is not None:
            return [(item, 1.0) for item in variant_items]
        return None

    special_mask = sap["Item"].astype(str).map(lambda value: _group_items(value) is not None)
    special_rows = sap.loc[special_mask].copy()
    other_rows = sap.loc[~special_mask].copy()
    if special_rows.empty:
        return sap.copy()

    split_parts = []
    for _, row in special_rows.iterrows():
        parent_item = clean_space(str(row.get("Item", "")))
        component_items = _group_items(parent_item) or []
        base_qty = float(row.get("Qty(+)", 0) or 0)
        component_rows = []
        for item, qty_per in component_items:
            out = row.copy()
            out["Parent_Item"] = parent_item
            out["Item"] = item
            out["Qty_per_parent"] = qty_per
            out["Qty(+)"] = base_qty * qty_per
            out["IsParent"] = False
            component_rows.append(out)
        if component_rows:
            split_parts.append(pd.DataFrame(component_rows))

    split_df = pd.concat(split_parts, ignore_index=True) if split_parts else special_rows.iloc[0:0].copy()
    needed_cols = list(sap.columns)
    for col in ["Parent_Item", "Qty_per_parent", "IsParent"]:
        if col not in needed_cols:
            needed_cols.append(col)
    split_df = split_df.reindex(columns=needed_cols, fill_value=pd.NA)
    other_rows = other_rows.reindex(columns=needed_cols, fill_value=pd.NA)
    frames = [df for df in (split_df, other_rows) if not df.empty]
    if not frames:
        return sap.iloc[0:0].copy()
    return frames[0].copy() if len(frames) == 1 else pd.concat(frames, ignore_index=True)


def expand_preinstalled_row(row: pd.Series) -> pd.DataFrame:
    parent, tokens = parse_description(row.get("Description", ""))
    base_qty = float(row.get("Qty(+)", 0) or 0)
    row_item = clean_space(str(row.get("Item", "")))
    parent_item = parent or row_item
    core_group = get_shipping_model_core_group(row_item)
    if core_group is not None:
        parent_item = row_item
        core_item_keys = {normalize_item(item).casefold() for item, _ in core_group}
        tokens = [
            token
            for token in tokens
            if normalize_item(parse_component_token(token)[0]).casefold() not in core_item_keys
        ]
    elif keep_model_skip_first_component(row_item or parent_item):
        parent_item = row_item or parent_item
        tokens = tokens[1:]

    comp_rows = []
    for item, qty_per in core_group or ():
        out = row.copy()
        out["Parent_Item"] = parent_item
        out["Item"] = item
        out["Qty_per_parent"] = qty_per
        out["Qty(+)"] = base_qty * qty_per
        out["IsParent"] = False
        comp_rows.append(out)

    for tok in tokens:
        item, qty_per = parse_component_token(tok)
        out = row.copy()
        out["Parent_Item"] = parent_item
        out["Item"] = item
        out["Qty_per_parent"] = qty_per
        out["Qty(+)"] = base_qty * qty_per
        out["IsParent"] = False
        comp_rows.append(out)

    parent_row = None
    if core_group is None:
        parent_row = row.copy()
        parent_row["Parent_Item"] = parent_item
        parent_row["Item"] = parent_item
        parent_row["Qty_per_parent"] = 1.0
        parent_row["IsParent"] = True
    if comp_rows:
        frames = [pd.DataFrame(comp_rows)]
        if parent_row is not None:
            frames.append(pd.DataFrame([parent_row]))
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame([parent_row])


def expand_sap_preinstalled(sap: pd.DataFrame) -> pd.DataFrame:
    sap = sap.copy()
    for col in ["Pre/Bare", "Qty(+)", "Item"]:
        if col not in sap.columns:
            raise ValueError(f"SAP must contain '{col}' column.")
    if "Description" not in sap.columns:
        sap["Description"] = ""

    sap["Description"] = sap["Description"].astype(str).apply(clean_space)
    configured_mask = sap["Item"].astype(str).map(lambda value: get_shipping_model_group(value) is not None)
    sap_configured = sap.loc[configured_mask].copy()
    sap = sap.loc[~configured_mask].copy()

    pre_mask = sap["Pre/Bare"].astype(str).str.strip().str.casefold().eq("pre")
    sap_pre = sap.loc[pre_mask].copy()
    sap_other = sap.loc[~pre_mask].copy()

    expanded_parts = [expand_preinstalled_row(r) for _, r in sap_pre.iterrows()]
    expanded_pre = pd.concat(expanded_parts, ignore_index=True) if expanded_parts else sap_pre.copy()

    needed_cols = list(sap.columns) + ["Parent_Item", "Qty_per_parent", "IsParent"]
    expanded_pre = expanded_pre.reindex(columns=needed_cols, fill_value=pd.NA)
    sap_other = sap_other.reindex(columns=needed_cols, fill_value=pd.NA)
    sap_other.loc[:, "Parent_Item"] = sap_other["Item"]
    sap_other.loc[:, "Qty_per_parent"] = 1.0
    sap_other.loc[:, "IsParent"] = True

    expanded_configured = _split_special_shipping_variants(sap_configured)
    expanded_configured = expanded_configured.reindex(columns=needed_cols, fill_value=pd.NA)

    expanded_all = pd.concat([expanded_configured, expanded_pre, sap_other], ignore_index=True)
    expanded_all = _split_special_shipping_variants(expanded_all, include_configured_groups=False)
    expanded_all["Qty(+)"] = pd.to_numeric(expanded_all["Qty(+)"], errors="coerce").fillna(0.0)
    expanded_all["Qty_per_parent"] = pd.to_numeric(expanded_all["Qty_per_parent"], errors="coerce").fillna(1.0)
    expanded_all["IsParent"] = expanded_all["IsParent"].astype(bool)
    expanded_all["Date"] = pd.to_datetime(expanded_all["Ship Date"], errors="coerce") + pd.Timedelta(days=5)
    expanded_all["Item"] = expanded_all["Item"].astype(str).map(normalize_item)
    return expanded_all


__all__ = [
    "NUVO_716_VARIANT_SPLITS",
    "SHIPPING_MODEL_CORE_MAPPINGS",
    "SHIPPING_MODEL_GROUP_MAPPINGS",
    "clean_space",
    "expand_sap_preinstalled",
    "get_shipping_model_core_group",
    "get_shipping_model_group",
    "parse_component_token",
    "parse_description",
    "split_nuvo_716_variant_item",
    "transform_shipping",
]
