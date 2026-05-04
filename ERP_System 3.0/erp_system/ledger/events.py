from __future__ import annotations

import re

import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype

from erp_system.normalize.erp_normalize import POD_SITE, normalize_item
from erp_system.runtime.policies import EXCLUDED_POD_SOURCE_NAMES
from erp_system.transform.common import _norm_cols, _norm_key


INCL_SPLIT = re.compile(r"\bincluding\b", re.IGNORECASE)
QTYX_RE = re.compile(r"^\s*(\d+)\s*x\s*(.+)\s*$", re.IGNORECASE)
ITEM_AND_SPLIT = re.compile(r"\s+\band\b\s+(?=(?:[A-Z0-9]+[.-]){1,}[A-Z0-9])", re.IGNORECASE)
NUVO_716_VARIANT_SPLITS: dict[str, tuple[str, str]] = {
    "NUVO-7160GC-POE": ("Nuvo-716xGC-PoE", "CSM-7160GC"),
    "NUVO-7162GC-POE": ("Nuvo-716xGC-PoE", "CSM-7162GC"),
    "NUVO-7166GC-POE": ("Nuvo-716xGC-PoE", "CSM-7166GC"),
    "NUVO-7160GC": ("Nuvo-716xGC", "CSM-7160GC"),
    "NUVO-7162GC": ("Nuvo-716xGC", "CSM-7162GC"),
    "NUVO-7166GC": ("Nuvo-716xGC", "CSM-7166GC"),
}


def clean_space(s: str) -> str:
    if not isinstance(s, str):
        return ""
    cleaned = s.replace("_x000D_", " ")
    cleaned = cleaned.replace("\r", " ").replace("\n", " ")
    cleaned = cleaned.replace("\u00A0", " ").replace("\u3000", " ")
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


def _split_special_shipping_variants(nav: pd.DataFrame) -> pd.DataFrame:
    if nav.empty or "Item" not in nav.columns:
        return nav.copy()

    special_mask = nav["Item"].astype(str).map(lambda value: split_nuvo_716_variant_item(value) is not None)
    special_rows = nav.loc[special_mask].copy()
    other_rows = nav.loc[~special_mask].copy()
    if special_rows.empty:
        return nav.copy()

    split_parts = []
    for _, row in special_rows.iterrows():
        parent_item = clean_space(str(row.get("Item", "")))
        component_items = split_nuvo_716_variant_item(parent_item) or []
        base_qty = float(row.get("Qty(+)", 0) or 0)
        component_rows = []
        for item in component_items:
            out = row.copy()
            out["Parent_Item"] = parent_item
            out["Item"] = item
            out["Qty_per_parent"] = 1.0
            out["Qty(+)"] = base_qty
            out["IsParent"] = False
            component_rows.append(out)
        if component_rows:
            split_parts.append(pd.DataFrame(component_rows))

    split_df = pd.concat(split_parts, ignore_index=True) if split_parts else special_rows.iloc[0:0].copy()
    needed_cols = list(nav.columns)
    for col in ["Parent_Item", "Qty_per_parent", "IsParent"]:
        if col not in needed_cols:
            needed_cols.append(col)
    split_df = split_df.reindex(columns=needed_cols, fill_value=pd.NA)
    other_rows = other_rows.reindex(columns=needed_cols, fill_value=pd.NA)
    frames = [df for df in (split_df, other_rows) if not df.empty]
    if not frames:
        return nav.iloc[0:0].copy()
    return frames[0].copy() if len(frames) == 1 else pd.concat(frames, ignore_index=True)


def expand_preinstalled_row(row: pd.Series) -> pd.DataFrame:
    parent, tokens = parse_description(row.get("Description", ""))
    base_qty = float(row.get("Qty(+)", 0) or 0)
    parent_item = parent or clean_space(str(row.get("Item", "")))
    variant_split = split_nuvo_716_variant_item(parent_item)
    if variant_split:
        tokens = variant_split
        parent_item = clean_space(str(row.get("Item", "")))

    comp_rows = []
    for tok in tokens:
        item, qty_per = parse_component_token(tok)
        out = row.copy()
        out["Parent_Item"] = parent_item
        out["Item"] = item
        out["Qty_per_parent"] = qty_per
        out["Qty(+)"] = base_qty * qty_per
        out["IsParent"] = False
        comp_rows.append(out)

    parent_row = row.copy()
    parent_row["Parent_Item"] = parent_item
    parent_row["Item"] = parent_item
    parent_row["Qty_per_parent"] = 1.0
    parent_row["IsParent"] = True
    if comp_rows:
        return pd.concat([pd.DataFrame(comp_rows), pd.DataFrame([parent_row])], ignore_index=True)
    return pd.DataFrame([parent_row])


def expand_nav_preinstalled(nav: pd.DataFrame) -> pd.DataFrame:
    nav = nav.copy()
    for col in ["Pre/Bare", "Qty(+)", "Item"]:
        if col not in nav.columns:
            raise ValueError(f"NAV must contain '{col}' column.")
    if "Description" not in nav.columns:
        nav["Description"] = ""

    nav["Description"] = nav["Description"].astype(str).apply(clean_space)
    pre_mask = nav["Pre/Bare"].astype(str).str.strip().str.casefold().eq("pre")
    nav_pre = nav.loc[pre_mask].copy()
    nav_other = nav.loc[~pre_mask].copy()

    expanded_parts = [expand_preinstalled_row(r) for _, r in nav_pre.iterrows()]
    expanded_pre = pd.concat(expanded_parts, ignore_index=True) if expanded_parts else nav_pre.copy()

    needed_cols = list(nav.columns) + ["Parent_Item", "Qty_per_parent", "IsParent"]
    expanded_pre = expanded_pre.reindex(columns=needed_cols, fill_value=pd.NA)
    nav_other = nav_other.reindex(columns=needed_cols, fill_value=pd.NA)
    nav_other.loc[:, "Parent_Item"] = nav_other["Item"]
    nav_other.loc[:, "Qty_per_parent"] = 1.0
    nav_other.loc[:, "IsParent"] = True

    expanded_all = pd.concat([expanded_pre, nav_other], ignore_index=True)
    expanded_all = _split_special_shipping_variants(expanded_all)
    expanded_all["Qty(+)"] = pd.to_numeric(expanded_all["Qty(+)"], errors="coerce").fillna(0.0)
    expanded_all["Qty_per_parent"] = pd.to_numeric(expanded_all["Qty_per_parent"], errors="coerce").fillna(1.0)
    expanded_all["IsParent"] = expanded_all["IsParent"].astype(bool)
    expanded_all["Date"] = pd.to_datetime(expanded_all["Ship Date"], errors="coerce") + pd.Timedelta(days=5)
    expanded_all["Item"] = expanded_all["Item"].astype(str).map(normalize_item)
    return expanded_all


def build_opening_stock(so: pd.DataFrame, inventory: pd.DataFrame | None = None) -> pd.DataFrame:
    if inventory is not None and not inventory.empty:
        inv = inventory.copy()
        item_col = "Part_Number" if "Part_Number" in inv.columns else ("Item" if "Item" in inv.columns else None)
        qty_col = "On Hand" if "On Hand" in inv.columns else ("On Hand - WIP" if "On Hand - WIP" in inv.columns else None)
        if item_col is not None and qty_col is not None:
            stock = (
                inv[[item_col, qty_col]]
                .rename(columns={item_col: "Item", qty_col: "Opening"})
                .dropna(subset=["Item"])
                .copy()
            )
            stock["Item"] = stock["Item"].astype(str).str.strip()
            stock["Opening"] = pd.to_numeric(stock["Opening"], errors="coerce").fillna(0.0)
            stock = stock.loc[stock["Item"].ne("")]
            stock["Item"] = _norm_key(stock["Item"].map(normalize_item))
            return stock.groupby("Item", as_index=False)["Opening"].sum().sort_values("Item", kind="mergesort").reset_index(drop=True)

    src = so.copy()
    if "On Hand" not in src.columns:
        src["On Hand"] = 0.0
    stock = src[["Item", "On Hand"]].dropna().drop_duplicates(subset=["Item"], keep="last").rename(columns={"On Hand": "Opening"})
    stock["Item"] = _norm_key(stock["Item"])
    return stock


def _order_events(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.normalize()
    out["Delta"] = pd.to_numeric(out["Delta"], errors="coerce")
    kind_cat = CategoricalDtype(categories=["OPEN", "IN", "ADJ", "OUT"], ordered=True)
    out["Kind"] = out["Kind"].astype(kind_cat)
    if not {"Date", "Item", "Delta", "Kind"}.issubset(out.columns):
        raise ValueError("events must have columns: ['Date','Item','Delta','Kind']")
    out = out.dropna(subset=["Date", "Item"]).loc[out["Delta"].notna()]
    zero_mask = out["Delta"].eq(0)
    keep_zero_open = zero_mask & out["Kind"].astype(str).eq("OPEN")
    out = out.loc[out["Delta"].ne(0) | keep_zero_open].copy()
    out.sort_values(["Item", "Date", "Kind"], inplace=True, kind="mergesort")
    out.reset_index(drop=True, inplace=True)
    return out


def build_events(so: pd.DataFrame, nav_exp: pd.DataFrame, pod: pd.DataFrame | None = None) -> pd.DataFrame:
    so = _norm_cols(so)
    nav = _norm_cols(nav_exp)

    nav_keep = ["Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"]
    for c in nav_keep:
        if c not in nav.columns:
            nav[c] = pd.NA
    inbound = nav.loc[nav["Qty(+)"] > 0, nav_keep].rename(columns={"Qty(+)": "Delta"}).assign(Kind="IN", Source="NAV")
    inbound["Item_raw"] = inbound["Item"]
    inbound["Item"] = _norm_key(inbound["Item"])

    outbound = (
        so.loc[so["Qty(-)"] > 0, ["Ship Date", "Item", "Qty(-)", "QB Num", "P. O. #", "Name"]]
        .rename(columns={"Ship Date": "Date", "Qty(-)": "Delta"})
        .assign(Kind="OUT", Source="SO")
    )
    outbound["Item_raw"] = outbound["Item"]
    outbound["Item"] = _norm_key(outbound["Item"])
    outbound["Delta"] = -outbound["Delta"]

    cols = ["Date", "Item", "Delta", "Kind", "Source", "QB Num", "P. O. #", "Name", "Item_raw"]
    inbound = inbound.reindex(columns=cols)
    outbound = outbound.reindex(columns=cols)

    pod_events = pd.DataFrame(columns=cols)
    if pod is not None and not pod.empty:
        pod = _norm_cols(pod)
        if "Source Name" in pod.columns:
            pod = pod.loc[~pod["Source Name"].astype(str).str.strip().isin(EXCLUDED_POD_SOURCE_NAMES)].copy()
        if "Ship Date" not in pod.columns and "Deliv Date" in pod.columns:
            pod["Ship Date"] = pod["Deliv Date"]
        pod_keep = ["Ship Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"]
        for c in pod_keep:
            if c not in pod.columns:
                pod[c] = pd.NA
        pod_events = pod.loc[pod["Qty(+)"] > 0, pod_keep].rename(columns={"Ship Date": "Date", "Qty(+)": "Delta"}).assign(Kind="IN", Source="POD")
        pod_events["Item_raw"] = pod_events["Item"]
        pod_events["Item"] = _norm_key(pod_events["Item"])
        pod_events = pod_events.reindex(columns=cols)

    events = pd.concat([inbound, pod_events, outbound], ignore_index=True, sort=False)
    excluded_pods = {str(k).strip() for k in POD_SITE.keys() if str(k).strip()}
    if excluded_pods:
        event_pod_no = events["QB Num"].fillna("").astype(str).str.strip()
        blank_mask = event_pod_no.eq("")
        event_pod_no = event_pod_no.mask(blank_mask, events["P. O. #"].fillna("").astype(str).str.strip())
        inbound_mask = events["Kind"].astype(str).eq("IN")
        events = events.loc[~(inbound_mask & event_pod_no.isin(excluded_pods))].copy()
    return _order_events(events)


def build_reconcile_events(
    inv_db: pd.DataFrame,
    inv_wh: pd.DataFrame,
    *,
    as_of: pd.Timestamp | None = None,
    item_col_db: str = "Part_Number",
    item_col_wh: str = "Part_Number",
    onhand_col: str = "On Hand",
    mappings: dict | None = None,
    min_abs_delta: float = 0.0,
) -> pd.DataFrame:
    as_of = (as_of or pd.Timestamp.today()).normalize()
    adj_date = as_of - pd.Timedelta(days=1)
    db = inv_db.copy()
    wh = inv_wh.copy()
    if item_col_db not in db.columns:
        raise ValueError(f"inv_db missing column: {item_col_db}")
    if item_col_wh not in wh.columns:
        raise ValueError(f"inv_wh missing column: {item_col_wh}")

    def _apply_normalizer(val: str) -> str:
        base = normalize_item(val)
        return mappings.get(base, base) if mappings else base

    db["Item"] = db[item_col_db].astype(str).str.strip().map(_apply_normalizer)
    wh["Item"] = wh[item_col_wh].astype(str).str.strip().map(_apply_normalizer)
    db[onhand_col] = pd.to_numeric(db.get(onhand_col, 0), errors="coerce").fillna(0.0)
    wh[onhand_col] = pd.to_numeric(wh.get(onhand_col, 0), errors="coerce").fillna(0.0)
    db_agg = db.groupby("Item", as_index=False, sort=False)[onhand_col].sum().rename(columns={onhand_col: "OnHand_DB"})
    wh_agg = wh.groupby("Item", as_index=False, sort=False)[onhand_col].sum().rename(columns={onhand_col: "OnHand_WH"})
    merged = db_agg.merge(wh_agg, on="Item", how="outer", validate="1:1")
    merged["OnHand_DB"] = pd.to_numeric(merged["OnHand_DB"], errors="coerce").fillna(0.0)
    merged["OnHand_WH"] = pd.to_numeric(merged["OnHand_WH"], errors="coerce").fillna(0.0)
    merged["Delta"] = merged["OnHand_WH"] - merged["OnHand_DB"]
    if min_abs_delta > 0:
        merged = merged.loc[merged["Delta"].abs() >= float(min_abs_delta)]
    if merged.empty:
        return pd.DataFrame(columns=["Date", "Item", "Delta", "Kind", "Source", "Notes"])
    out = merged.loc[:, ["Item", "Delta", "OnHand_DB", "OnHand_WH"]].copy()
    out.insert(0, "Date", adj_date)
    out["Kind"] = "ADJ"
    out["Source"] = "Reconcile"
    out["Notes"] = "InvRecon: WH(" + out["OnHand_WH"].astype(str) + ") - DB(" + out["OnHand_DB"].astype(str) + ") = " + out["Delta"].astype(str)
    return out.loc[:, ["Date", "Item", "Delta", "Kind", "Source", "Notes"]]


__all__ = [
    "_order_events",
    "build_events",
    "build_opening_stock",
    "build_reconcile_events",
    "clean_space",
    "expand_nav_preinstalled",
    "parse_component_token",
    "parse_description",
]
