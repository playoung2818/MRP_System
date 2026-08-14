from __future__ import annotations

import pandas as pd
from pandas.api.types import CategoricalDtype

from erp_system.normalize.erp_normalize import POD_SITE, normalize_item
from erp_system.runtime.policies import EXCLUDED_POD_SOURCE_NAMES
from erp_system.transform.common import _norm_cols, _norm_key


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


def build_events(so: pd.DataFrame, sap_exp: pd.DataFrame, pod: pd.DataFrame | None = None) -> pd.DataFrame:
    so = _norm_cols(so)
    sap = _norm_cols(sap_exp)

    sap_keep = ["Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"]
    for c in sap_keep:
        if c not in sap.columns:
            sap[c] = pd.NA
    inbound = sap.loc[sap["Qty(+)"] > 0, sap_keep].rename(columns={"Qty(+)": "Delta"}).assign(Kind="IN", Source="HQ")
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
        pod_events = pod.loc[pod["Qty(+)"] > 0, pod_keep].rename(columns={"Ship Date": "Date", "Qty(+)": "Delta"}).assign(Kind="IN", Source="USA")
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
]
