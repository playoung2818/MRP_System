from __future__ import annotations

import pandas as pd
from pandas.api.types import CategoricalDtype

from mrp_system.normalize.mrp_normalize import POD_SITE, normalize_item
from mrp_system.runtime.policies import EXCLUDED_POD_SOURCE_NAMES
from mrp_system.transform.common import _norm_cols, _norm_key


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
    """Assemble movements and exclusions; ledger construction cleans/orders them."""
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
        # Protect direct callers and stale snapshots as well as transformed PODs.
        items = pod.get("Item", pd.Series(pd.NA, index=pod.index, dtype="string")).astype("string").str.strip()
        valid_item = items.notna() & ~items.str.lower().isin({"", "nan", "none", "<na>", "nat"})
        valid_item &= ~items.str.match(r"(?i)^total\b", na=False)
        pod = pod.loc[valid_item].copy()
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
    return events


__all__ = [
    "_order_events",
    "build_events",
    "build_opening_stock",
]
