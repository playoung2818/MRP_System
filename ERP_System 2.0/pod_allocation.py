from __future__ import annotations

import pandas as pd

from core import _norm_key
from erp_normalize import normalize_item


ALLOCATION_COLUMNS = [
    "row_key",
    "occurrence_index",
    "Order Date",
    "Name",
    "QB Num",
    "Item",
    "Component_Status",
    "Qty(-)",
    "Pre/Bare",
    "Current Ship Date",
    "candidate_pod_list",
    "candidate_pod_qty",
    "POD#",
    "allocated_qty",
    "allocation_type",
    "locked",
    "reason",
    "note",
    "source_run_date",
    "updated_at",
]

MANUAL_COLUMNS = ["POD#", "allocated_qty", "allocation_type", "locked", "reason", "note"]


def _normalize_item_key(item: object) -> str:
    series = pd.Series([item], dtype="string").map(normalize_item)
    return str(_norm_key(series).iloc[0])


def _empty_pod_allocation() -> pd.DataFrame:
    return pd.DataFrame(columns=ALLOCATION_COLUMNS)


def _prepare_current_allocation(current_allocation: pd.DataFrame | None) -> pd.DataFrame:
    if current_allocation is None or current_allocation.empty:
        return _empty_pod_allocation()

    out = current_allocation.copy()
    for col in ALLOCATION_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    string_cols = [
        "row_key",
        "Order Date",
        "Name",
        "QB Num",
        "Item",
        "Component_Status",
        "Pre/Bare",
        "Current Ship Date",
        "candidate_pod_list",
        "POD#",
        "allocation_type",
        "reason",
        "note",
        "source_run_date",
    ]
    for col in string_cols:
        out[col] = out[col].fillna("").astype(str).str.strip()

    out["occurrence_index"] = pd.to_numeric(out["occurrence_index"], errors="coerce").fillna(1).astype(int)
    out["Qty(-)"] = pd.to_numeric(out["Qty(-)"], errors="coerce").fillna(0.0)
    out["candidate_pod_qty"] = pd.to_numeric(out["candidate_pod_qty"], errors="coerce").fillna(0.0)
    out["allocated_qty"] = pd.to_numeric(out["allocated_qty"], errors="coerce").fillna(0.0)
    out["locked"] = out["locked"].fillna(False).astype(bool)
    out["updated_at"] = pd.to_datetime(out["updated_at"], errors="coerce")
    return out[ALLOCATION_COLUMNS].copy()


def _build_candidate_lookup(pod: pd.DataFrame) -> pd.DataFrame:
    if pod is None or pod.empty:
        return pd.DataFrame(columns=["item_key", "candidate_pod_list", "candidate_pod_qty"])

    src = pod.copy()
    for col in ["Item", "POD#", "Qty(+)", "Ship Date"]:
        if col not in src.columns:
            src[col] = pd.NA

    src["POD#"] = src["POD#"].fillna("").astype(str).str.strip()
    src["Item"] = src["Item"].fillna("").astype(str).str.strip()
    src["item_key"] = src["Item"].map(_normalize_item_key)
    src["qty"] = pd.to_numeric(src["Qty(+)"], errors="coerce").fillna(0.0)
    src["Ship Date"] = pd.to_datetime(src["Ship Date"], errors="coerce")
    src["ship_date_label"] = src["Ship Date"].dt.strftime("%Y-%m-%d").fillna("TBD")
    src = src.loc[src["POD#"].ne("") & src["item_key"].ne("") & src["qty"].gt(0)].copy()
    if src.empty:
        return pd.DataFrame(columns=["item_key", "candidate_pod_list", "candidate_pod_qty"])

    pod_rows = (
        src.groupby(["item_key", "POD#", "ship_date_label"], as_index=False)["qty"]
        .sum()
        .sort_values(["item_key", "ship_date_label", "POD#"], kind="mergesort")
    )
    pod_rows["pod_label"] = pod_rows.apply(
        lambda r: f'{r["POD#"]} ({float(r["qty"]):g}, {r["ship_date_label"]})',
        axis=1,
    )

    list_df = (
        pod_rows.groupby("item_key", as_index=False)["pod_label"]
        .agg(", ".join)
        .rename(columns={"pod_label": "candidate_pod_list"})
    )
    qty_df = (
        pod_rows.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "candidate_pod_qty"})
    )
    return list_df.merge(qty_df, on="item_key", how="outer")


def build_pod_allocation_table(
    structured: pd.DataFrame,
    pod: pd.DataFrame,
    *,
    current_allocation: pd.DataFrame | None = None,
    run_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    run_ts = pd.Timestamp.now() if run_ts is None else pd.to_datetime(run_ts)
    current_df = _prepare_current_allocation(current_allocation)

    if structured is None or structured.empty:
        return _empty_pod_allocation()

    src = structured.copy()
    for col in ["Order Date", "Name", "QB Num", "Item", "Component_Status", "Qty(-)", "Pre/Bare", "Ship Date"]:
        if col not in src.columns:
            src[col] = pd.NA

    src["Order Date"] = pd.to_datetime(src["Order Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    src["Current Ship Date"] = pd.to_datetime(src["Ship Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    src["Name"] = src["Name"].fillna("").astype(str).str.strip()
    src["QB Num"] = src["QB Num"].fillna("").astype(str).str.strip()
    src["Item"] = src["Item"].fillna("").astype(str).str.strip()
    src["Component_Status"] = src["Component_Status"].fillna("").astype(str).str.strip()
    src["Pre/Bare"] = src["Pre/Bare"].fillna("").astype(str).str.strip()
    src["Qty(-)"] = pd.to_numeric(src["Qty(-)"], errors="coerce").fillna(0.0)

    src = src.loc[src["QB Num"].ne("") & src["Item"].ne("") & src["Qty(-)"].gt(0)].copy()
    if src.empty:
        return _empty_pod_allocation()

    src = src.sort_values(
        ["QB Num", "Item", "Order Date", "Name", "Component_Status", "Qty(-)"],
        kind="mergesort",
    ).reset_index(drop=True)
    src["occurrence_index"] = src.groupby(["QB Num", "Item"], sort=False).cumcount() + 1
    src["row_key"] = (
        src["QB Num"].astype(str)
        + "|"
        + src["Item"].astype(str)
        + "|"
        + src["occurrence_index"].astype(int).astype(str)
    )

    candidate_lookup = _build_candidate_lookup(pod)
    src["item_key"] = src["Item"].map(_normalize_item_key)
    src = src.merge(candidate_lookup, on="item_key", how="left")
    src["candidate_pod_list"] = src["candidate_pod_list"].fillna("")
    src["candidate_pod_qty"] = pd.to_numeric(src["candidate_pod_qty"], errors="coerce").fillna(0.0)

    out = src[
        [
            "row_key",
            "occurrence_index",
            "Order Date",
            "Name",
            "QB Num",
            "Item",
            "Component_Status",
            "Qty(-)",
            "Pre/Bare",
            "Current Ship Date",
            "candidate_pod_list",
            "candidate_pod_qty",
        ]
    ].copy()
    out["POD#"] = ""
    out["allocated_qty"] = 0.0
    out["allocation_type"] = ""
    out["locked"] = False
    out["reason"] = ""
    out["note"] = ""
    out["source_run_date"] = run_ts.normalize().strftime("%Y-%m-%d")
    out["updated_at"] = run_ts

    if not current_df.empty:
        carry = current_df[["row_key"] + MANUAL_COLUMNS].drop_duplicates(subset=["row_key"], keep="last")
        out = out.merge(carry, on="row_key", how="left", suffixes=("", "_old"))
        for col in MANUAL_COLUMNS:
            old_col = f"{col}_old"
            if old_col not in out.columns:
                continue
            if col == "locked":
                out[col] = out[old_col].fillna(out[col]).fillna(False).astype(bool)
            elif col == "allocated_qty":
                out[col] = pd.to_numeric(out[old_col], errors="coerce").fillna(out[col]).fillna(0.0)
            else:
                out[col] = out[old_col].fillna(out[col]).fillna("").astype(str).str.strip()
            out.drop(columns=[old_col], inplace=True)

    out["allocation_type"] = out["allocation_type"].fillna("").astype(str).str.strip()
    out.loc[out["allocation_type"].eq("") & out["Pre/Bare"].astype(str).str.upper().eq("PRE"), "allocation_type"] = "hard"
    out.loc[out["reason"].eq("") & out["Pre/Bare"].astype(str).str.upper().eq("PRE"), "reason"] = "preinstall_default"
    out.loc[out["locked"].eq(False) & out["Pre/Bare"].astype(str).str.upper().eq("PRE"), "locked"] = True
    out.loc[out["allocated_qty"].eq(0) & out["Pre/Bare"].astype(str).str.upper().eq("PRE"), "allocated_qty"] = out["Qty(-)"]

    return out[ALLOCATION_COLUMNS].sort_values(
        ["QB Num", "Item", "occurrence_index"],
        kind="mergesort",
    ).reset_index(drop=True)
