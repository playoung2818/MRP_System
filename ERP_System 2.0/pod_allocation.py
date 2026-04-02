from __future__ import annotations

import pandas as pd

from erp_system.contracts import TABLE_CONTRACTS, ensure_contract_columns


ALLOCATION_COLUMNS = [
    "Ship Date",
    "Name",
    "QB Num",
    "Item",
    "Qty(-)",
    "Pre/Bare",
    "POD#",
    "Customer expected Date",
]

MANUAL_COLUMNS = ["Pre/Bare", "POD#", "Customer expected Date"]


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
        "Ship Date",
        "Name",
        "QB Num",
        "Item",
        "Pre/Bare",
        "POD#",
        "Customer expected Date",
    ]
    for col in string_cols:
        out[col] = out[col].fillna("").astype(str).str.strip()

    out["Qty(-)"] = pd.to_numeric(out["Qty(-)"], errors="coerce").fillna(0.0)
    return out[ALLOCATION_COLUMNS].copy()


def _add_row_key(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        out = frame.copy()
        out["row_key"] = pd.Series(dtype="string")
        return out

    out = frame.copy()
    out["__ord__"] = (
        out["Ship Date"].astype(str)
        + "|"
        + out["Name"].astype(str)
        + "|"
        + out["Qty(-)"].astype(str)
    )
    out = out.sort_values(["QB Num", "Item", "__ord__"], kind="mergesort").reset_index(drop=True)
    out["__occ__"] = out.groupby(["QB Num", "Item"], sort=False).cumcount() + 1
    out["row_key"] = (
        out["QB Num"].astype(str).str.strip()
        + "|"
        + out["Item"].astype(str).str.strip()
        + "|"
        + out["__occ__"].astype(int).astype(str)
    )
    return out.drop(columns=["__ord__", "__occ__"])


def build_pod_allocation_table(
    structured: pd.DataFrame,
    pod: pd.DataFrame,
    *,
    current_allocation: pd.DataFrame | None = None,
    run_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    del pod, run_ts

    current_df = _prepare_current_allocation(current_allocation)

    if structured is None or structured.empty:
        return _empty_pod_allocation()

    src = ensure_contract_columns(
        structured,
        TABLE_CONTRACTS["wo_structured"],
        extra_columns=("Name",),
    )

    src["Ship Date"] = pd.to_datetime(src["Ship Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    src["Name"] = src["Name"].fillna("").astype(str).str.strip()
    src["QB Num"] = src["QB Num"].fillna("").astype(str).str.strip()
    src["Item"] = src["Item"].fillna("").astype(str).str.strip()
    src["Qty(-)"] = pd.to_numeric(src["Qty(-)"], errors="coerce").fillna(0.0)

    src = src.loc[src["QB Num"].ne("") & src["Item"].ne("") & src["Qty(-)"].gt(0)].copy()
    if src.empty:
        return _empty_pod_allocation()

    out = src[["Ship Date", "Name", "QB Num", "Item", "Qty(-)"]].copy()
    out = _add_row_key(out)
    out["Pre/Bare"] = ""
    out["POD#"] = ""
    out["Customer expected Date"] = ""

    if not current_df.empty:
        current_df = _add_row_key(current_df)
        carry = current_df[["row_key"] + MANUAL_COLUMNS].drop_duplicates(subset=["row_key"], keep="last")
        out = out.merge(carry, on="row_key", how="left", suffixes=("", "_old"))
        for col in MANUAL_COLUMNS:
            old_col = f"{col}_old"
            if old_col not in out.columns:
                continue
            out[col] = out[old_col].fillna(out[col]).fillna("").astype(str).str.strip()
            out.drop(columns=[old_col], inplace=True)

    return out.sort_values(["QB Num", "Item", "Ship Date", "Name", "Qty(-)"], kind="mergesort")[
        ALLOCATION_COLUMNS
    ].reset_index(drop=True)
