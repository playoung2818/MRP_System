from __future__ import annotations

import pandas as pd

from atp import earliest_atp_strict
from core import _norm_key
from erp_normalize import normalize_item


def _normalize_item_key(item: str) -> str:
    series = pd.Series([item], dtype="string").map(normalize_item)
    return str(_norm_key(series).iloc[0])


def _build_adjusted_item_atp(
    ledger: pd.DataFrame,
    *,
    qb_num: str,
    item: str,
    from_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Build ATP view for an existing SO item by removing that SO's own demand rows
    from the ledger first, then recomputing projected NAV.
    """
    led = ledger.copy()
    if led.empty or "Delta" not in led.columns or "Date" not in led.columns:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])

    normalized_item = _normalize_item_key(item)
    if "Item" not in led.columns:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])
    mask_item = _norm_key(led["Item"]).astype(str) == normalized_item
    item_df = led.loc[mask_item].copy()
    if item_df.empty:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])

    opening_series = pd.to_numeric(item_df.get("Opening"), errors="coerce").dropna()
    opening = float(opening_series.iloc[0]) if not opening_series.empty else 0.0

    so_col = item_df.get("QB Num", pd.Series("", index=item_df.index)).astype(str)
    kind_col = item_df.get("Kind", pd.Series("", index=item_df.index)).astype(str)
    remove_mask = so_col.eq(str(qb_num)) & kind_col.eq("OUT")

    adjusted = item_df.loc[~remove_mask].copy()
    if adjusted.empty:
        base = pd.DataFrame(
            {
                "Item_raw": [item],
                "Item": [item],
                "Date": [from_date],
                "Projected_NAV": [opening],
            }
        )
        base["FutureMin_NAV"] = base["Projected_NAV"]
        return base[["Item", "Date", "Projected_NAV", "FutureMin_NAV"]]

    adjusted["Date"] = pd.to_datetime(adjusted["Date"], errors="coerce")
    adjusted["Delta"] = pd.to_numeric(adjusted["Delta"], errors="coerce").fillna(0.0)
    adjusted = adjusted.loc[adjusted["Date"].notna()].copy()
    adjusted = adjusted.sort_values("Date", kind="mergesort").reset_index(drop=True)
    adjusted["Projected_NAV"] = opening + adjusted["Delta"].cumsum()
    projected = pd.to_numeric(adjusted["Projected_NAV"], errors="coerce").tolist()
    future_min: list[float] = [0.0] * len(projected)
    current_min = float("inf")
    for idx in range(len(projected) - 1, -1, -1):
        value = projected[idx]
        if pd.notna(value):
            current_min = min(current_min, float(value))
        future_min[idx] = current_min

    item_name = str(adjusted["Item"].iloc[0])
    return pd.DataFrame(
        {
            "Item": item_name,
            "Date": adjusted["Date"].tolist(),
            "Projected_NAV": projected,
            "FutureMin_NAV": future_min,
        }
    )


def _earliest_assignment_date_before_cutoff(
    ledger: pd.DataFrame,
    *,
    qb_num: str,
    item: str,
    qty: float,
    from_date: pd.Timestamp,
    cutoff: pd.Timestamp,
) -> pd.Timestamp | None:
    adj_atp = _build_adjusted_item_atp(ledger, qb_num=qb_num, item=item, from_date=from_date)
    if adj_atp.empty:
        return None

    scoped = adj_atp.loc[pd.to_datetime(adj_atp["Date"], errors="coerce") < cutoff].copy()
    if scoped.empty:
        return None

    scoped["Date"] = pd.to_datetime(scoped["Date"], errors="coerce")
    scoped["Projected_NAV"] = pd.to_numeric(scoped["Projected_NAV"], errors="coerce")
    projected = scoped["Projected_NAV"].tolist()
    future_min: list[float] = [0.0] * len(projected)
    current_min = float("inf")
    for idx in range(len(projected) - 1, -1, -1):
        value = projected[idx]
        if pd.notna(value):
            current_min = min(current_min, float(value))
        future_min[idx] = current_min
    scoped["FutureMin_NAV"] = future_min
    return earliest_atp_strict(
        scoped[["Item", "Date", "Projected_NAV", "FutureMin_NAV"]],
        _normalize_item_key(item),
        qty,
        from_date=from_date,
        allow_zero=True,
    )


def _earliest_assignment_date_for_mode(
    ledger: pd.DataFrame,
    *,
    qb_num: str,
    item: str,
    qty: float,
    from_date: pd.Timestamp,
    cutoff: pd.Timestamp,
    include_cutoff_in_check: bool,
) -> pd.Timestamp | None:
    adj_atp = _build_adjusted_item_atp(ledger, qb_num=qb_num, item=item, from_date=from_date)
    if adj_atp.empty:
        return None

    date_series = pd.to_datetime(adj_atp["Date"], errors="coerce")
    if include_cutoff_in_check:
        scoped = adj_atp.loc[date_series <= cutoff].copy()
    else:
        scoped = adj_atp.loc[date_series < cutoff].copy()
    if scoped.empty:
        return None

    scoped["Date"] = pd.to_datetime(scoped["Date"], errors="coerce")
    scoped["Projected_NAV"] = pd.to_numeric(scoped["Projected_NAV"], errors="coerce")
    projected = scoped["Projected_NAV"].tolist()
    future_min: list[float] = [0.0] * len(projected)
    current_min = float("inf")
    for idx in range(len(projected) - 1, -1, -1):
        value = projected[idx]
        if pd.notna(value):
            current_min = min(current_min, float(value))
        future_min[idx] = current_min
    scoped["FutureMin_NAV"] = future_min

    candidates = scoped.loc[scoped["Date"] < cutoff].copy()
    if candidates.empty:
        return None
    return earliest_atp_strict(
        candidates[["Item", "Date", "Projected_NAV", "FutureMin_NAV"]],
        _normalize_item_key(item),
        qty,
        from_date=from_date,
        allow_zero=True,
    )


def _build_assignment_readiness_for_mode(
    structured: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    from_date: pd.Timestamp | None = None,
    cutoff_date: str = "2099-07-04",
    mode: str = "loose",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    summary_cols = [
        "QB Num",
        "Name",
        "P. O. #",
        "Order Date",
        "Current Ship Date",
        "Item Count",
        "Ready to be assigned",
        "Earliest Ready Date",
        "Blocking Item Count",
        "Blocking Items",
        "Waiting / Shortage Items",
    ]
    blocker_cols = [
        "QB Num",
        "Name",
        "P. O. #",
        "Order Date",
        "Current Ship Date",
        "Item",
        "Required Qty",
        "Earliest Feasible Date",
        "Block Reason",
    ]

    if structured is None or structured.empty or ledger is None or ledger.empty:
        return pd.DataFrame(columns=summary_cols), pd.DataFrame(columns=blocker_cols)

    cutoff = pd.Timestamp(cutoff_date).normalize()
    start_date = pd.Timestamp.today().normalize() if from_date is None else pd.to_datetime(from_date).normalize()
    mode_norm = str(mode).strip().lower()
    include_cutoff_in_check = mode_norm == "strict"

    so = structured.copy()
    for c in ["QB Num", "Name", "P. O. #", "Order Date", "Ship Date", "Item", "Qty(-)", "Component_Status"]:
        if c not in so.columns:
            so[c] = pd.NA

    so["QB Num"] = so["QB Num"].astype(str).str.strip()
    so["Name"] = so["Name"].astype(str).str.strip()
    so["P. O. #"] = so["P. O. #"].astype(str).str.strip()
    so["Item"] = so["Item"].astype(str).str.strip()
    so["Qty(-)"] = pd.to_numeric(so["Qty(-)"], errors="coerce").fillna(0.0)
    so["Ship Date"] = pd.to_datetime(so["Ship Date"], errors="coerce").dt.normalize()
    so["Order Date"] = pd.to_datetime(so["Order Date"], errors="coerce")

    pending = so.loc[
        so["Ship Date"].eq(cutoff)
        & so["QB Num"].ne("")
        & so["Item"].ne("")
        & so["Qty(-)"].gt(0)
    ].copy()
    if pending.empty:
        return pd.DataFrame(columns=summary_cols), pd.DataFrame(columns=blocker_cols)

    summary_rows: list[dict[str, object]] = []
    blocker_rows: list[dict[str, object]] = []

    for qb_num, grp in pending.groupby("QB Num", sort=True):
        demands_df = (
            grp.groupby("Item", as_index=False)["Qty(-)"]
            .sum()
            .sort_values("Item", kind="mergesort")
        )
        demand_map = {str(r["Item"]): float(r["Qty(-)"]) for _, r in demands_df.iterrows()}
        first = grp.iloc[0]

        item_dates: dict[str, pd.Timestamp] = {}
        for item, qty in demand_map.items():
            dt = _earliest_assignment_date_for_mode(
                ledger,
                qb_num=str(qb_num),
                item=item,
                qty=qty,
                from_date=start_date,
                cutoff=cutoff,
                include_cutoff_in_check=include_cutoff_in_check,
            )
            if dt is not None:
                item_dates[item] = dt

        blocking_items: list[str] = []
        for item, qty in demand_map.items():
            dt = item_dates.get(item)
            if dt is None:
                blocking_items.append(item)
                blocker_rows.append(
                    {
                        "QB Num": str(qb_num),
                        "Name": str(first.get("Name") or ""),
                        "P. O. #": str(first.get("P. O. #") or ""),
                        "Order Date": first["Order Date"].strftime("%Y-%m-%d") if pd.notna(first["Order Date"]) else "",
                        "Current Ship Date": cutoff.strftime("%Y-%m-%d"),
                        "Item": item,
                        "Required Qty": qty,
                        "Earliest Feasible Date": None,
                        "Block Reason": (
                            "No feasible ATP date after removing this SO's placeholder demand"
                            if not include_cutoff_in_check
                            else "No feasible ATP date before placeholder date under strict check"
                        ),
                    }
                )
            elif dt > cutoff:
                blocking_items.append(item)
                blocker_rows.append(
                    {
                        "QB Num": str(qb_num),
                        "Name": str(first.get("Name") or ""),
                        "P. O. #": str(first.get("P. O. #") or ""),
                        "Order Date": first["Order Date"].strftime("%Y-%m-%d") if pd.notna(first["Order Date"]) else "",
                        "Current Ship Date": cutoff.strftime("%Y-%m-%d"),
                        "Item": item,
                        "Required Qty": qty,
                        "Earliest Feasible Date": dt.strftime("%Y-%m-%d"),
                        "Block Reason": "Feasible only after placeholder date",
                    }
                )

        ready_dt = None
        if item_dates:
            adjusted_dates = {item: item_dates[item] for item in demand_map.keys() if item in item_dates}
            if len(adjusted_dates) == len(demand_map):
                ready_dt = max(adjusted_dates.values()) if adjusted_dates else None

        is_ready = ready_dt is not None and ready_dt <= cutoff and not blocking_items

        waiting_items = sorted(
            set(
                grp.loc[grp["Component_Status"].isin(["Waiting", "Shortage"]), "Item"]
                .dropna()
                .astype(str)
                .str.strip()
                .loc[lambda s: s.ne("")]
                .tolist()
            )
        )

        summary_rows.append(
            {
                "QB Num": str(qb_num),
                "Name": str(first.get("Name") or ""),
                "P. O. #": str(first.get("P. O. #") or ""),
                "Order Date": first["Order Date"].strftime("%Y-%m-%d") if pd.notna(first["Order Date"]) else "",
                "Current Ship Date": cutoff.strftime("%Y-%m-%d"),
                "Item Count": int(len(demand_map)),
                "Ready to be assigned": bool(is_ready),
                "Earliest Ready Date": ready_dt.strftime("%Y-%m-%d") if ready_dt is not None else None,
                "Blocking Item Count": int(len(blocking_items)),
                "Blocking Items": ", ".join(sorted(blocking_items)),
                "Waiting / Shortage Items": ", ".join(waiting_items),
            }
        )

    summary_df = pd.DataFrame(summary_rows, columns=summary_cols).sort_values(
        ["Ready to be assigned", "Earliest Ready Date", "QB Num"],
        ascending=[False, True, True],
        kind="mergesort",
    )
    blocker_df = pd.DataFrame(blocker_rows, columns=blocker_cols).sort_values(
        ["QB Num", "Item"], kind="mergesort"
    )
    return summary_df.reset_index(drop=True), blocker_df.reset_index(drop=True)


def build_assignment_readiness_reports(
    structured: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    from_date: pd.Timestamp | None = None,
    cutoff_date: str = "2099-07-04",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return _build_assignment_readiness_for_mode(
        structured,
        ledger,
        from_date=from_date,
        cutoff_date=cutoff_date,
        mode="loose",
    )


REFERENCE_BASE_COLUMNS = [
    "row_key",
    "occurrence_index",
    "Order Date",
    "Name",
    "QB Num",
    "Item",
    "Component_Status",
    "Qty(-)",
    "Pre/Bare",
    "POD#",
    "POD ship date",
    "Expected Date",
    "source_run_date",
    "stage_run_ts",
]
STAGE_EXTRA_COLUMNS = ["forwarded_from_main", "forward_match_type", "review_status", "review_note"]
MAIN_EXTRA_COLUMNS = ["is_active", "archived_at", "archive_reason", "approved_at", "updated_at"]
DIFF_COLUMNS = [
    "row_key",
    "change_type",
    "changed_fields",
    "QB Num",
    "Item",
    "main_Order Date",
    "stage_Order Date",
    "main_Name",
    "stage_Name",
    "main_Component_Status",
    "stage_Component_Status",
    "main_Qty(-)",
    "stage_Qty(-)",
    "main_Pre/Bare",
    "stage_Pre/Bare",
    "main_POD#",
    "stage_POD#",
    "main_POD ship date",
    "stage_POD ship date",
    "main_Expected Date",
    "stage_Expected Date",
    "source_run_date",
]


def _empty_reference_stage() -> pd.DataFrame:
    return pd.DataFrame(columns=REFERENCE_BASE_COLUMNS + STAGE_EXTRA_COLUMNS)


def _empty_reference_main() -> pd.DataFrame:
    return pd.DataFrame(columns=REFERENCE_BASE_COLUMNS + MAIN_EXTRA_COLUMNS)


def _empty_reference_diff() -> pd.DataFrame:
    return pd.DataFrame(columns=DIFF_COLUMNS)


def _normalize_reference_source(structured: pd.DataFrame, run_ts: pd.Timestamp) -> pd.DataFrame:
    if structured is None or structured.empty:
        return _empty_reference_stage()

    src = structured.copy()
    for col in ["Order Date", "Name", "QB Num", "Item", "Component_Status", "Qty(-)", "Pre/Bare"]:
        if col not in src.columns:
            src[col] = pd.NA

    src["Order Date"] = pd.to_datetime(src["Order Date"], errors="coerce").dt.strftime("%Y-%m-%d").fillna("")
    src["Name"] = src["Name"].fillna("").astype(str).str.strip()
    src["QB Num"] = src["QB Num"].fillna("").astype(str).str.strip()
    src["Item"] = src["Item"].fillna("").astype(str).str.strip()
    src["Component_Status"] = src["Component_Status"].fillna("").astype(str).str.strip()
    src["Qty(-)"] = pd.to_numeric(src["Qty(-)"], errors="coerce").fillna(0.0)
    src["Pre/Bare"] = src["Pre/Bare"].fillna("").astype(str).str.strip()

    src = src.loc[src["QB Num"].ne("") & src["Item"].ne("") & src["Qty(-)"].gt(0)].copy()
    if src.empty:
        return _empty_reference_stage()

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
    src["POD#"] = ""
    src["POD ship date"] = ""
    src["Expected Date"] = ""
    src["source_run_date"] = run_ts.normalize().strftime("%Y-%m-%d")
    src["stage_run_ts"] = run_ts
    src["forwarded_from_main"] = False
    src["forward_match_type"] = ""
    src["review_status"] = "new"
    src["review_note"] = ""
    return src[REFERENCE_BASE_COLUMNS + STAGE_EXTRA_COLUMNS].copy()


def _prepare_current_main(current_main: pd.DataFrame | None) -> pd.DataFrame:
    if current_main is None or current_main.empty:
        return _empty_reference_main()

    out = current_main.copy()
    for col in REFERENCE_BASE_COLUMNS + MAIN_EXTRA_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    string_cols = [
        "row_key", "Order Date", "Name", "QB Num", "Item", "Component_Status", "Pre/Bare",
        "POD#", "POD ship date", "Expected Date", "source_run_date", "archive_reason",
    ]
    for col in string_cols:
        out[col] = out[col].fillna("").astype(str).str.strip()
    out["occurrence_index"] = pd.to_numeric(out["occurrence_index"], errors="coerce").fillna(1).astype(int)
    out["Qty(-)"] = pd.to_numeric(out["Qty(-)"], errors="coerce").fillna(0.0)
    out["is_active"] = out["is_active"].fillna(False).astype(bool)
    for col in ["stage_run_ts", "archived_at", "approved_at", "updated_at"]:
        out[col] = pd.to_datetime(out[col], errors="coerce")

    return out[REFERENCE_BASE_COLUMNS + MAIN_EXTRA_COLUMNS].copy()


def _build_pod_ship_lookup(ledger: pd.DataFrame) -> pd.DataFrame:
    if ledger is None or ledger.empty:
        return pd.DataFrame(columns=["POD#", "item_key", "POD ship date"])

    led = ledger.copy()
    for col in ["Kind", "Item", "Date", "QB Num", "P. O. #"]:
        if col not in led.columns:
            led[col] = pd.NA

    led = led.loc[led["Kind"].astype(str).str.strip().eq("IN")].copy()
    if led.empty:
        return pd.DataFrame(columns=["POD#", "item_key", "POD ship date"])

    led["POD#"] = led["QB Num"].fillna("").astype(str).str.strip()
    blank_mask = led["POD#"].eq("")
    led.loc[blank_mask, "POD#"] = led.loc[blank_mask, "P. O. #"].fillna("").astype(str).str.strip()
    led["item_key"] = led["Item"].astype(str).map(_normalize_item_key)
    led["Date"] = pd.to_datetime(led["Date"], errors="coerce")
    led = led.loc[led["POD#"].ne("") & led["item_key"].ne("") & led["Date"].notna()].copy()
    if led.empty:
        return pd.DataFrame(columns=["POD#", "item_key", "POD ship date"])

    lookup = (
        led.groupby(["POD#", "item_key"], as_index=False)["Date"]
        .min()
        .rename(columns={"Date": "POD ship date"})
    )
    lookup["POD ship date"] = lookup["POD ship date"].dt.strftime("%Y-%m-%d")
    return lookup


def _forward_manual_fields(stage_df: pd.DataFrame, current_main: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
    if stage_df.empty:
        return stage_df

    active_main = current_main.loc[current_main["is_active"]].copy()
    if not active_main.empty:
        active_main = active_main.sort_values(["updated_at", "approved_at", "stage_run_ts"], kind="mergesort")
        active_main = active_main.drop_duplicates(subset=["row_key"], keep="last")
        carry = active_main[["row_key", "POD#", "Expected Date"]].rename(
            columns={"POD#": "POD#_main", "Expected Date": "Expected Date_main"}
        )
        stage_df = stage_df.merge(carry, on="row_key", how="left")
        stage_df["forwarded_from_main"] = stage_df["POD#_main"].notna() | stage_df["Expected Date_main"].notna()
        stage_df["forward_match_type"] = stage_df["forwarded_from_main"].map({True: "row_key", False: ""})
        stage_df["POD#"] = stage_df["POD#_main"].fillna(stage_df["POD#"]).fillna("")
        stage_df["Expected Date"] = stage_df["Expected Date_main"].fillna(stage_df["Expected Date"]).fillna("")
        stage_df = stage_df.drop(columns=["POD#_main", "Expected Date_main"])

    lookup = _build_pod_ship_lookup(ledger)
    stage_df["item_key"] = stage_df["Item"].astype(str).map(_normalize_item_key)
    stage_df = stage_df.merge(lookup, on=["POD#", "item_key"], how="left", suffixes=("", "_lookup"))
    stage_df["POD ship date"] = stage_df["POD ship date_lookup"].fillna(stage_df["POD ship date"]).fillna("")
    stage_df = stage_df.drop(columns=["item_key", "POD ship date_lookup"])
    return stage_df


def _values_differ(left: object, right: object) -> bool:
    if pd.isna(left) and pd.isna(right):
        return False
    left_str = "" if pd.isna(left) else str(left).strip()
    right_str = "" if pd.isna(right) else str(right).strip()
    return left_str != right_str


def _build_reference_diff(active_main: pd.DataFrame, stage_df: pd.DataFrame, run_ts: pd.Timestamp) -> pd.DataFrame:
    if active_main.empty and stage_df.empty:
        return _empty_reference_diff()

    compare_cols = [
        "Order Date", "Name", "QB Num", "Item", "Component_Status", "Qty(-)", "Pre/Bare",
        "POD#", "POD ship date", "Expected Date",
    ]
    main_cmp = active_main[["row_key"] + compare_cols].copy() if not active_main.empty else pd.DataFrame(columns=["row_key"] + compare_cols)
    stage_cmp = stage_df[["row_key"] + compare_cols].copy() if not stage_df.empty else pd.DataFrame(columns=["row_key"] + compare_cols)
    merged = main_cmp.merge(stage_cmp, on="row_key", how="outer", suffixes=("_main", "_stage"), indicator=True)

    rows: list[dict[str, object]] = []
    system_fields = ["Order Date", "Name", "Component_Status", "Qty(-)", "Pre/Bare"]
    manual_fields = ["POD#", "Expected Date", "POD ship date"]
    for _, row in merged.iterrows():
        if row["_merge"] == "right_only":
            change_type = "new"
            changed_fields = "new_row"
        elif row["_merge"] == "left_only":
            change_type = "archived"
            changed_fields = "archived"
        else:
            changed = [field for field in compare_cols if _values_differ(row.get(f"{field}_main"), row.get(f"{field}_stage"))]
            if not changed:
                continue
            change_type = "manual_changed" if set(changed).issubset(set(manual_fields)) else "system_changed"
            changed_fields = ", ".join(changed)

        rows.append({
            "row_key": row["row_key"],
            "change_type": change_type,
            "changed_fields": changed_fields,
            "QB Num": row.get("QB Num_stage") if pd.notna(row.get("QB Num_stage")) else row.get("QB Num_main"),
            "Item": row.get("Item_stage") if pd.notna(row.get("Item_stage")) else row.get("Item_main"),
            "main_Order Date": row.get("Order Date_main"),
            "stage_Order Date": row.get("Order Date_stage"),
            "main_Name": row.get("Name_main"),
            "stage_Name": row.get("Name_stage"),
            "main_Component_Status": row.get("Component_Status_main"),
            "stage_Component_Status": row.get("Component_Status_stage"),
            "main_Qty(-)": row.get("Qty(-)_main"),
            "stage_Qty(-)": row.get("Qty(-)_stage"),
            "main_Pre/Bare": row.get("Pre/Bare_main"),
            "stage_Pre/Bare": row.get("Pre/Bare_stage"),
            "main_POD#": row.get("POD#_main"),
            "stage_POD#": row.get("POD#_stage"),
            "main_POD ship date": row.get("POD ship date_main"),
            "stage_POD ship date": row.get("POD ship date_stage"),
            "main_Expected Date": row.get("Expected Date_main"),
            "stage_Expected Date": row.get("Expected Date_stage"),
            "source_run_date": run_ts.normalize().strftime("%Y-%m-%d"),
        })

    if not rows:
        return _empty_reference_diff()
    return pd.DataFrame(rows, columns=DIFF_COLUMNS).sort_values(["change_type", "QB Num", "Item"], kind="mergesort").reset_index(drop=True)


def _build_updated_main(current_main: pd.DataFrame, stage_df: pd.DataFrame, run_ts: pd.Timestamp) -> pd.DataFrame:
    current_main = _prepare_current_main(current_main)
    archived_history = current_main.loc[~current_main["is_active"]].copy()
    active_current = current_main.loc[current_main["is_active"]].copy()
    if not active_current.empty:
        active_current = active_current.sort_values(["updated_at", "approved_at", "stage_run_ts"], kind="mergesort")
        active_current = active_current.drop_duplicates(subset=["row_key"], keep="last")

    stage_keys = set(stage_df["row_key"].astype(str))
    to_archive = active_current.loc[~active_current["row_key"].isin(stage_keys)].copy()
    if not to_archive.empty:
        to_archive["is_active"] = False
        to_archive["archived_at"] = run_ts
        to_archive["archive_reason"] = "missing_from_latest_stage"
        to_archive["updated_at"] = run_ts

    stage_main = stage_df[REFERENCE_BASE_COLUMNS].copy() if not stage_df.empty else _empty_reference_main()[REFERENCE_BASE_COLUMNS].copy()
    preserved = active_current[["row_key", "approved_at"]].copy() if not active_current.empty else pd.DataFrame(columns=["row_key", "approved_at"])
    stage_main = stage_main.merge(preserved, on="row_key", how="left")
    stage_main["is_active"] = True
    stage_main["archived_at"] = pd.NaT
    stage_main["archive_reason"] = ""
    stage_main["approved_at"] = pd.to_datetime(stage_main["approved_at"], errors="coerce").fillna(run_ts)
    stage_main["updated_at"] = run_ts

    main_df = pd.concat([archived_history, to_archive, stage_main], ignore_index=True, sort=False)
    if main_df.empty:
        return _empty_reference_main()
    return main_df[REFERENCE_BASE_COLUMNS + MAIN_EXTRA_COLUMNS].sort_values(
        ["is_active", "QB Num", "Item", "occurrence_index"],
        ascending=[False, True, True, True],
        kind="mergesort",
    ).reset_index(drop=True)


def build_so_reference_tables(
    structured: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    current_main: pd.DataFrame | None = None,
    run_ts: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    run_ts = pd.Timestamp.now() if run_ts is None else pd.to_datetime(run_ts)
    current_main_df = _prepare_current_main(current_main)
    stage_df = _normalize_reference_source(structured, run_ts)
    stage_df = _forward_manual_fields(stage_df, current_main_df, ledger)

    active_main = current_main_df.loc[current_main_df["is_active"]].copy()
    if not active_main.empty:
        active_main = active_main.sort_values(["updated_at", "approved_at", "stage_run_ts"], kind="mergesort")
        active_main = active_main.drop_duplicates(subset=["row_key"], keep="last")

    diff_df = _build_reference_diff(active_main, stage_df, run_ts)
    review_map = {
        row["row_key"]: ("needs_review" if row["change_type"] in {"new", "system_changed", "manual_changed"} else row["change_type"])
        for _, row in diff_df.iterrows()
        if row["change_type"] != "archived"
    }
    if not stage_df.empty:
        stage_df["review_status"] = stage_df["row_key"].map(review_map).fillna("unchanged")
    main_df = _build_updated_main(current_main_df, stage_df, run_ts)
    return (
        stage_df[REFERENCE_BASE_COLUMNS + STAGE_EXTRA_COLUMNS].copy(),
        diff_df,
        main_df,
    )


def build_assignment_run_tables(
    structured: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    from_date: pd.Timestamp | None = None,
    cutoff_date: str = "2099-07-04",
    run_ts: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    run_ts = pd.Timestamp.now() if run_ts is None else pd.to_datetime(run_ts)
    run_id = run_ts.strftime("run_%Y%m%d_%H%M%S")
    modes = ["strict", "loose"]

    run_rows: list[pd.DataFrame] = []
    per_mode: dict[str, pd.DataFrame] = {}
    blocker_by_mode: dict[str, pd.DataFrame] = {}

    for mode in modes:
        summary_df, blocker_df = _build_assignment_readiness_for_mode(
            structured,
            ledger,
            from_date=from_date,
            cutoff_date=cutoff_date,
            mode=mode,
        )
        per_mode[mode] = summary_df.copy()
        blocker_by_mode[mode] = blocker_df.copy()

        run_df = summary_df.rename(
            columns={
                "Ready to be assigned": "is_ready",
                "Earliest Ready Date": "earliest_ready_date",
                "Blocking Item Count": "blocking_item_count",
                "Blocking Items": "blocking_items",
                "Waiting / Shortage Items": "waiting_shortage_items",
            }
        ).copy()
        run_df.insert(0, "run_id", run_id)
        run_df.insert(1, "run_ts", run_ts)
        run_df.insert(2, "mode", mode)
        run_df["decision_status"] = run_df["is_ready"].map({True: "ready", False: "blocked"}).fillna("blocked")
        reason_map = (
            blocker_df.groupby("QB Num")["Block Reason"].agg(lambda s: "; ".join(sorted(set(s.astype(str)))))
            if not blocker_df.empty else pd.Series(dtype="object")
        )
        run_df["blocking_reasons"] = run_df["QB Num"].map(reason_map).fillna("")
        run_df["note"] = ""
        run_rows.append(
            run_df[
                [
                    "run_id", "run_ts", "mode", "QB Num", "Name", "P. O. #", "Order Date",
                    "Current Ship Date", "Item Count", "is_ready", "earliest_ready_date",
                    "blocking_item_count", "blocking_items", "waiting_shortage_items",
                    "blocking_reasons", "decision_status", "note",
                ]
            ]
        )

    strict_df = run_rows[0].copy()
    loose_df = run_rows[1].copy()
    strict_cmp = strict_df[
        ["QB Num", "is_ready", "earliest_ready_date", "blocking_items", "blocking_reasons"]
    ].rename(
        columns={
            "is_ready": "strict_ready",
            "earliest_ready_date": "strict_date",
            "blocking_items": "strict_blocking_items",
            "blocking_reasons": "strict_blocking_reasons",
        }
    )
    loose_cmp = loose_df[
        ["QB Num", "is_ready", "earliest_ready_date", "blocking_items", "blocking_reasons"]
    ].rename(
        columns={
            "is_ready": "loose_ready",
            "earliest_ready_date": "loose_date",
            "blocking_items": "loose_blocking_items",
            "blocking_reasons": "loose_blocking_reasons",
        }
    )
    comparison = strict_cmp.merge(loose_cmp, on="QB Num", how="outer")
    comparison["diff_type"] = "same_blocked"
    comparison.loc[comparison["strict_ready"].fillna(False) & comparison["loose_ready"].fillna(False), "diff_type"] = "same_ready"
    comparison.loc[~comparison["strict_ready"].fillna(False) & comparison["loose_ready"].fillna(False), "diff_type"] = "loose_only_ready"
    strict_date = pd.to_datetime(comparison["strict_date"], errors="coerce")
    loose_date = pd.to_datetime(comparison["loose_date"], errors="coerce")
    same_ready_mask = comparison["strict_ready"].fillna(False) & comparison["loose_ready"].fillna(False)
    comparison.loc[same_ready_mask & strict_date.ne(loose_date), "diff_type"] = "date_changed"
    comparison["diff_items"] = ""
    for idx, row in comparison.iterrows():
        strict_items = {s.strip() for s in str(row.get("strict_blocking_items") or "").split(",") if s.strip()}
        loose_items = {s.strip() for s in str(row.get("loose_blocking_items") or "").split(",") if s.strip()}
        comparison.at[idx, "diff_items"] = ", ".join(sorted(strict_items.symmetric_difference(loose_items)))

    runs_df = pd.concat(run_rows, ignore_index=True).reset_index(drop=True)
    runs_df = runs_df.merge(comparison, on="QB Num", how="left")
    runs_df["comparison_mode"] = runs_df["mode"].map({"strict": "loose", "loose": "strict"})
    runs_df["comparison_is_ready"] = runs_df["loose_ready"].where(runs_df["mode"].eq("strict"), runs_df["strict_ready"])
    runs_df["comparison_ready_date"] = runs_df["loose_date"].where(runs_df["mode"].eq("strict"), runs_df["strict_date"])
    runs_df["comparison_blocking_items"] = runs_df["loose_blocking_items"].where(
        runs_df["mode"].eq("strict"), runs_df["strict_blocking_items"]
    )
    runs_df["comparison_blocking_reasons"] = runs_df["loose_blocking_reasons"].where(
        runs_df["mode"].eq("strict"), runs_df["strict_blocking_reasons"]
    )
    runs_df["admin_decision"] = ""
    runs_df["admin_note"] = ""
    runs_df = runs_df[
        [
            "run_id", "run_ts", "mode", "QB Num", "Name", "P. O. #", "Order Date",
            "Current Ship Date", "Item Count", "is_ready", "earliest_ready_date",
            "blocking_item_count", "blocking_items", "blocking_reasons",
            "waiting_shortage_items", "decision_status", "comparison_mode",
            "comparison_is_ready", "comparison_ready_date", "comparison_blocking_items",
            "comparison_blocking_reasons", "diff_type", "diff_items",
            "admin_decision", "admin_note", "note",
        ]
    ].sort_values(["mode", "QB Num"], kind="mergesort").reset_index(drop=True)

    return runs_df
