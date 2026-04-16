from __future__ import annotations

import pandas as pd

from erp_system.contracts import TABLE_CONTRACTS, ensure_contract_columns
from erp_system.ledger.atp import earliest_atp_strict
from erp_system.normalize.erp_normalize import normalize_item
from erp_system.transform.common import _norm_key


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
    Build an ATP view for an existing SO item by removing that SO's own demand
    rows from the ledger first, then recomputing projected NAV.
    """
    led = ledger.copy()
    if led.empty or "Delta" not in led.columns or "Date" not in led.columns:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])

    normalized_item = _normalize_item_key(item)
    if "Item" not in led.columns:
        raise ValueError("Required column 'Item' is missing from led")

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
    include_cutoff_in_check = str(mode).strip().lower() == "strict"

    so = ensure_contract_columns(
        structured,
        TABLE_CONTRACTS["wo_structured"],
        extra_columns=("Name", "P. O. #", "Order Date", "Component_Status"),
    )

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
        demands_df = grp.groupby("Item", as_index=False)["Qty(-)"].sum().sort_values("Item", kind="mergesort")
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


def build_assignment_run_tables(
    structured: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    from_date: pd.Timestamp | None = None,
    cutoff_date: str = "2099-07-04",
    run_ts: pd.Timestamp | None = None,
) -> pd.DataFrame:
    run_ts = pd.Timestamp.now() if run_ts is None else pd.to_datetime(run_ts)
    run_id = run_ts.strftime("run_%Y%m%d_%H%M%S")
    run_rows: list[pd.DataFrame] = []

    for mode in ("strict", "loose"):
        summary_df, blocker_df = _build_assignment_readiness_for_mode(
            structured,
            ledger,
            from_date=from_date,
            cutoff_date=cutoff_date,
            mode=mode,
        )

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
            if not blocker_df.empty
            else pd.Series(dtype="object")
        )
        run_df["blocking_reasons"] = run_df["QB Num"].map(reason_map).fillna("")
        run_df["note"] = ""
        run_rows.append(
            run_df[
                [
                    "run_id",
                    "run_ts",
                    "mode",
                    "QB Num",
                    "Name",
                    "P. O. #",
                    "Order Date",
                    "Current Ship Date",
                    "Item Count",
                    "is_ready",
                    "earliest_ready_date",
                    "blocking_item_count",
                    "blocking_items",
                    "waiting_shortage_items",
                    "blocking_reasons",
                    "decision_status",
                    "note",
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
    comparison.loc[
        comparison["strict_ready"].fillna(False) & comparison["loose_ready"].fillna(False), "diff_type"
    ] = "same_ready"
    comparison.loc[
        ~comparison["strict_ready"].fillna(False) & comparison["loose_ready"].fillna(False), "diff_type"
    ] = "loose_only_ready"
    strict_date = pd.to_datetime(comparison["strict_date"], errors="coerce")
    loose_date = pd.to_datetime(comparison["loose_date"], errors="coerce")
    same_ready_mask = comparison["strict_ready"].fillna(False) & comparison["loose_ready"].fillna(False)
    comparison.loc[same_ready_mask & strict_date.ne(loose_date), "diff_type"] = "date_changed"
    comparison["diff_items"] = ""
    for idx, row in comparison.iterrows():
        strict_items = {
            s.strip() for s in str(row.get("strict_blocking_items") or "").split(",") if s.strip()
        }
        loose_items = {
            s.strip() for s in str(row.get("loose_blocking_items") or "").split(",") if s.strip()
        }
        comparison.at[idx, "diff_items"] = ", ".join(sorted(strict_items.symmetric_difference(loose_items)))

    runs_df = pd.concat(run_rows, ignore_index=True).reset_index(drop=True)
    runs_df = runs_df.merge(comparison, on="QB Num", how="left")
    runs_df["comparison_mode"] = runs_df["mode"].map({"strict": "loose", "loose": "strict"})
    runs_df["comparison_is_ready"] = runs_df["loose_ready"].where(
        runs_df["mode"].eq("strict"), runs_df["strict_ready"]
    )
    runs_df["comparison_ready_date"] = runs_df["loose_date"].where(
        runs_df["mode"].eq("strict"), runs_df["strict_date"]
    )
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
            "run_id",
            "run_ts",
            "mode",
            "QB Num",
            "Name",
            "P. O. #",
            "Order Date",
            "Current Ship Date",
            "Item Count",
            "is_ready",
            "earliest_ready_date",
            "blocking_item_count",
            "blocking_items",
            "blocking_reasons",
            "waiting_shortage_items",
            "decision_status",
            "comparison_mode",
            "comparison_is_ready",
            "comparison_ready_date",
            "comparison_blocking_items",
            "comparison_blocking_reasons",
            "diff_type",
            "diff_items",
            "admin_decision",
            "admin_note",
            "note",
        ]
    ].sort_values(["mode", "QB Num"], kind="mergesort").reset_index(drop=True)

    return runs_df


__all__ = [
    "build_assignment_readiness_reports",
    "build_assignment_run_tables",
]
