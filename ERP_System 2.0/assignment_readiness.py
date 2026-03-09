from __future__ import annotations

import pandas as pd

from atp import earliest_atp_strict


def _item_mask(df: pd.DataFrame, item: str) -> pd.Series:
    if "Item_raw" in df.columns:
        return df["Item_raw"].astype(str) == str(item)
    return df["Item"].astype(str) == str(item)


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

    mask_item = _item_mask(led, item)
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

    item_name = str(adjusted["Item_raw"].iloc[0] if "Item_raw" in adjusted.columns else adjusted["Item"].iloc[0])
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
    return earliest_atp_strict(scoped[["Item", "Date", "Projected_NAV", "FutureMin_NAV"]], item, qty, from_date=from_date, allow_zero=True)


def build_assignment_readiness_reports(
    structured: pd.DataFrame,
    ledger: pd.DataFrame,
    *,
    from_date: pd.Timestamp | None = None,
    cutoff_date: str = "2099-07-04",
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
            dt = _earliest_assignment_date_before_cutoff(
                ledger,
                qb_num=str(qb_num),
                item=item,
                qty=qty,
                from_date=start_date,
                cutoff=cutoff,
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
                        "Block Reason": "No feasible ATP date after removing this SO's placeholder demand",
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
