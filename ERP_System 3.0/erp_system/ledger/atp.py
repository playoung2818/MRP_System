from __future__ import annotations

import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item
from erp_system.runtime.constants import PLACEHOLDER_DATE
from erp_system.transform.common import _norm_key


def backward_cumulative_min(values: list[float]) -> list[float]:
    """
    Walk `values` backward, tracking a running minimum, and return that
    running minimum stamped onto each position -- i.e. out[i] is the smallest
    value at or after position i. NaN entries don't update the minimum, they
    just inherit whatever it currently is.

    Shared FutureMin_NAV calculation for general and per-SO ATP views.
    """
    out: list[float] = [0.0] * len(values)
    current_min = float("inf")
    for idx in range(len(values) - 1, -1, -1):
        value = values[idx]
        if pd.notna(value):
            current_min = min(current_min, float(value))
        out[idx] = current_min
    return out


def build_atp_view(ledger: pd.DataFrame) -> pd.DataFrame:
    """
    Build an ATP-ready view from the ledger.

    Expected columns in `ledger`:
      - 'Item'
      - 'Date'
      - 'Projected_NAV'

    Returns DataFrame with columns:
      - 'Item'
      - 'Date'
      - 'Projected_NAV'
      - 'FutureMin_NAV'  (backward cumulative min of Projected_NAV per item)

    Rows with missing Item or Date are dropped.
    Pseudo-items whose name starts with 'Total ' are excluded.
    """
    if ledger is None or ledger.empty:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])

    df = ledger.copy()

    # Basic hygiene
    item_col = "Item_raw" if "Item_raw" in df.columns else "Item"
    if item_col not in df.columns or "Date" not in df.columns or "Projected_NAV" not in df.columns:
        raise ValueError("ledger must contain 'Item' (or 'Item_raw'), 'Date', and 'Projected_NAV' columns.")

    if item_col != "Item":
        df["Item"] = df[item_col]

    df = df.loc[df["Item"].notna() & df["Date"].notna()].copy()

    # Drop pseudo "Total ..." rollups
    df = df.loc[~df["Item"].astype(str).str.startswith("Total ")].copy()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.loc[df["Date"].notna()].copy()

    df["Projected_NAV"] = pd.to_numeric(df["Projected_NAV"], errors="coerce")

    # Sort ascending by date, then compute backward cumulative min per item
    df.sort_values(["Item", "Date"], inplace=True)

    df["FutureMin_NAV"] = df.groupby("Item")["Projected_NAV"].transform(
        lambda values: backward_cumulative_min(values.tolist())
    )

    # Final column selection / ordering
    atp_view = df.loc[:, ["Item", "Date", "Projected_NAV", "FutureMin_NAV"]].copy()
    atp_view.sort_values(["Item", "Date"], inplace=True)
    atp_view.reset_index(drop=True, inplace=True)

    return atp_view


def earliest_atp_strict(
    atp_view: pd.DataFrame,
    item: str,
    qty: float,
    from_date: pd.Timestamp | None = None,
    *,
    allow_zero: bool = True,
) -> pd.Timestamp | None:
    """
    Earliest date D where:
      - We add a new OUT of `qty` at D for `item`, and
      - Inventory stays >= 0 (or > 0) for all dates t >= D.

    Uses precomputed FutureMin_NAV:
      - If allow_zero is True: require FutureMin_NAV >= qty
      - Else:                  require FutureMin_NAV > qty
    """
    if atp_view is None or atp_view.empty:
        return None

    if from_date is None:
        from_date = pd.Timestamp.today().normalize()
    else:
        from_date = pd.to_datetime(from_date).normalize()

    df = atp_view.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    mask = (df["Item"].astype(str) == str(item)) & (df["Date"] >= from_date)
    df_item = df.loc[mask, ["Date", "FutureMin_NAV"]].dropna(subset=["Date", "FutureMin_NAV"])
    if df_item.empty:
        return None

    df_item["FutureMin_NAV"] = pd.to_numeric(df_item["FutureMin_NAV"], errors="coerce")
    if allow_zero:
        ok = df_item["FutureMin_NAV"] >= float(qty)
    else:
        ok = df_item["FutureMin_NAV"] > float(qty)

    candidates = df_item.loc[ok, "Date"]
    if candidates.empty:
        return None
    return candidates.min()


def earliest_atp_for_items_strict(
    atp_view: pd.DataFrame,
    demands: dict[str, float],
    from_date: pd.Timestamp | None = None,
    *,
    allow_zero: bool = True,
) -> pd.Timestamp | None:
    """
    Given multiple items and required quantities (a BOM or full quote),
    return the earliest date when all items can be supplied without
    making any future NAV negative.

    Logic:
      - For each (item, qty), compute its own earliest ATP date.
      - If any item has no feasible date -> return None.
      - Otherwise, return max of all item dates.
    """
    if not demands:
        return None

    dates: list[pd.Timestamp] = []
    for itm, qty in demands.items():
        d = earliest_atp_strict(
            atp_view=atp_view,
            item=itm,
            qty=qty,
            from_date=from_date,
            allow_zero=allow_zero,
        )
        if d is None:
            return None
        dates.append(d)

    if not dates:
        return None
    return max(dates)


def earliest_atp_by_projected_nav(
    ledger: pd.DataFrame,
    item: str,
    qty: float,
    from_date: pd.Timestamp | None = None,
) -> pd.Timestamp | None:
    """Point-in-time availability only; unlike strict ATP, ignores future dips."""
    if ledger is None or ledger.empty:
        return None
    from_date = pd.Timestamp.today().normalize() if from_date is None else pd.to_datetime(from_date).normalize()
    qty_val = pd.to_numeric(qty, errors="coerce")
    if pd.isna(qty_val):
        return None
    qty_val = int(qty_val)
    if not {"Item", "Date", "Projected_NAV"}.issubset(ledger.columns):
        return None
    df = ledger.loc[ledger["Item"].astype(str) == str(item)].copy()
    if df.empty:
        return None
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.loc[df["Date"].notna() & df["Date"].ne(PLACEHOLDER_DATE)]
    df["Projected_NAV"] = pd.to_numeric(df["Projected_NAV"], errors="coerce")
    df = df.loc[df["Projected_NAV"].notna() & (df["Date"] >= from_date)].sort_values("Date")
    candidates = df.loc[df["Projected_NAV"] >= qty_val, "Date"]
    return None if candidates.empty else candidates.min()


def _normalize_item_key(item: str) -> str:
    series = pd.Series([item], dtype="string").map(normalize_item)
    return str(_norm_key(series).iloc[0])


def build_adjusted_item_atp(
    ledger: pd.DataFrame,
    *,
    qb_num: str,
    item: str,
    from_date: pd.Timestamp,
) -> pd.DataFrame:
    """Remove an existing SO's own demand and recompute its item's ATP view."""
    led = ledger.copy()
    if led.empty or "Delta" not in led.columns or "Date" not in led.columns:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])
    if "Item" not in led.columns:
        raise ValueError("Required column 'Item' is missing from led")
    mask_item = _norm_key(led["Item"]).astype(str) == _normalize_item_key(item)
    item_df = led.loc[mask_item].copy()
    if item_df.empty:
        return pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])

    opening_series = pd.to_numeric(item_df.get("Opening"), errors="coerce").dropna()
    opening = float(opening_series.iloc[0]) if not opening_series.empty else 0.0
    so_col = item_df.get("QB Num", pd.Series("", index=item_df.index)).astype(str)
    kind_col = item_df.get("Kind", pd.Series("", index=item_df.index)).astype(str)
    adjusted = item_df.loc[~(so_col.eq(str(qb_num)) & kind_col.eq("OUT"))].copy()
    adjusted["Date"] = pd.to_datetime(adjusted["Date"], errors="coerce")
    adjusted["Delta"] = pd.to_numeric(adjusted["Delta"], errors="coerce").fillna(0.0)
    adjusted = adjusted.loc[adjusted["Date"].notna()].copy()
    kind_col = adjusted.get("Kind", pd.Series("", index=adjusted.index)).astype(str)
    adjusted = adjusted.loc[~(kind_col.eq("IN") & adjusted["Date"].ge(PLACEHOLDER_DATE))].copy()
    if adjusted.empty:
        return pd.DataFrame({
            "Item": [item], "Date": [from_date],
            "Projected_NAV": [opening], "FutureMin_NAV": [opening],
        })

    adjusted = adjusted.sort_values("Date", kind="mergesort").reset_index(drop=True)
    adjusted["Projected_NAV"] = opening + adjusted["Delta"].cumsum()
    projected = pd.to_numeric(adjusted["Projected_NAV"], errors="coerce").tolist()
    return pd.DataFrame({
        "Item": str(adjusted["Item"].iloc[0]),
        "Date": adjusted["Date"].tolist(), "Projected_NAV": projected,
        "FutureMin_NAV": backward_cumulative_min(projected),
    })


def earliest_assignment_date(
    ledger: pd.DataFrame,
    *,
    qb_num: str,
    item: str,
    qty: float,
    from_date: pd.Timestamp,
    cutoff: pd.Timestamp,
    include_cutoff_in_check: bool,
) -> pd.Timestamp | None:
    """Existing-SO ATP, with the assignment workflow's strict/loose cutoff."""
    adj_atp = build_adjusted_item_atp(ledger, qb_num=qb_num, item=item, from_date=from_date)
    if adj_atp.empty:
        return None
    dates = pd.to_datetime(adj_atp["Date"], errors="coerce")
    mask = dates.le(cutoff) if include_cutoff_in_check else dates.lt(cutoff)
    scoped = adj_atp.loc[mask].copy()
    if scoped.empty:
        return None
    scoped["Date"] = pd.to_datetime(scoped["Date"], errors="coerce")
    scoped["Projected_NAV"] = pd.to_numeric(scoped["Projected_NAV"], errors="coerce")
    scoped["FutureMin_NAV"] = backward_cumulative_min(scoped["Projected_NAV"].tolist())
    candidates = scoped.loc[scoped["Date"] < cutoff].copy()
    if candidates.empty:
        return None
    return earliest_atp_strict(
        candidates[["Item", "Date", "Projected_NAV", "FutureMin_NAV"]],
        _normalize_item_key(item), qty, from_date=from_date, allow_zero=True,
    )
