from __future__ import annotations

import pandas as pd


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

    # Reverse within each item, run cumulative min, then flip back
    def _future_min(group: pd.DataFrame) -> pd.Series:
        vals = group["Projected_NAV"].values[::-1]
        out = []
        current_min = float("inf")
        for v in vals:
            if pd.isna(v):
                current_min = min(current_min, float("inf"))
            else:
                current_min = min(current_min, float(v))
            out.append(current_min)
        out = out[::-1]
        return pd.Series(out, index=group.index)

    df["FutureMin_NAV"] = df.groupby("Item", group_keys=False).apply(_future_min)

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
