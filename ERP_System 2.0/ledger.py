from __future__ import annotations
import re
import numpy as np
import pandas as pd
from pandas.api.types import CategoricalDtype
from core import _norm_cols, _norm_key
from erp_normalize import normalize_item

## 1) SAP (shipping) → expand pre-installed components
INCL_SPLIT = re.compile(r"\bincluding\b", re.IGNORECASE)
QTYX_RE = re.compile(r"^\s*(\d+)\s*x\s*(.+)\s*$", re.IGNORECASE)  # "2x SSD-1TB"

def clean_space(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return s.replace("\u00A0", " ").replace("\u3000", " ").strip()

def parse_description(desc: str) -> tuple[str, list[str]]:
    s = clean_space(desc)
    parts = INCL_SPLIT.split(s, maxsplit=1)
    parent = clean_space(parts[0].split(",")[0])
    comps = []
    if len(parts) > 1:
        comps = [clean_space(x) for x in parts[1].split(",") if clean_space(x)]
    return parent, comps

def parse_component_token(token: str) -> tuple[str, float]:
    m = QTYX_RE.match(token)
    if m:
        qty = float(m.group(1))
        item = clean_space(m.group(2))
        return item, qty
    return clean_space(token), 1.0

def expand_preinstalled_row(row: pd.Series) -> pd.DataFrame:
    parent, tokens = parse_description(row.get("Description", ""))
    base_qty = float(row.get("Qty(+)", 0) or 0)
    parent_item = parent or clean_space(str(row.get("Item", "")))

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

def expand_nav_preinstalled(NAV: pd.DataFrame) -> pd.DataFrame:
    NAV = NAV.copy()
    for col in ["Pre/Bare", "Qty(+)", "Item"]:
        if col not in NAV.columns:
            raise ValueError(f"NAV must contain '{col}' column.")
    if "Description" not in NAV.columns:
        NAV["Description"] = ""

    NAV["Description"] = NAV["Description"].astype(str).apply(clean_space)

    pre_mask = NAV["Pre/Bare"].astype(str).str.strip().str.casefold().eq("pre")
    nav_pre   = NAV.loc[pre_mask].copy()
    nav_other = NAV.loc[~pre_mask].copy()

    expanded_parts = [expand_preinstalled_row(r) for _, r in nav_pre.iterrows()]
    expanded_pre = (pd.concat(expanded_parts, ignore_index=True) if expanded_parts else nav_pre.copy())

    needed_cols = list(NAV.columns) + ["Parent_Item", "Qty_per_parent", "IsParent"]
    expanded_pre = expanded_pre.reindex(columns=needed_cols, fill_value=pd.NA)
    nav_other    = nav_other.reindex(columns=needed_cols, fill_value=pd.NA)

    nav_other.loc[:, "Parent_Item"]    = nav_other["Item"]
    nav_other.loc[:, "Qty_per_parent"] = 1.0
    nav_other.loc[:, "IsParent"]       = True

    expanded_all = pd.concat([expanded_pre, nav_other], ignore_index=True)

    expanded_all["Qty(+)"]         = pd.to_numeric(expanded_all["Qty(+)"], errors="coerce").fillna(0.0)
    expanded_all["Qty_per_parent"] = pd.to_numeric(expanded_all["Qty_per_parent"], errors="coerce").fillna(1.0)
    expanded_all["IsParent"]       = expanded_all["IsParent"].astype(bool)
    expanded_all["Date"] = pd.to_datetime(expanded_all["Ship Date"], errors="coerce") + pd.Timedelta(days=5)
    expanded_all["Item"] = expanded_all["Item"].astype(str).map(normalize_item)
    return expanded_all


## 2) Events + Ledger
def build_opening_stock(SO: pd.DataFrame) -> pd.DataFrame:
    src = SO.copy()
    col = "On Hand"
    if col not in src.columns:
        src[col] = 0.0
    stock = (
        src[["Item", col]]
        .dropna()
        .drop_duplicates(subset=["Item"], keep="last")
        .rename(columns={col: "Opening"})
    )
    stock["Item"] = _norm_key(stock["Item"])
    return stock

def _order_events(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["Date"]  = pd.to_datetime(out["Date"], errors="coerce").dt.normalize()
    out["Delta"] = pd.to_numeric(out["Delta"], errors="coerce")
    kind_cat = CategoricalDtype(categories=["OPEN", "IN", "ADJ", "OUT"], ordered=True)
    out["Kind"] = out["Kind"].astype(kind_cat)

    if not set(["Date", "Item", "Delta", "Kind"]).issubset(out.columns):
        raise ValueError("events must have columns: ['Date','Item','Delta','Kind']")
    out = out.dropna(subset=["Date", "Item"]).loc[out["Delta"].notna()]
    out = out.loc[out["Delta"].ne(0)].copy()
    out.sort_values(["Item", "Date", "Kind"], inplace=True, kind="mergesort")
    out.reset_index(drop=True, inplace=True)
    return out

def build_events(
    SO: pd.DataFrame,
    NAV_EXP: pd.DataFrame,
    POD: pd.DataFrame | None = None,
) -> pd.DataFrame:
    so = _norm_cols(SO)
    nav = _norm_cols(NAV_EXP)

    inbound = (
        nav.loc[nav["Qty(+)"] > 0, ["Date", "Item", "Qty(+)"]]
        .rename(columns={"Qty(+)": "Delta"})
        .assign(Kind="IN", Source="NAV")
    )
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
    inbound  = inbound.reindex(columns=cols)
    outbound = outbound.reindex(columns=cols)

    pod_events = pd.DataFrame(columns=cols)
    if POD is not None and not POD.empty:
        pod = _norm_cols(POD)
        if "Source Name" in pod.columns:
            pod = pod.loc[
                pod["Source Name"].astype(str).str.strip().ne("Neousys Technology Incorp.")
            ].copy()
        if "Ship Date" not in pod.columns and "Deliv Date" in pod.columns:
            pod["Ship Date"] = pod["Deliv Date"]
        pod_keep = ["Ship Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"]
        for c in pod_keep:
            if c not in pod.columns:
                pod[c] = pd.NA
        pod_events = (
            pod.loc[pod["Qty(+)"] > 0, pod_keep]
            .rename(columns={"Ship Date": "Date", "Qty(+)": "Delta"})
            .assign(Kind="IN", Source="POD")
        )
        pod_events["Item_raw"] = pod_events["Item"]
        pod_events["Item"] = _norm_key(pod_events["Item"])
        pod_events = pod_events.reindex(columns=cols)

    events = pd.concat([inbound, pod_events, outbound], ignore_index=True, sort=False)
    return _order_events(events)

def build_ledger_from_events(
    SO: pd.DataFrame,
    EVENTS: pd.DataFrame,
    INVENTORY: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Build ledger using prebuilt events (IN/ADJ/OUT already combined).
    SO only used to compute per-item Opening.
    """
    so = _norm_cols(SO)
    stock = build_opening_stock(so)  # Item, Opening

    events = EVENTS.copy()
    events = events.merge(stock, on="Item", how="left")
    events["Opening"] = events["Opening"].fillna(0.0)

    today = pd.Timestamp.today().normalize()
    open_df = pd.DataFrame({
        "Date":   [today]*len(stock),
        "Item":   stock["Item"].values,
        "Delta":  0.0,
        "Kind":   "OPEN",
        "Source": "Snapshot",
        "Opening": stock["Opening"].values,
    })

    ledger = pd.concat([open_df, events], ignore_index=True, sort=False)
    ledger = _order_events(ledger)

    ledger["CumDelta"]      = ledger.groupby("Item", sort=False)["Delta"].cumsum()
    ledger["Projected_NAV"] = ledger["Opening"] + ledger["CumDelta"]

    is_out = ledger["Kind"].eq("OUT")
    ledger["NAV_before"] = np.where(is_out, ledger["Projected_NAV"] - ledger["Delta"], np.nan)
    ledger["NAV_after"]  = np.where(is_out, ledger["Projected_NAV"], np.nan)

    item_min = (ledger.groupby("Item", as_index=False)["Projected_NAV"]
                      .min().rename(columns={"Projected_NAV": "Min_Projected_NAV"}))
    first_neg = (
        ledger.loc[ledger["Projected_NAV"] < 0]
              .sort_values(["Item", "Date"])
              .groupby("Item", as_index=False)
              .first()[["Item", "Date", "Projected_NAV"]]
              .rename(columns={"Date": "First_Shortage_Date", "Projected_NAV": "NAV_at_First_Shortage"})
    )

    # Per-item list of customer + QB numbers that consume the item.
    so_for_users = so.copy()
    for col in ["Name", "QB Num", "Qty(-)"]:
        if col not in so_for_users.columns:
            so_for_users[col] = pd.NA
    so_for_users["Item"] = _norm_key(so_for_users["Item"])
    so_for_users["Name"] = so_for_users["Name"].fillna("").astype(str).str.strip()
    so_for_users["QB Num"] = so_for_users["QB Num"].fillna("").astype(str).str.strip()
    so_for_users["Qty(-)"] = pd.to_numeric(so_for_users["Qty(-)"], errors="coerce").fillna(0.0)

    item_users = so_for_users.loc[
        so_for_users["Qty(-)"] > 0, ["Item", "Name", "QB Num"]
    ].copy()
    item_users = item_users.loc[item_users["Name"].ne("") | item_users["QB Num"].ne("")]
    item_users["Customer_QB"] = np.where(
        item_users["Name"].ne("") & item_users["QB Num"].ne(""),
        item_users["Name"] + " (" + item_users["QB Num"] + ")",
        np.where(item_users["Name"].ne(""), item_users["Name"], item_users["QB Num"]),
    )
    if item_users.empty:
        item_users = pd.DataFrame(columns=["Item", "Customer_QB_List"])
    else:
        item_users = (
            item_users.sort_values(["Item", "QB Num", "Name"])
            .drop_duplicates(subset=["Item", "Customer_QB"])
            .groupby("Item", as_index=False)["Customer_QB"]
            .agg(", ".join)
            .rename(columns={"Customer_QB": "Customer_QB_List"})
        )

    inv_cols = pd.DataFrame(columns=["Item", "On Sales Order", "On PO"])
    if INVENTORY is not None and not INVENTORY.empty:
        inv = INVENTORY.copy()
        item_col = "Part_Number" if "Part_Number" in inv.columns else ("Item" if "Item" in inv.columns else None)
        if item_col is not None:
            inv["Item"] = _norm_key(inv[item_col])
            for c in ["On Sales Order", "On PO"]:
                if c not in inv.columns:
                    inv[c] = 0.0
                inv[c] = pd.to_numeric(inv[c], errors="coerce").fillna(0.0)
            inv_cols = (
                inv[["Item", "On Sales Order", "On PO"]]
                .groupby("Item", as_index=False)[["On Sales Order", "On PO"]]
                .sum()
            )

    item_summary = (stock.merge(item_min, on="Item", how="outer")
                         .merge(first_neg, on="Item", how="left")
                         .merge(item_users, on="Item", how="left")
                         .merge(inv_cols, on="Item", how="left"))
    item_summary["On Sales Order"] = pd.to_numeric(item_summary["On Sales Order"], errors="coerce").fillna(0.0)
    item_summary["On PO"] = pd.to_numeric(item_summary["On PO"], errors="coerce").fillna(0.0)
    item_summary["OK"] = item_summary["Min_Projected_NAV"].fillna(0) >= 0

    cutoff = pd.Timestamp("2026-07-04")

    mask = (
        (ledger["Projected_NAV"] < 0)               # real shortage
        & ledger["Date"].notna()
        & (ledger["Date"] != pd.Timestamp("2099-12-31"))   # ignore fake future rows
        & (ledger["Kind"].eq("OUT"))                # only consumption
        & (ledger["Source"].eq("SO"))               # only customer SOs
        & ~ledger["Item"].fillna("").str.startswith("Total ")  # drop roll-up lines
        & (ledger["Date"] < cutoff)                 # horizon filter
    )

    violations = (
    ledger.loc[mask]
    .sort_values(by="Date")
    .copy()
)



    ledger.sort_values(["Item","Date","Kind"], inplace=True, kind="mergesort")
    item_summary.sort_values(["OK","Min_Projected_NAV"], ascending=[True, True], inplace=True)
    return ledger, item_summary, violations


## 3) Reconciliation ADJ events
def build_reconcile_events(
    inv_db: pd.DataFrame,
    inv_wh: pd.DataFrame,
    *,
    as_of: pd.Timestamp | None = None,
    item_col_db: str = "Part_Number",
    item_col_wh: str = "Part_Number",
    onhand_col: str = "On Hand",
    mappings: dict | None = None,
    min_abs_delta: float = 0.0
) -> pd.DataFrame:
    """
    Compare DB 'Inventory Status' vs warehouse file and emit adjustment events
    where On Hand differs. Positive = IN, negative = OUT.
    Returns: DataFrame with columns [Date, Item, Delta, Kind, Source, Notes]
    """
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

    db_agg = (db.groupby("Item", as_index=False, sort=False)[onhand_col]
                .sum().rename(columns={onhand_col: "OnHand_DB"}))
    wh_agg = (wh.groupby("Item", as_index=False, sort=False)[onhand_col]
                .sum().rename(columns={onhand_col: "OnHand_WH"}))

    merged = db_agg.merge(wh_agg, on="Item", how="outer", validate="1:1")
    merged["OnHand_DB"] = pd.to_numeric(merged["OnHand_DB"], errors="coerce").fillna(0.0)
    merged["OnHand_WH"] = pd.to_numeric(merged["OnHand_WH"], errors="coerce").fillna(0.0)

    merged["Delta"] = merged["OnHand_WH"] - merged["OnHand_DB"]

    if min_abs_delta > 0:
        merged = merged.loc[merged["Delta"].abs() >= float(min_abs_delta)]

    if merged.empty:
        return pd.DataFrame(columns=["Date","Item","Delta","Kind","Source","Notes"])

    out = merged.loc[:, ["Item","Delta","OnHand_DB","OnHand_WH"]].copy()
    out.insert(0, "Date", adj_date)
    out["Kind"] = "ADJ"
    out["Source"] = "Reconcile"
    out["Notes"] = (
        "InvRecon: WH("
        + out["OnHand_WH"].astype(str)
        + ") - DB("
        + out["OnHand_DB"].astype(str)
        + ") = "
        + out["Delta"].astype(str)
    )
    return out.loc[:, ["Date","Item","Delta","Kind","Source","Notes"]]


def earliest_atp_by_projected_nav(
    ledger: pd.DataFrame,
    item: str,
    qty: float,
    from_date: pd.Timestamp | None = None,
) -> pd.Timestamp | None:
    """
    Earliest date where Projected_NAV for an item meets/exceeds qty.
    This is a simple "first time we reach the qty" check (no future min).
    """
    if ledger is None or ledger.empty:
        return None

    if from_date is None:
        from_date = pd.Timestamp.today().normalize()
    else:
        from_date = pd.to_datetime(from_date).normalize()

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
    df = df.loc[df["Date"].notna()]
    if df.empty:
        return None

    dummy_dates = {pd.Timestamp("2099-07-04"), pd.Timestamp("2099-12-31")}
    df = df.loc[~df["Date"].isin(dummy_dates)]
    if df.empty:
        return None

    df["Projected_NAV"] = pd.to_numeric(df["Projected_NAV"], errors="coerce")
    df = df.loc[df["Projected_NAV"].notna()]
    if df.empty:
        return None

    df = df.loc[df["Date"] >= from_date].sort_values("Date")
    if df.empty:
        return None

    candidates = df.loc[df["Projected_NAV"] >= qty_val, "Date"]
    if candidates.empty:
        return None
    return candidates.min()
