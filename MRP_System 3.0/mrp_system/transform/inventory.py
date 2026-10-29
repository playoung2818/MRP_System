from __future__ import annotations

import pandas as pd

from mrp_system.normalize.mrp_normalize import normalize_item

from .sales_order import normalize_wo_number


WIP_SITE = "WH01S-NTA"


def build_wip_lookup(so_full: pd.DataFrame, mes_quantities: pd.DataFrame, site: str = WIP_SITE) -> pd.DataFrame:
    """Warehouse WIP comes exclusively from saved MES WO quantities."""
    lines = match_mes_quantities(so_full, mes_quantities)
    lines = lines[lines['Inventory Site'].eq(site) & lines['WIP_Qty'].gt(0)]
    if lines.empty:
        return pd.DataFrame(columns=['Part_Number', 'WIP', 'WIP_Qty'])
    return lines.groupby('Item', as_index=False).agg(
        WIP_Qty=('WIP_Qty', 'sum'),
        WIP=('QB Num', lambda s: ', '.join(pd.unique(s.astype(str))))).rename(columns={'Item': 'Part_Number'})


def transform_inventory(inventory_df: pd.DataFrame, wip_lookup: pd.DataFrame | None = None) -> pd.DataFrame:
    inv = inventory_df.copy()
    inv = inv.rename(columns={"Unnamed: 0": "Part_Number"})
    inv["Part_Number"] = inv["Part_Number"].astype(str).str.strip().map(normalize_item)
    for c in ["On Hand", "On Sales Order", "On PO", "Available", "On Hand - WIP", "WIP_Qty"]:
        if c in inv.columns:
            inv[c] = pd.to_numeric(inv[c], errors="coerce").fillna(0)

    if wip_lookup is not None:
        wip = wip_lookup.copy()
        if "Part_Number" not in wip.columns and "Item" in wip.columns:
            wip["Part_Number"] = wip["Item"]
        wip["Part_Number"] = wip["Part_Number"].astype(str).str.strip().map(normalize_item)
        wip = wip[["Part_Number", "WIP", "WIP_Qty"]].drop_duplicates(subset=["Part_Number"])
        # MES facts replace stale spreadsheet WIP, including an empty/zero result.
        inv = inv.drop(columns=["WIP", "WIP_Qty"], errors="ignore").merge(wip, on="Part_Number", how="left")

    if "WIP" not in inv.columns:
        inv["WIP"] = ""
    inv["WIP"] = inv["WIP"].fillna("")
    if "WIP_Qty" not in inv.columns:
        inv["WIP_Qty"] = 0
    inv["WIP_Qty"] = pd.to_numeric(inv["WIP_Qty"], errors="coerce").fillna(0)
    # Always derive this from On Hand - WIP_Qty. The old code defaulted a missing
    # "On Hand - WIP" straight to On Hand, which made it a real (non-NaN) value before
    # the fillna(On Hand - WIP_Qty) below ever ran — so that subtraction was dead code
    # and WIP_Qty was silently ignored here.
    inv["On Hand - WIP"] = inv["On Hand"] - inv["WIP_Qty"]
    return inv


def match_mes_quantities(so_full, quantities):
    """Count only saved MES releases explicitly marked not shipped.

    Never subtract invoice shipments again: shipped releases are already excluded.

    Missing legacy sites are inferred only when the open SO/item has one site.
    Finished Goods flags never participate in this calculation.
    """
    sales = so_full.copy()
    sales['QB Num'] = sales['QB Num'].astype(str).map(normalize_wo_number)
    sales['Item'] = sales['Item'].astype(str).str.strip().map(normalize_item)
    sales['Inventory Site'] = sales['Inventory Site'].fillna('').astype(str).str.strip()
    keys = ['QB Num', 'Item', 'Inventory Site']
    if 'Shipped Qty' not in sales:
        sales['Shipped Qty'] = 0
    sales['Qty(-)'] = pd.to_numeric(sales['Qty(-)'], errors='raise')
    sales['Shipped Qty'] = pd.to_numeric(sales['Shipped Qty'], errors='raise')
    if 'partial' not in sales:
        sales['partial'] = False
    if 'Ordered Qty' not in sales:
        # Compatibility for older callers; source reductions are not MES shipments.
        sales['Ordered Qty'] = sales['Qty(-)'] + sales['Shipped Qty']
    sales['Ordered Qty'] = pd.to_numeric(sales['Ordered Qty'], errors='raise')
    sales = sales.groupby(keys, as_index=False).agg({
        'Qty(-)': 'sum', 'Ordered Qty': 'sum', 'Shipped Qty': 'sum', 'partial': 'any'})
    mes = quantities.rename(columns={'sales_order': 'QB Num', 'item': 'Item', 'inventory_site': 'Inventory Site'}).copy()
    # Mirror MES's exact raw-reference filter. Do not normalize an intentionally
    # excluded/returned WO (for example a trailing apostrophe) back into WIP.
    mes = mes[mes['QB Num'].isin(sales['QB Num'])].copy()
    mes['QB Num'] = mes['QB Num'].astype(str).map(normalize_wo_number)
    mes['Item'] = mes['Item'].astype(str).str.strip().map(normalize_item)
    mes['Inventory Site'] = mes['Inventory Site'].fillna('').astype(str).str.strip()
    for index, row in mes[mes['Inventory Site'].eq('')].iterrows():
        sites = sales.loc[sales['QB Num'].eq(row['QB Num']) & sales['Item'].eq(row['Item']), 'Inventory Site'].unique()
        if len(sites) > 1:
            raise ValueError(f"Missing MES warehouse for {row['QB Num']} / {row['Item']}")
        if len(sites) == 1:
            mes.at[index, 'Inventory Site'] = sites[0]
    for column in ('quantity', 'shipped_quantity'):
        mes[column] = pd.to_numeric(mes[column], errors='raise')
    mes = mes.groupby(keys, as_index=False)[['quantity', 'shipped_quantity']].sum()
    sales = sales.merge(mes, on=keys, how='left')
    sales['MES Shipped Qty'] = sales['shipped_quantity'].fillna(0)
    sales['MES Unshipped Qty'] = sales['quantity'].fillna(0)
    sales['WIP_Qty'] = sales['MES Unshipped Qty'].clip(lower=0)
    # Source outstanding can represent awaiting-pick demand, not a physical-stock
    # limit. Never cap saved unshipped releases against it or subtract invoices.
    sales['MES Picked Qty'] = sales['MES Unshipped Qty'] + sales['MES Shipped Qty']
    sales['Awaiting Pick Qty'] = (sales['Ordered Qty'] - sales['MES Picked Qty']).clip(lower=0)
    sales['Execution Issue'] = sales['MES Picked Qty'].gt(sales['Ordered Qty'])
    return sales.drop(columns=['quantity', 'shipped_quantity'])


EXECUTION_COLUMNS = [
    'SO Ordered Qty', 'SO WIP Qty', 'Physically Shipped Qty', 'Awaiting Pick Qty',
    'Source Outstanding Qty', 'Execution Issue', 'Picked_Flag', 'Picked',
]


def execution_metrics(so_full, quantities):
    """One physical execution summary per SO/item/site, separate from source demand."""
    matched = match_mes_quantities(so_full, quantities)
    matched['Picked_Flag'] = matched['MES Picked Qty'].gt(0)
    matched['Picked'] = 'No'
    matched.loc[matched['Picked_Flag'], 'Picked'] = 'Picked'
    matched.loc[matched['Picked_Flag'] & matched['Awaiting Pick Qty'].gt(0), 'Picked'] = 'Partial'
    matched = matched.rename(columns={
        'Ordered Qty': 'SO Ordered Qty', 'WIP_Qty': 'SO WIP Qty',
        'MES Shipped Qty': 'Physically Shipped Qty', 'Qty(-)': 'Source Outstanding Qty',
    })
    return matched[['QB Num', 'Item', 'Inventory Site', 'partial', *EXECUTION_COLUMNS]]


__all__ = ['build_wip_lookup', 'transform_inventory', 'match_mes_quantities',
           'execution_metrics', 'EXECUTION_COLUMNS', 'WIP_SITE']
