"""Outstanding partial-shipment reconciliation against explicit MES WO flags."""
from .inventory import match_mes_quantities
from .sales_order import normalize_wo_number

REPORT_COLUMNS = ['QB Num', 'Customer']


def partial_shipment_report(so_full, mes_quantities):
    # Preserve the established definition: original Qty != Backordered.
    if 'partial' not in so_full or 'Shipped Qty' not in so_full:
        raise ValueError('SO partial-shipment information is required for reconciliation.')
    matched = match_mes_quantities(so_full, mes_quantities)
    matched['Qty to reconcile'] = matched['Shipped Qty'] - matched['MES Shipped Qty']
    drop_ship = matched['Inventory Site'].astype(str).str.strip().str.casefold().eq('drop ship')
    pending = matched[matched['partial'] & ~drop_ship & matched['Qty to reconcile'].abs().gt(0.000001)].copy()
    # Matching aggregates item/site rows, so retrieve customer names from source.
    customers = so_full.reindex(columns=['QB Num', 'Name']).rename(columns={'Name': 'Customer'})
    customers['QB Num'] = customers['QB Num'].astype(str).map(normalize_wo_number)
    customers['Customer'] = customers['Customer'].fillna('').astype(str).str.strip()
    customers = customers.loc[customers['Customer'].ne('')].drop_duplicates('QB Num')
    orders = pending[['QB Num']].drop_duplicates().merge(customers, on='QB Num', how='left')
    orders['Customer'] = orders['Customer'].fillna('')
    return orders[REPORT_COLUMNS].sort_values('QB Num').reset_index(drop=True)
