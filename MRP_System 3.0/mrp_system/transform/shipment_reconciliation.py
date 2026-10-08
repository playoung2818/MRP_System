"""Outstanding partial-shipment reconciliation against explicit MES WO flags."""
from .inventory import match_mes_quantities

REPORT_COLUMNS = ['QB Num', 'Item', 'Inventory Site', 'Open Qty', 'Shipped Qty',
                  'MES Shipped Qty', 'MES Unshipped Qty', 'Qty to reconcile']


def partial_shipment_report(so_full, mes_quantities):
    # Preserve the established definition: original Qty != Backordered.
    if 'partial' not in so_full or 'Shipped Qty' not in so_full:
        raise ValueError('SO partial-shipment information is required for reconciliation.')
    matched = match_mes_quantities(so_full, mes_quantities)
    matched['Qty to reconcile'] = matched['Shipped Qty'] - matched['MES Shipped Qty']
    drop_ship = matched['Inventory Site'].astype(str).str.strip().str.casefold().eq('drop ship')
    pending = matched[matched['partial'] & ~drop_ship & matched['Qty to reconcile'].abs().gt(0.000001)].copy()
    pending = pending.rename(columns={'Qty(-)': 'Open Qty'})
    return pending[REPORT_COLUMNS].sort_values(['QB Num', 'Item', 'Inventory Site']).reset_index(drop=True)
