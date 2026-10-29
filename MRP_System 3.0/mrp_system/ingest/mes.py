"""MES saved quantities, scoped to current open SOs. No legacy API fallback."""
import os
import math
import pandas as pd
import requests
from mrp_system.transform.sales_order import normalize_wo_number

COLUMNS = ['sales_order', 'item', 'inventory_site', 'quantity', 'shipped_quantity']


def quantities_from_saved_wos(records, sales_orders):
    """Read-only DB adapter for the same v2 facts as MES's quantity endpoint.

    Match raw saved SO references exactly, before any downstream normalization.
    Intentional suffixes (including apostrophes) must not reactivate returned WOs.
    """
    numbers = {normalize_wo_number(str(number).strip()) for number in sales_orders}
    totals = {}
    for record in records:
        number = record['sales_order']
        if number not in numbers or record.get('record_source', 'mes') != 'mes':
            continue
        for item in record['items'] or []:
            qty = max(0, int(item.get('quantity') or 0))
            if not qty:
                continue
            key = (str(number).strip(), str(item.get('item') or '').strip(),
                   str(item.get('inventory_site') or '').strip())
            values = totals.setdefault(key, [0, 0])
            values[1 if record['is_shipped'] else 0] += qty
    return pd.DataFrame([
        dict(sales_order=so, item=item, inventory_site=site,
             quantity=values[0], shipped_quantity=values[1])
        for (so, item, site), values in sorted(totals.items())
    ], columns=COLUMNS)


def fetch_mes_quantities(sales_orders):
    url = os.environ.get('MES_WO_QUANTITIES_URL', 'http://192.168.60.215:5001/api/wo-quantities')
    numbers = sorted({normalize_wo_number(str(number).strip()) for number in sales_orders if str(number).strip()})
    rows = []
    for offset in range(0, len(numbers), 100):
        batch = numbers[offset:offset + 100]
        response = requests.get(url, params=[('sales_order', number) for number in batch], timeout=15)
        response.raise_for_status()
        payload = response.json()
        if payload.get('schema_version') != 2 or not isinstance(payload.get('quantities'), list):
            raise ValueError('MES shipment-aware WO API v2 is required. Restart/update MES.')
        for row in payload['quantities']:
            for field in ('quantity', 'shipped_quantity'):
                qty = float(row[field])
                if not math.isfinite(qty) or qty < 0:
                    raise ValueError('Invalid MES quantity row')
            if row['sales_order'] not in batch or not row['item']:
                raise ValueError('Unexpected MES sales order or empty item')
            rows.append(row)
    return pd.DataFrame(rows, columns=COLUMNS)
