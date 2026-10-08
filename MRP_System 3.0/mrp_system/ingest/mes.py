"""MES saved quantities, scoped to current open SOs. No legacy API fallback."""
import os
import math
import pandas as pd
import requests
from mrp_system.transform.sales_order import normalize_wo_number

COLUMNS = ['sales_order', 'item', 'inventory_site', 'quantity', 'shipped_quantity']


def fetch_mes_quantities(sales_orders):
    url = os.environ.get('MES_WO_QUANTITIES_URL', 'http://localhost:5001/api/wo-quantities')
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
