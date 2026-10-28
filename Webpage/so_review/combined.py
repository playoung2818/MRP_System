"""SO-content review inside the same transaction as material approval."""
from collections import defaultdict
from copy import deepcopy
from types import SimpleNamespace
import json

from sqlalchemy import text
from .service import _normalize_review_rows, review_items, is_drop_ship_order


def warehouse_source(source):
    from mrp_system.order_scope import is_nonproduction_item
    source = deepcopy(source or {})
    source['items'] = [i for i in source.get('items', [])
                       if not is_nonproduction_item(i.get('item'))
                       and str(i.get('inventory_site') or source.get('inventory_site') or '').strip().casefold() != 'drop ship']
    return source


def quantity_signature(source):
    from mrp_system.so_workflow import order_signature
    return order_signature(warehouse_source(source))


def quantity_changes(before, after):
    return [f'{name}: {before.get(name, 0)} → {after.get(name, 0)} ordered'
            for name in sorted(set(before) | set(after)) if before.get(name, 0) != after.get(name, 0)]


def content_context(row):
    if not row or is_drop_ship_order(row['latest_snapshot']):
        return None
    row = dict(row)
    row['latest_snapshot'] = warehouse_source(row['latest_snapshot'])
    row['items'] = warehouse_source({'items': row.get('items') or []})['items']
    return {'source': row['latest_snapshot'] or {}, 'source_revision': row['source_revision'],
            'revision': row['revision'], 'items': review_items(SimpleNamespace(**dict(row))),
            'changes': quantity_changes(row.get('approved_order_signature') or quantity_signature(row['approved_snapshot']), quantity_signature(row['latest_snapshot'])),
            'sync_changes': quantity_changes(quantity_signature(row['previous_snapshot']), quantity_signature(row['latest_snapshot']))}


def review_content(conn, number, payload):
    conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
    row = conn.execute(text('SELECT * FROM public.sales_order_workflow WHERE sales_order=:so FOR UPDATE'), {'so': number}).mappings().first()
    if not row or not row['source_present'] or not row['latest_snapshot']:
        raise ValueError('Run MRP CLI to stage this sales order before reviewing it.')
    if is_drop_ship_order(row['latest_snapshot']):
        raise ValueError('Drop Ship sales orders are excluded from MRP and MES production planning.')
    row = dict(row)
    row['latest_snapshot'] = warehouse_source(row['latest_snapshot'])
    try:
        versions_match = int(payload['source_revision']) == row['source_revision'] and int(payload['revision']) == row['revision']
    except (KeyError, TypeError, ValueError):
        versions_match = False
    if not versions_match:
        raise ValueError('Sales-order content changed since opening this review. Reload and review again.')
    items = _normalize_review_rows(payload.get('items'))
    source_items = {item['id']: item for item in row['latest_snapshot']['items']}
    if {i['id'] for i in items if not i['id'].startswith('manual-')} != set(source_items):
        raise ValueError('The imported SO item list changed. Reload before approval.')
    for item in items:
        if item['id'] in source_items:
            original = source_items[item['id']]
            if any(item.get(k, '') != original.get(k, '') for k in ('item', 'quantity', 'configuration', 'inventory_site')):
                raise ValueError('Imported SO lines are read-only. Correct the source and rerun MRP if needed.')
    planned, saved = defaultdict(int), defaultdict(int)
    for item in items:
        planned[item['item'].strip().casefold()] += item['quantity']
    for wo in conn.execute(text('SELECT items FROM public."WO Details" WHERE sales_order=:so AND record_source=\'mes\''), {'so': number}).mappings():
        for item in wo['items'] or []:
            saved[item['item'].strip().casefold()] += int(item.get('quantity', 0))
    if not row.get('returned_from_mes') and any(qty > planned.get(item, 0) for item, qty in saved.items()):
        raise ValueError('SO quantities are below saved WO allocations. Resolve them before approval.')
    incoming = row['latest_snapshot']
    conn.execute(text('''UPDATE public.sales_order_workflow SET items=CAST(:items AS json),
        customer=:customer, customer_po=:po, metadata_fields=CAST(:metadata AS json),
        approved_snapshot=CAST(:snapshot AS json), approved_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
        revision=revision+1 WHERE sales_order=:so'''),
        {'so': number, 'items': json.dumps(items), 'customer': incoming.get('customer', ''),
         'po': incoming.get('customer_po', ''), 'metadata': json.dumps(dict(row.get('metadata_fields') or {}, **{k: incoming.get(k, '') for k in ('ship_date', 'terms', 'inventory_site')})),
         'snapshot': json.dumps(incoming)})
