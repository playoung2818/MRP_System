"""Stage CLI SOs privately; publish to MES only with a confirmed production plan.

Uses existing approved_work_orders. No table creation and no automatic approval.
"""
from collections import defaultdict, deque
from copy import deepcopy
import json
import math
from uuid import uuid4

import pandas as pd
from sqlalchemy import text
from mrp_system.order_scope import exclude_drop_ship_orders, is_drop_ship_order


def _identity(item):
    return (str(item.get('item') or '').strip().casefold(), str(item.get('quantity') or '').strip(),
            str(item.get('inventory_site') or '').strip().casefold())


def staged_snapshot(source, prior):
    pools = defaultdict(deque)
    for item in (prior or {}).get('items', []):
        pools[_identity(item)].append(item['id'])
    incoming = deepcopy(source)
    used = set()
    for item in incoming['items']:
        item['id'] = pools[_identity(item)].popleft() if pools[_identity(item)] else None
        if item['id']:
            used.add(item['id'])
    old, new = defaultdict(list), defaultdict(list)
    for item in (prior or {}).get('items', []):
        if item['id'] not in used:
            old[item['item'].strip().casefold()].append(item['id'])
    for item in incoming['items']:
        if not item['id']:
            new[item['item'].strip().casefold()].append(item)
    for product, items in new.items():
        if len(items) == len(old[product]) == 1:
            items[0]['id'] = old[product][0]
        else:
            for item in items:
                item['id'] = str(uuid4())
    return incoming


def cli_sources(frame):
    required = {'QB Num', 'Item', 'Qty'}
    if frame.empty or not required.issubset(frame):
        raise ValueError('SO staging requires a complete, nonempty CLI sales-order output.')
    frame = exclude_drop_ship_orders(frame).astype(object).where(pd.notna(frame), '')
    orders = {}
    for _, row in frame.iterrows():
        number, item = str(row['QB Num']).strip(), str(row['Item']).strip()
        qty = float(row['Qty'])
        if not number or not item or not math.isfinite(qty) or qty < 0 or not qty.is_integer():
            raise ValueError('SO staging rejected invalid identity or non-whole quantity; prior snapshots preserved.')
        ship = pd.to_datetime(row.get('Lead Time', ''), errors='coerce')
        source = orders.setdefault(number, dict(sales_order=number, customer=str(row.get('Customer', '')),
            customer_po=str(row.get('Customer PO', '')), ship_date='', terms=str(row.get('Terms', '')),
            inventory_site=str(row.get('Inventory Site', '')), items=[]))
        if pd.notna(ship):
            source['ship_date'] = ship.strftime('%Y-%m-%d')
        source['items'].append(dict(item=item, quantity=int(qty),
            configuration=str(row.get('Configuration', row.get('Description', ''))),
            inventory_site=str(row.get('Inventory Site', ''))))
    return orders


def stage_cli_orders(engine, frame):
    orders = cli_sources(frame)  # Validate everything before touching shared rows.
    with engine.begin() as conn:
        conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
        existing = {r['sales_order']: r for r in conn.execute(text('SELECT * FROM public.approved_work_orders FOR UPDATE')).mappings()}
        for number in sorted(set(existing) | set(orders)):
            old = existing.get(number)
            if number not in orders:
                conn.execute(text('''UPDATE public.approved_work_orders SET previous_snapshot=COALESCE(latest_snapshot, previous_snapshot),
                    latest_snapshot=NULL, google_present=FALSE, is_active=FALSE,
                    synced_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                    source_revision=source_revision + CASE WHEN google_present THEN 1 ELSE 0 END
                    WHERE sales_order=:so'''), {'so': number})
                continue
            prior = old['latest_snapshot'] or old['approved_snapshot'] or {'items': old['items']} if old else None
            incoming = staged_snapshot(orders[number], prior)
            changed = not old or old['latest_snapshot'] != incoming
            conn.execute(text('''INSERT INTO public.approved_work_orders
                (sales_order, items, metadata_fields, previous_snapshot, latest_snapshot,
                 google_present, is_active, synced_at, source_revision, revision)
                VALUES (:so, '[]', '{}', NULL, CAST(:snapshot AS json), TRUE, FALSE,
                        CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 1, 0)
                ON CONFLICT (sales_order) DO UPDATE SET previous_snapshot=approved_work_orders.latest_snapshot,
                    latest_snapshot=EXCLUDED.latest_snapshot, google_present=TRUE,
                    is_active=CASE WHEN approved_work_orders.google_present THEN approved_work_orders.is_active ELSE FALSE END,
                    synced_at=EXCLUDED.synced_at,
                    source_revision=approved_work_orders.source_revision + :changed'''),
                {'so': number, 'snapshot': json.dumps(incoming), 'changed': int(changed)})
    return len(orders)


def publish_completed_order(conn, number, notes=None):
    """Caller owns transaction: publishing + schedule + planning history are atomic."""
    conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
    row = conn.execute(text('SELECT * FROM public.approved_work_orders WHERE sales_order=:so FOR UPDATE'), {'so': number}).mappings().first()
    if row is None or not row['google_present'] or not row['latest_snapshot']:
        raise ValueError('Run MRP CLI to stage this SO before completing production planning.')
    incoming = row['latest_snapshot']
    if is_drop_ship_order(incoming):
        raise ValueError('Drop Ship sales orders are excluded from MRP and MES production planning.')
    by_id = {i['id']: deepcopy(i) for i in incoming['items']}
    items = []
    # Preserve MRP-reviewed order/manual rows while merging the latest source.
    for item in row['items'] or []:
        if item.get('id') in by_id:
            items.append(by_id.pop(item['id']))
        elif str(item.get('id', '')).startswith('manual-'):
            items.append(deepcopy(item))
    items.extend(by_id.values())
    saved, planned = defaultdict(int), defaultdict(int)
    for wo in conn.execute(text('SELECT items FROM public."WO Details" WHERE sales_order=:so AND record_source=\'mes\''), {'so': number}).mappings():
        for item in wo['items'] or []:
            saved[item['item'].strip().casefold()] += int(item.get('quantity', 0))
    for item in items:
        planned[item['item'].strip().casefold()] += int(item['quantity'])
    if any(qty > planned.get(item, 0) for item, qty in saved.items()):
        raise ValueError('SO quantities are below saved WO allocations. Resolve them before publication.')
    metadata = dict(row.get('metadata_fields') or {})
    metadata.update({k: incoming.get(k, '') for k in ('ship_date', 'terms', 'inventory_site')})
    if notes is not None:
        metadata['notes'] = notes
    conn.execute(text('''UPDATE public.approved_work_orders SET customer=:customer, customer_po=:po,
        items=CAST(:items AS json), metadata_fields=CAST(:metadata AS json),
        approved_snapshot=CAST(:snapshot AS json), approved_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
        is_active=TRUE, revision=revision+1 WHERE sales_order=:so'''),
        {'so': number, 'customer': incoming.get('customer', ''), 'po': incoming.get('customer_po', ''),
         'items': json.dumps(items), 'metadata': json.dumps(metadata),
         'snapshot': json.dumps(incoming)})
