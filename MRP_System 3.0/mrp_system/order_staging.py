"""Stage CLI SOs privately; publish to MES only with a confirmed production plan.

Uses existing sales_order_workflow. No table creation and no automatic approval.
"""
from collections import defaultdict, deque
from copy import deepcopy
import json
from uuid import uuid4
from mrp_system.so_workflow import order_signature, whole_quantity, next_state, allocation_conflicts, execution_limits, restore_fulfilled_lines, shipment_conflicts

import pandas as pd
from sqlalchemy import text
from mrp_system.transform.sales_order import normalize_wo_number
from mrp_system.order_scope import exclude_drop_ship_orders, is_drop_ship_order, is_nonproduction_item


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
    frame = frame.copy()
    frame['QB Num'] = frame['QB Num'].fillna('').astype(str).str.strip().map(normalize_wo_number)
    frame = frame.loc[~frame['Item'].map(is_nonproduction_item)]
    frame = exclude_drop_ship_orders(frame).astype(object).where(pd.notna(frame), '')
    orders = {}
    for _, row in frame.iterrows():
        number, item = normalize_wo_number(str(row['QB Num']).strip()), str(row['Item']).strip()
        remaining = whole_quantity(row['Qty'])
        shipped = whole_quantity(row.get('Shipped Qty', 0))
        qty = whole_quantity(row.get('Ordered Qty', remaining + shipped))
        if number.casefold() in ('', 'nan', 'none', 'null') or not item or remaining + shipped != qty:
            raise ValueError('SO staging rejected inconsistent ordered/shipped/remaining quantities; prior snapshots preserved.')
        ship = pd.to_datetime(row.get('Lead Time', ''), errors='coerce')
        source = orders.setdefault(number, dict(sales_order=number, customer=str(row.get('Customer', '')),
            customer_po=str(row.get('Customer PO', '')), ship_date='', terms=str(row.get('Terms', '')),
            inventory_site=str(row.get('Inventory Site', '')), items=[]))
        if pd.notna(ship):
            source['ship_date'] = ship.strftime('%Y-%m-%d')
        source['items'].append(dict(item=item, quantity=qty, ordered_quantity=qty,
            remaining_quantity=remaining, shipped_quantity=shipped,
            configuration=str(row.get('Configuration', row.get('Description', ''))),
            inventory_site=str(row.get('Inventory Site', ''))))
    return orders


def stage_cli_orders(engine, frame):
    if 'QB Num' in frame:
        frame = frame.copy()
        frame['QB Num'] = frame['QB Num'].fillna('').astype(str).str.strip().map(normalize_wo_number)
    orders = cli_sources(frame)  # Validate everything before touching shared rows.
    production = frame.loc[~frame['Item'].map(is_nonproduction_item)]
    eligible = exclude_drop_ship_orders(production)
    excluded_keys = set(frame['QB Num'].astype(str).str.strip()) - set(eligible['QB Num'].astype(str).str.strip())
    with engine.begin() as conn:
        conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
        existing = {r['sales_order']: r for r in conn.execute(text('SELECT * FROM public.sales_order_workflow FOR UPDATE')).mappings()}
        wo_by_so = defaultdict(list)
        for wo in conn.execute(text('SELECT sales_order, items, is_shipped FROM public."WO Details" WHERE record_source=\'mes\'')).mappings():
            wo_by_so[wo['sales_order']].append(wo)
        for number in sorted(set(existing) | set(orders)):
            old = existing.get(number)
            if number not in orders:
                prior = old.get('latest_snapshot') or old.get('previous_snapshot') or old.get('approved_snapshot') or {}
                proven = restore_fulfilled_lines({'items': []}, prior, wo_by_so[number])
                fulfilled = bool(old.get('workflow_status') == 'released_to_mes' and old.get('approved_order_signature')
                                 and not old.get('reconciliation_required')
                                 and not allocation_conflicts(
                                     [{'item': name, 'quantity': qty} for name, qty in (old.get('approved_order_signature') or {}).items()],
                                     [i for wo in wo_by_so[number] for i in wo['items'] or []])
                                 and order_signature(proven) == old['approved_order_signature'])
                # Missing from an open-order export is NOT proof of cancellation.
                # Retain ownership and records, but block new releases pending disposition.
                conn.execute(text('''UPDATE public.sales_order_workflow SET previous_snapshot=COALESCE(latest_snapshot, previous_snapshot),
                    latest_snapshot=NULL, source_present=FALSE,
                    workflow_status=CASE WHEN :excluded THEN 'closed' ELSE workflow_status END,
                    synced_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
                    source_revision=source_revision + CASE WHEN source_present THEN 1 ELSE 0 END
                    WHERE sales_order=:so'''), {'so': number, 'excluded': fulfilled or number in excluded_keys or is_drop_ship_order(
                        old.get('latest_snapshot') or old.get('previous_snapshot') or old.get('approved_snapshot'))})
                continue
            prior = old['latest_snapshot'] or old['approved_snapshot'] or {'items': old['items']} if old else None
            incoming = staged_snapshot(restore_fulfilled_lines(orders[number], prior, wo_by_so[number]), prior)
            changed = not old or old['latest_snapshot'] != incoming
            status, returned = next_state(old, incoming)
            incident = (str(uuid4()) if not old or status != old.get('workflow_status')
                        else str(old['review_incident_id']) if old.get('review_incident_id') else str(uuid4()))
            conn.execute(text('''INSERT INTO public.sales_order_workflow
                (sales_order, items, metadata_fields, previous_snapshot, latest_snapshot,
                 source_present, synced_at, source_revision, revision,
                 workflow_status, returned_from_mes, review_incident_id)
                VALUES (:so, '[]', '{}', NULL, CAST(:snapshot AS json), TRUE,
                        CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 1, 0, :status, :returned, CAST(:incident AS uuid))
                ON CONFLICT (sales_order) DO UPDATE SET previous_snapshot=COALESCE(sales_order_workflow.latest_snapshot, sales_order_workflow.previous_snapshot),
                    latest_snapshot=EXCLUDED.latest_snapshot, source_present=TRUE,
                    workflow_status=:status, returned_from_mes=:returned,
                    review_incident_id=CAST(:incident AS uuid),
                    synced_at=EXCLUDED.synced_at,
                    source_revision=sales_order_workflow.source_revision + :changed,
                    metadata_fields=CASE WHEN :status = 'released_to_mes'
                        THEN CAST(CAST(sales_order_workflow.metadata_fields AS jsonb) || jsonb_build_object('ship_date', :ship) AS json)
                        ELSE sales_order_workflow.metadata_fields END'''),
                {'so': number, 'snapshot': json.dumps(incoming), 'changed': int(changed),
                 'status': status, 'returned': returned, 'incident': incident, 'ship': incoming.get('ship_date', '')})
    return len(orders)


def publish_completed_order(conn, number, notes=None, allow_reconciliation=False):
    """Caller owns transaction: publishing + schedule + planning history are atomic."""
    conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
    row = conn.execute(text('SELECT * FROM public.sales_order_workflow WHERE sales_order=:so FOR UPDATE'), {'so': number}).mappings().first()
    if row is None or not row['source_present'] or not row['latest_snapshot']:
        raise ValueError('Run MRP CLI to stage this SO before completing production planning.')
    incoming = deepcopy(row['latest_snapshot'])
    incoming['items'] = [i for i in incoming['items'] if not is_nonproduction_item(i.get('item'))
                         and str(i.get('inventory_site') or incoming.get('inventory_site') or '').strip().casefold() != 'drop ship']
    if not incoming['items']:
        raise ValueError('This SO has no production items to publish (Drop Ship/non-production lines excluded).')
    if is_drop_ship_order(incoming):
        raise ValueError('Drop Ship sales orders are excluded from MRP and MES production planning.')
    by_id = {i['id']: deepcopy(i) for i in incoming['items']}
    items = []
    # Preserve MRP-reviewed order/manual rows while merging the latest source.
    for item in row['items'] or []:
        if (is_nonproduction_item(item.get('item')) or
                str(item.get('inventory_site') or incoming.get('inventory_site') or '').strip().casefold() == 'drop ship'):
            continue
        if item.get('id') in by_id:
            items.append(by_id.pop(item['id']))
        elif str(item.get('id', '')).startswith('manual-'):
            items.append(deepcopy(item))
    items.extend(by_id.values())
    wo_records = list(conn.execute(text('SELECT items, is_shipped FROM public."WO Details" WHERE sales_order=:so AND record_source=\'mes\''), {'so': number}).mappings())
    saved_items = [i for wo in wo_records for i in wo['items'] or []]
    limits = execution_limits(items, wo_records, incoming)
    conflicts = allocation_conflicts([{'item': name, 'quantity': qty} for name, qty in limits.items()], saved_items)
    if conflicts and not allow_reconciliation:
        raise ValueError('SO quantities are below saved WO allocations. Resolve them before publication.')
    old_quantities = row.get('approved_order_signature') or order_signature(row.get('approved_snapshot') or {'items': row.get('items') or []})
    new_quantities = order_signature(incoming)
    finished = conn.execute(text('SELECT is_finished_goods FROM public.production_overrides WHERE wo_number=:so'), {'so': number}).scalar_one_or_none()
    fg_review = bool(finished is True and allow_reconciliation and any(
        qty > old_quantities.get(name, 0) for name, qty in new_quantities.items()))
    shipping_issues = shipment_conflicts(items, wo_records, incoming)
    details = {'allocations': conflicts, 'finished_goods': fg_review, 'shipments': shipping_issues}
    metadata = dict(row.get('metadata_fields') or {})
    metadata.update({k: incoming.get(k, '') for k in ('ship_date', 'terms', 'inventory_site')})
    if notes is not None:
        metadata['notes'] = notes
    conn.execute(text('''UPDATE public.sales_order_workflow SET customer=:customer, customer_po=:po,
        items=CAST(:items AS json), metadata_fields=CAST(:metadata AS json),
        approved_snapshot=CAST(:snapshot AS json), approved_at=CURRENT_TIMESTAMP AT TIME ZONE 'UTC',
        workflow_status='released_to_mes', returned_from_mes=FALSE,
        approved_order_signature=CAST(:signature AS jsonb),
        reconciliation_required=:reconcile, reconciliation_details=CAST(:details AS jsonb),
        mes_released_at=COALESCE(mes_released_at, CURRENT_TIMESTAMP),
        revision=revision+1 WHERE sales_order=:so'''),
        {'so': number, 'customer': incoming.get('customer', ''), 'po': incoming.get('customer_po', ''),
         'items': json.dumps(items), 'metadata': json.dumps(metadata),
         'snapshot': json.dumps(incoming), 'signature': json.dumps(new_quantities),
         'reconcile': bool(conflicts or fg_review or shipping_issues), 'details': json.dumps(details)})
