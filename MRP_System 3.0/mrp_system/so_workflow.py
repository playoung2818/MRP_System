"""One current SO owner; date and fulfillment changes are not order amendments."""
from collections import defaultdict
from decimal import Decimal, InvalidOperation
from copy import deepcopy
from mrp_system.order_scope import is_nonproduction_item

REVIEW = 'needs_material_review'
PLANNING = 'needs_production_planning'
MES = 'released_to_mes'
CLOSED = 'closed'


def whole_quantity(value):
    try:
        qty = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError('SO quantities must be finite nonnegative whole numbers.') from None
    if not qty.is_finite() or qty < 0 or qty != qty.to_integral_value():
        raise ValueError('SO quantities must be finite nonnegative whole numbers.')
    return int(qty)


def order_signature(source):
    """Compare total ordered quantities, not remaining quantities, IDs or dates.

    Aggregate repeated product lines: reallocating identical products between
    source lines without changing the customer's total is not an amendment.
    """
    quantities = defaultdict(int)
    for item in (source or {}).get('items', []):
        if is_nonproduction_item(item.get('item')) or str(item.get('id', '')).startswith('manual-'):
            continue
        name = str(item.get('item') or '').strip().casefold()
        if not name:
            raise ValueError('SO items must have a product name.')
        quantities[name] += whole_quantity(item.get('ordered_quantity', item.get('quantity')))
    return dict(sorted(quantities.items()))


def next_state(old, incoming):
    if not old:
        return REVIEW, False
    status = old.get('workflow_status') or REVIEW
    if status == CLOSED and not old.get('source_present'):
        return REVIEW, bool(old.get('mes_released_at'))
    baseline = old.get('approved_order_signature')
    changed = baseline is not None and baseline != order_signature(incoming)
    if status in (MES, PLANNING, CLOSED) and changed:
        return REVIEW, bool(old.get('mes_released_at') or status == MES or old.get('returned_from_mes'))
    return status, bool(old.get('returned_from_mes'))


def shipment_totals(wo_records):
    result = defaultdict(int)
    for wo in wo_records:
        if wo.get('is_shipped'):
            for item in wo['items'] or []:
                if not is_nonproduction_item(item.get('item')):
                    result[str(item['item']).strip().casefold()] += whole_quantity(item['quantity'])
    return dict(result)


def restore_fulfilled_lines(incoming, prior, wo_records):
    """An open-order export may omit fully shipped lines. Preserve proven fulfillment.

    Without shipment evidence, disappearance remains an item-change exception;
    never guess that an unrecorded shipment happened.
    """
    result = deepcopy(incoming)
    present = set(order_signature(incoming))
    prior = prior or {}
    required = order_signature(prior)
    shipped = shipment_totals(wo_records)
    imported = defaultdict(int)
    for item in prior.get('items', []):
        imported[str(item['item']).strip().casefold()] += whole_quantity(item.get('shipped_quantity', 0))
    for item in prior.get('items', []):
        name = str(item['item']).strip().casefold()
        if name not in present and name in required and max(shipped.get(name, 0), imported[name]) >= required[name]:
            completed = deepcopy(item)
            completed.update(remaining_quantity=0, shipped_quantity=whole_quantity(item.get('ordered_quantity', item['quantity'])))
            result['items'].append(completed)
    return result


def execution_limits(items, wo_records, source):
    """Account for legacy shipments outside MES without subtracting MES shipments twice."""
    ordered, imported_shipped, mes_shipped, allocated = (defaultdict(int) for _ in range(4))
    for item in items:
        if not is_nonproduction_item(item.get('item')):
            ordered[str(item['item']).strip().casefold()] += whole_quantity(item['quantity'])
    for item in (source or {}).get('items', []):
        imported_shipped[str(item['item']).strip().casefold()] += whole_quantity(item.get('shipped_quantity', 0))
    for wo in wo_records:
        for item in wo['items'] or []:
            name = str(item['item']).strip().casefold()
            allocated[name] += whole_quantity(item['quantity'])
            if wo.get('is_shipped'):
                mes_shipped[name] += whole_quantity(item['quantity'])
    # If source shipments overlap unshipped-marked WOs, we cannot know which
    # quantities were external. Keep the original WO ceiling; MES separately
    # blocks new picking with a shipment-reconciliation warning rather than
    # inventing a customer order reduction or rewriting the existing WO.
    return {name: qty if allocated[name] > max(0, qty-max(0, imported_shipped[name]-mes_shipped[name]))
            else max(0, qty-max(0, imported_shipped[name]-mes_shipped[name])) for name, qty in ordered.items()}


def shipment_conflicts(items, wo_records, source):
    ordered, imported, allocated = (defaultdict(int) for _ in range(3))
    mes_shipped = shipment_totals(wo_records)
    for item in items:
        if not is_nonproduction_item(item.get('item')):
            ordered[str(item['item']).strip().casefold()] += whole_quantity(item['quantity'])
    for item in (source or {}).get('items', []):
        imported[str(item['item']).strip().casefold()] += whole_quantity(item.get('shipped_quantity', 0))
    for wo in wo_records:
        for item in wo['items'] or []:
            allocated[str(item['item']).strip().casefold()] += whole_quantity(item['quantity'])
    return [{'item': name, 'source_shipped': imported[name], 'mes_shipped': mes_shipped.get(name, 0)}
            for name, qty in ordered.items() if imported[name] > mes_shipped.get(name, 0)
            and max(0, qty-max(0, imported[name]-mes_shipped.get(name, 0))) < allocated[name] <= qty]


def allocation_conflicts(items, wo_items):
    planned, allocated = defaultdict(int), defaultdict(int)
    for item in items:
        if not is_nonproduction_item(item.get('item')):
            planned[str(item['item']).strip().casefold()] += whole_quantity(item['quantity'])
    for item in wo_items:
        if not is_nonproduction_item(item.get('item')):
            allocated[str(item['item']).strip().casefold()] += whole_quantity(item['quantity'])
    return [{'item': name, 'ordered': planned.get(name, 0), 'allocated': qty}
            for name, qty in sorted(allocated.items()) if qty > planned.get(name, 0)]
