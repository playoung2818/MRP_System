"""Pure item-review utilities for the combined MRP material/content review.

Import/staging and approval/publication belong to the current CLI and Planning
transaction. This module has no database session or independent approval path.
"""
from copy import deepcopy
import json
from uuid import uuid4


def is_drop_ship_order(source):
    source = source or {}
    items = source.get('items') or []
    if not items:
        return str(source.get('inventory_site') or '').strip().casefold() == 'drop ship'
    return all(str(item.get('inventory_site') or source.get('inventory_site') or '').strip().casefold() == 'drop ship'
               for item in items)


def review_items(row):
    """Keep reviewed/manual order while merging the latest imported content."""
    latest = deepcopy((row.latest_snapshot or {}).get('items', []))
    by_id = {item['id']: item for item in latest}
    ordered = []
    for item in row.items or []:
        item_id = item.get('id')
        if item_id in by_id:
            ordered.append(by_id.pop(item_id))
        elif str(item_id or '').startswith('manual-'):
            ordered.append(deepcopy(item))
    return ordered + [item for item in latest if item['id'] in by_id]


def _normalize_review_rows(item_rows):
    if isinstance(item_rows, str):
        item_rows = json.loads(item_rows or '[]')
    if not isinstance(item_rows, list):
        raise ValueError('Invalid review item rows.')
    rows, seen = [], set()
    for index, raw in enumerate(item_rows, start=1):
        if not isinstance(raw, dict):
            raise ValueError(f'Row {index}: invalid review item row.')
        item_id = str(raw.get('id') or '').strip()
        product = str(raw.get('item') or '').strip()
        configuration = str(raw.get('configuration') or '').strip()
        site = str(raw.get('inventory_site') or '').strip()
        if (not item_id or item_id.startswith('manual-')) and not product and not configuration and not site:
            continue
        if not product:
            raise ValueError(f'Row {index}: item is required. Delete blank added rows before pushing.')
        if not item_id:
            item_id = f'manual-{uuid4()}'
        if item_id in seen:
            if item_id.startswith('manual-'):
                item_id = f'manual-{uuid4()}'
            else:
                raise ValueError(f'Row {index}: duplicate imported row id. Reload the review page and try again.')
        try:
            quantity = int(str(raw.get('quantity') or '').strip())
        except ValueError:
            raise ValueError(f'Row {index} ({product}): quantity must be a nonnegative whole number.')
        if quantity < 0:
            raise ValueError(f'Row {index} ({product}): quantity must be a nonnegative whole number.')
        seen.add(item_id)
        rows.append({'id': item_id, 'item': product, 'quantity': quantity,
                     'configuration': configuration, 'inventory_site': site})
    return rows
