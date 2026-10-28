"""Drop Ship lines, not their warehouse siblings, are outside production scope."""


def is_nonproduction_item(item):
    return str(item or '').strip().casefold() in {
        'shipping charge', 'forwarding charge', 'tariff (estimation)', 'tariff charge', 'engineer services', 'education discount',
    }


def is_drop_ship_order(source):
    source = source or {}
    items = source.get('items') or []
    if not items:
        return str(source.get('inventory_site') or '').strip().casefold() == 'drop ship'
    return all(str(item.get('inventory_site') or source.get('inventory_site') or '').strip().casefold() == 'drop ship'
               for item in items)


def exclude_drop_ship_orders(frame):
    """Exclude only Drop Ship lines; retain warehouse lines on mixed-site SOs."""
    if 'Inventory Site' not in frame:
        return frame.copy()
    dropped = frame['Inventory Site'].fillna('').astype(str).str.strip().str.casefold().eq('drop ship')
    return frame.loc[~dropped].copy()
