"""Orders fulfilled by drop shipment are outside MRP/MES production scope."""


def is_drop_ship_order(source):
    source = source or {}
    return (str(source.get('inventory_site') or '').strip().casefold() == 'drop ship'
            or any(str(item.get('inventory_site') or '').strip().casefold() == 'drop ship'
                   for item in source.get('items', []) or []))


def exclude_drop_ship_orders(frame):
    """Exclude the entire SO if any of its source lines is Drop Ship."""
    if 'Inventory Site' not in frame or 'QB Num' not in frame:
        return frame.copy()
    numbers = frame['QB Num'].fillna('').astype(str).str.strip()
    dropped = frame['Inventory Site'].fillna('').astype(str).str.strip().str.casefold().eq('drop ship')
    return frame.loc[~numbers.isin(numbers.loc[dropped])].copy()
