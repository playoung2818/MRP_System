"""Read-only stock coverage exceptions from the item-level inventory snapshot."""
import pandas as pd


def stock_coverage_exceptions(inventory: pd.DataFrame | None) -> list[dict]:
    required = {'Part_Number', 'Available', 'On PO', 'Sales/Week'}
    if inventory is None or inventory.empty or not required.issubset(inventory.columns):
        return []
    frame = inventory[list(required)].copy()
    frame['Part_Number'] = frame['Part_Number'].fillna('').astype(str).str.strip()
    for column in ('Available', 'On PO', 'Sales/Week'):
        frame[column] = pd.to_numeric(frame[column], errors='coerce')
    frame = frame.replace([float('inf'), -float('inf')], float('nan'))
    frame = frame.dropna(subset=['Available', 'On PO', 'Sales/Week'])
    frame = frame.loc[frame['Part_Number'].ne('') & frame['Sales/Week'].ge(0.2)]
    frame = frame.drop_duplicates(subset=['Part_Number'])
    rows = []
    for _, row in frame.iterrows():
        available = float(row['Available'] + row['On PO'])
        weekly = float(row['Sales/Week'])
        if available < weekly:
            status = 'Below 1× weekly sales'
        elif available > 4 * weekly:
            status = 'Above 4× weekly sales'
        else:
            continue
        rows.append({'item': row['Part_Number'], 'available': float(row['Available']),
                     'on_po': float(row['On PO']), 'total': available,
                     'weekly': weekly, 'lower': weekly, 'upper': 4 * weekly,
                     'status': status})
    return sorted(rows, key=lambda row: (row['status'].startswith('Above'), row['item']))
