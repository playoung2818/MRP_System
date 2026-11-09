"""Compare current production assignments with the imported SO ship dates."""
import pandas as pd
from mrp_system.transform.sales_order import normalize_wo_number


def assigned_placeholder_ship_dates(sales: pd.DataFrame | None, assignments: pd.DataFrame) -> list[dict]:
    if sales is None or sales.empty or assignments.empty:
        return []
    if not {'QB Num', 'Ship Date'}.issubset(sales.columns):
        return []
    if not {'wo_number', 'production_date'}.issubset(assignments.columns):
        return []
    so = sales.copy()
    so['Ship Date'] = pd.to_datetime(so['Ship Date'], errors='coerce')
    so = so.loc[so['Ship Date'].dt.year.eq(2099) & so['QB Num'].notna()].copy()
    so['key'] = so['QB Num'].astype(str).map(normalize_wo_number)
    assigned = assignments.copy()
    assigned['production_date'] = pd.to_datetime(assigned['production_date'], errors='coerce')
    assigned = assigned.loc[assigned['production_date'].notna() & assigned['wo_number'].notna()].copy()
    assigned['key'] = assigned['wo_number'].astype(str).map(normalize_wo_number)
    assigned = assigned.loc[assigned['key'].ne('')].drop_duplicates('key')
    merged = so.merge(assigned[['key', 'production_date']], on='key', how='inner')
    rows = []
    for key, group in merged.groupby('key', sort=True):
        customer = group['Name'].dropna().astype(str).iloc[0] if 'Name' in group and group['Name'].notna().any() else ''
        rows.append({'so': key, 'customer': customer,
                     'ship_dates': ', '.join(sorted(set(group['Ship Date'].dt.strftime('%Y-%m-%d')))),
                     'production_date': group['production_date'].iloc[0].strftime('%Y-%m-%d')})
    return sorted(rows, key=lambda row: (row['production_date'], row['so']))
