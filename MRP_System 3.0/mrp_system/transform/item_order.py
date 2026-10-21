"""Order SO lines using PDF references; otherwise retain source order."""
from collections import defaultdict, deque

import pandas as pd

from mrp_system.normalize.mrp_normalize import normalize_item
from .sales_order import normalize_wo_number


def item_key(value):
    if value is None or pd.isna(value):
        return ""
    name = str(normalize_item(value)).strip()
    if name.casefold() in {"", "nan", "none", "<na>", "null"}:
        return ""
    return name.casefold()


def so_key(value):
    if value is None or pd.isna(value):
        return ""
    value = str(value).strip()
    if value.casefold() in {"", "nan", "none", "<na>", "null"}:
        return ""
    return normalize_wo_number(value).upper()


def reorder_so_items(df, reference=None):
    """Change row order only; match canonical names and repeated occurrences."""
    if df.empty:
        return df.copy().reset_index(drop=True)
    refs = defaultdict(list)
    if reference is not None and not reference.empty:
        for order, item in reference[["QB Num", "Item"]].itertuples(index=False, name=None):
            key = item_key(item)
            if key:
                refs[so_key(order)].append(key)
    groups = defaultdict(list)
    for position, order in enumerate(df["QB Num"]):
        groups[so_key(order)].append(position)
    result = []
    for order in sorted(groups):
        positions = groups[order]
        keys = [item_key(df.iloc[pos]["Item"]) for pos in positions]
        queues = defaultdict(deque)
        for pos, key in zip(positions, keys):
            queues[key].append(pos)
        matched = []
        for key in refs.get(order, []):
            if queues[key]:
                matched.append(queues[key].popleft())
        used = set(matched)
        remaining = [pos for pos in positions if pos not in used]
        result.extend(matched + remaining)
    return df.iloc[result].copy().reset_index(drop=True)
