from __future__ import annotations

import re
from collections import deque

import pandas as pd


def level1_normalize_wo(raw: str) -> str:
    """
    Level 1
    Goal: normalize any WO/SO text to canonical 'SO-20xxxxxx' when possible.
    """
    m = re.search(r'(20\d{6})', raw)
    return f"SO-{m.group(0)}" if m else raw
    raise NotImplementedError("Implement level1_normalize_wo")


def level2_build_pick_flags(word_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Level 2
    Input columns: order_id, status
    Output columns: QB Num, Picked_Flag
    Rules:
      - normalize order_id with level1_normalize_wo
      - Picked_Flag is True when status == 'Picked' (case-insensitive, trim spaces)
      - group by QB Num and keep max Picked_Flag
    """
    out = pd.DataFrame()
    out['QB Num'] = word_rows['order_id'].apply(level1_normalize_wo)
    out['Picked_Flag'] = word_rows['status'].str.lower().eq('picked')
    out = out.groupby('QB Num', as_index=False)['Picked_Flag'].max()
    return out

    raise NotImplementedError("Implement level2_build_pick_flags")


def level3_select_shipping_qty(df_shipping: pd.DataFrame) -> pd.Series:
    """
    Level 3
    Return one numeric Series used as Qty(+):
      - Prefer 'Confirmed Qty'
      - If confirmed is missing/NaN, use 0 (never fallback to 'Qty')
      - Non-numeric values become 0
    """
    return pd.to_numeric(df_shipping['Confirmed Qty'], errors="coerce").fillna(0)
    
    raise NotImplementedError("Implement level3_select_shipping_qty")


def level4_reconcile_open_po_vs_ledger(
    open_po: pd.DataFrame,
    ledger_in: pd.DataFrame,
    *,
    excluded_vendors: tuple[str, ...] = ("CoastIPC, Inc.", "Industrial PC, Inc."),
    eps: float = 1e-9,
) -> pd.DataFrame:
    """
    Level 4
    Build mismatch table with columns:
      item_key, open_po_qty, ledger_in_qty, gap_qty

    Business rules:
      - open_po uses columns Item, Qty(+), Name
      - ledger_in uses columns Item, Delta
      - normalize item key using strip + upper
      - quantities are numeric and must be > 0 before grouping
      - exclude vendors in excluded_vendors from open_po
      - gap_qty = ledger_in_qty - open_po_qty
      - return only rows where abs(gap_qty) > eps, sorted by gap_qty desc
    """
    df = pd.DataFrame()
    open_po = open_po.loc[~open_po["Name"].isin(excluded_vendors)].copy()

    open_po['item_key']=open_po["Item"].astype(str).str.upper().str.strip()
    ledger_in['item_key']=ledger_in["Item"].astype(str).str.upper().str.strip()

    open_po["Qty(+)"] = pd.to_numeric(open_po["Qty(+)"], errors="coerce").fillna(0)
    ledger_in["Delta"] = pd.to_numeric(ledger_in["Delta"], errors="coerce").fillna(0)

    open_po = open_po.loc[open_po["Qty(+)"] > 0].copy()
    ledger_in = ledger_in.loc[ledger_in["Delta"] > 0].copy()

    po = open_po.groupby('item_key', as_index=False)["Qty(+)"].sum().rename(columns={"Qty(+)": "open_po_qty"})
    led = ledger_in.groupby("item_key", as_index=False)["Delta"].sum().rename(columns={"Delta": "ledger_in_qty"})

    df = po.merge(led, on='item_key', how='outer')
    df["open_po_qty"] = pd.to_numeric(df["open_po_qty"], errors="coerce").fillna(0.0)
    df["ledger_in_qty"] = pd.to_numeric(df["ledger_in_qty"], errors="coerce").fillna(0.0)
    df['gap_qty'] = df['ledger_in_qty'] - df['open_po_qty']
    
    result = df[df['gap_qty'].abs() > eps].sort_values('gap_qty', ascending=False)
    return result
    raise NotImplementedError("Implement level4_reconcile_open_po_vs_ledger")


def level5_topological_order(graph: dict[str, list[str]]) -> list[str]:
    """
    Level 5
    Return a topological order for a dependency graph.

    Graph convention:
      - key is a node
      - value is a list of nodes that must run BEFORE the key
    Example:
      {'structured': ['sales_order', 'inventory']}
    means structured depends on sales_order and inventory.

    Requirements:
      - include every node in output exactly once
      - if graph has a cycle, raise ValueError
    """
    raise NotImplementedError("Implement level5_topological_order")


# Optional helper references you can reuse while solving.
WO_PATTERN = re.compile(r"(20\d{6})")


def _to_item_key(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.upper()


def _kahn_topological_order(graph: dict[str, list[str]]) -> list[str]:
    """
    Helper for Level 5.
    Not used by default; you can use or ignore this.
    """
    nodes: set[str] = set(graph.keys())
    for deps in graph.values():
        nodes.update(deps)

    in_degree = {n: 0 for n in nodes}
    downstream: dict[str, list[str]] = {n: [] for n in nodes}

    for node, deps in graph.items():
        in_degree[node] += len(deps)
        for dep in deps:
            downstream[dep].append(node)

    q = deque(sorted(n for n, d in in_degree.items() if d == 0))
    order: list[str] = []

    while q:
        n = q.popleft()
        order.append(n)
        for nxt in sorted(downstream[n]):
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                q.append(nxt)

    if len(order) != len(nodes):
        raise ValueError("Cycle detected in graph")
    return order
