"""Pure, read-only material suggestions for manual LT reviews.

Uses the readiness SQL's ledger replay: remove own OUT, exclude undated inbound,
retain other demand, check end-of-day candidates against all subsequent balances.
Uncovered quantity is capped at this SO's demand; unrelated existing deficits
are not attributed to this order. Suggestions are never auto-approved.
"""
import hashlib
import json
import math
from datetime import date

import pandas as pd

CUTOFF = pd.Timestamp("2099-12-31")
REASONS = ("No open POD", "POD ship date unknown", "Confirmed supply insufficient", "Other / manual assessment")


def item_key(series):
    return series.fillna("").astype(str).str.strip().str.upper()


def material_context(so, ledger, pods, qb_num, today=None):
    today = pd.Timestamp(today or date.today()).normalize()
    from mrp_system.order_scope import is_nonproduction_item
    order = so.loc[so["QB Num"].fillna("").astype(str).str.strip().eq(qb_num) & ~so["Item"].map(is_nonproduction_item)].copy()
    order["Item"] = item_key(order["Item"])
    order["Qty(-)"] = pd.to_numeric(order["Qty(-)"], errors="coerce").fillna(0)
    demands = order.loc[order["Item"].ne("") & order["Qty(-)"].gt(0)].groupby("Item", sort=True)["Qty(-)"].sum()
    led = ledger.copy()
    po = pods.copy()
    for frame in (led, po):
        if "Item" not in frame:
            frame["Item"] = pd.Series(dtype=str)
        frame["Item"] = item_key(frame["Item"])
    led = led.loc[led["Item"].isin(demands.index)]
    po = po.loc[po["Item"].isin(demands.index)]
    # Stable content identity detects changed quantities, PODs and competing demand.
    parts = []
    own_out = led.get('Kind', pd.Series('', index=led.index)).eq('OUT') & led.get('QB Num', pd.Series('', index=led.index)).fillna('').astype(str).str.strip().eq(qb_num)
    relevant_ledger = led.loc[~own_out].reindex(columns=['Item', 'Date', 'Opening', 'Delta', 'Kind', 'QB Num'])
    own_validation = led.loc[own_out].reindex(columns=['Item', 'Opening', 'Delta', 'Kind', 'QB Num']).copy()
    own_validation['Date valid'] = pd.to_datetime(led.loc[own_out, 'Date'], errors='coerce').notna() if 'Date' in led else False
    # Source ship dates and descriptive inventory columns do not define material
    # approval. Own OUT is removed by the calculation too; its rescheduling must
    # not manufacture a fresh shortage observation.
    for frame in (order.reindex(columns=['QB Num', 'Item', 'Qty(-)']), relevant_ledger, own_validation, po):
        data = frame.reindex(sorted(frame.columns), axis=1).astype("string").fillna("")
        parts.append(sorted(json.dumps(row, sort_keys=True, ensure_ascii=False) for row in data.to_dict(orient="records")))
    fingerprint = hashlib.sha256(json.dumps([str(today.date()), parts], sort_keys=True).encode()).hexdigest()
    suggestions = []
    for item, required in demands.items():
        entries = led.loc[led["Item"].eq(item)].copy()
        pos = po.loc[po["Item"].eq(item)].copy()
        po_qty = pd.to_numeric(pos.get("Qty(+)", pd.Series(0, index=pos.index)), errors="coerce").fillna(0)
        pos = pos.loc[po_qty.gt(0)]
        dates = pd.to_datetime(pos.get("Ship Date", pd.Series(pd.NaT, index=pos.index)), errors="coerce")
        unknown = dates.isna() | dates.ge(CUTOFF)
        open_qty = float(po_qty.clip(lower=0).sum())
        unknown_qty = float(po_qty.reindex(pos.index).loc[unknown].sum())
        ready = None
        best_cover = 0.0
        error = ""
        required_cols = {"Opening", "Delta", "Date", "Kind", "QB Num"}
        if entries.empty or not required_cols.issubset(entries.columns):
            error = "Ledger data unavailable; verify quantity manually"
        else:
            openings = pd.to_numeric(entries["Opening"], errors="coerce").dropna().unique()
            delta = pd.to_numeric(entries["Delta"], errors="coerce")
            event_dates = pd.to_datetime(entries["Date"], errors="coerce").dt.normalize()
            if len(openings) != 1 or delta.isna().any() or (event_dates.isna() & delta.ne(0)).any():
                error = "Ledger data inconsistent; verify quantity manually"
            else:
                own = entries["Kind"].eq("OUT") & entries["QB Num"].fillna("").astype(str).str.strip().eq(qb_num)
                unknown_in = entries["Kind"].eq("IN") & (event_dates.isna() | event_dates.ge(CUTOFF))
                work = entries.loc[~own & ~unknown_in & event_dates.notna()].copy()
                work["Date"] = event_dates.loc[work.index]
                work["Delta"] = delta.loc[work.index]
                work["priority"] = work["Kind"].map({"OPEN": 0, "IN": 1, "ADJ": 2}).fillna(3)
                marker = pd.DataFrame([{"Date": today, "Delta": 0.0, "priority": 9}])
                work = pd.concat([work[["Date", "Delta", "priority"]], marker], ignore_index=True)
                work = work.sort_values(["Date", "priority"], kind="stable")
                work["balance"] = float(openings[0]) + work["Delta"].cumsum()
                work["future_min"] = work["balance"][::-1].cummin()[::-1]
                candidates = work.drop_duplicates("Date", keep="last")
                candidates = candidates.loc[candidates["Date"].ge(today) & candidates["Date"].lt(CUTOFF)]
                if not candidates.empty:
                    best_cover = max(0.0, float(candidates["future_min"].max()))
                    covered = candidates.loc[candidates["future_min"].ge(required)]
                    if not covered.empty:
                        ready = covered.iloc[0]["Date"].strftime("%Y-%m-%d")
        uncovered = max(0.0, float(required) - best_cover)
        if error:
            reason = "Other / manual assessment"
        elif open_qty <= 0:
            reason = "No open POD"
        elif unknown_qty > 0:
            reason = "POD ship date unknown"
        else:
            reason = "Confirmed supply insufficient"
        suggestions.append({"item": str(item), "required_qty": float(required),
                            "uncovered_qty": uncovered, "blocking": ready is None,
                            "reason": reason if ready is None else "", "ready_date": ready or "",
                            "open_po_qty": open_qty, "unknown_date_po_qty": unknown_qty,
                            "warning": error})
    return fingerprint, suggestions


def validate_blockers(rows, no_blockers, requirements):
    if not isinstance(rows, list) or len(rows) > 500:
        raise ValueError("Invalid blocker rows.")
    if no_blockers:
        if rows:
            raise ValueError("Remove blocker rows before approving no material blockers.")
        return []
    if not rows:
        raise ValueError("Enter blocking items or explicitly approve no material blockers.")
    result, seen = [], set()
    for row in rows:
        item = str(row.get("item", "")).strip().upper()
        if item not in requirements or item in seen:
            raise ValueError("Each blocker must be a unique component of this SO.")
        seen.add(item)
        try:
            required = float(row["required_qty"])
            uncovered = float(row["uncovered_qty"])
        except (KeyError, TypeError, ValueError):
            raise ValueError("Enter numeric required and uncovered quantities.") from None
        if not all(math.isfinite(v) for v in (required, uncovered)) or not 0 < uncovered <= required:
            raise ValueError("Quantities must be finite: 0 < uncovered quantity <= required quantity.")
        if abs(required - requirements[item]) > 0.000001:
            raise ValueError("Required quantity must match the SO component total. Reload if the SO changed.")
        reason = str(row.get("reason", "")).strip()
        if reason not in REASONS:
            raise ValueError("Select a blocking reason.")
        pod_num = row.get('pod_num') or ''
        if not isinstance(pod_num, str) or len(pod_num.strip()) > 200:
            raise ValueError('POD# must be text up to 200 characters.')
        blocker = {"item": item, "required_qty": required, "uncovered_qty": uncovered, "reason": reason}
        if pod_num.strip():
            blocker['pod_num'] = pod_num.strip()
        result.append(blocker)
    return result
