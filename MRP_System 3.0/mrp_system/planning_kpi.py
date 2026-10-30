"""Calendar-day SO creation -> first recorded production-assignment KPI.

Pure calculations: no schema changes, database writes, schedule inference, or
work-order mutations. Creation-date retention uses existing workflow metadata.
"""
from copy import deepcopy
from datetime import date, datetime, timezone

import pandas as pd

from mrp_system.transform.sales_order import normalize_wo_number

STAGES = {
    'needs_material_review': 'Needs Material Review',
    'needs_production_planning': 'Needs Production Planning',
    'released_to_mes': 'MES',
    'closed': 'Closed',
}


def created_date(value):
    if not isinstance(value, (str, date, datetime, pd.Timestamp)):
        return None
    parsed = pd.to_datetime(value, errors='coerce')
    if pd.isna(parsed):
        return None
    result = parsed.date()
    return result if date(1900, 1, 1) < result < date(2099, 12, 31) else None


def source_created_dates(frame):
    """Original source date, never import time or source ship/production dates."""
    column = next((name for name in ('SO Entry Date', 'Order Date') if name in frame), None)
    if column is None or 'QB Num' not in frame:
        return {}
    dates = {}
    for number, value in zip(frame['QB Num'], frame[column]):
        number = normalize_wo_number(str(number).strip())
        parsed = created_date(value)
        if parsed and (number not in dates or parsed < dates[number]):
            dates[number] = parsed
    return dates


def retain_creation_date(metadata, value, today=None):
    """Keep the first valid creation date even after an SO leaves the open import."""
    result = deepcopy(metadata or {})
    kpi = dict(result.get('planning_kpi') or {})
    existing = created_date(kpi.get('created_date'))
    today = today or datetime.now(timezone.utc).date()
    incoming = created_date(value)
    if (not existing or existing > today) and incoming and incoming <= today:
        kpi.update(created_date=incoming.isoformat(), created_date_source='CLI original order date')
        result['planning_kpi'] = kpi
    return result


def build_planning_kpi(records, assignments, source_dates, now=None):
    now = pd.Timestamp(now or datetime.now(timezone.utc))
    now = now.tz_localize('UTC') if now.tzinfo is None else now.tz_convert('UTC')
    today = now.date()
    orders = {}
    for record in records:
        number = normalize_wo_number(str(record['sales_order']).strip())
        old = orders.get(number)
        # Prefer the actual canonical workflow record over retired EO/WO aliases.
        if old is None or record['sales_order'] == number:
            orders[number] = record
    first = {}
    for assignment in assignments:
        number = normalize_wo_number(str(assignment['qb_num']).strip())
        assigned = pd.to_datetime(assignment['assigned_at'], errors='coerce', utc=True)
        if pd.notna(assigned) and (number not in first or assigned < first[number]):
            first[number] = assigned
    rows, durations = [], []
    awaiting_count = unknown_count = 0
    for number in sorted(set(orders) | set(first)):
        record = orders.get(number, {})
        metadata = record.get('metadata_fields') or {}
        created = created_date((metadata.get('planning_kpi') or {}).get('created_date'))
        created = created or created_date(source_dates.get(number))
        assigned = first.get(number)
        status = record.get('workflow_status', '')
        # Content approval alone is not a production assignment. Older pending
        # MRP orders can have approved snapshots without ever reaching MES.
        legacy = bool(record.get('mes_released_at') or record.get('returned_from_mes')
                      or status == 'released_to_mes')
        awaiting = (assigned is None and not legacy and record.get('source_present')
                    and status in ('needs_material_review', 'needs_production_planning'))
        awaiting_count += bool(awaiting)
        days, issue = None, ''
        if assigned is not None:
            if not created:
                issue = 'Original creation date unavailable'
            elif assigned > now or assigned.date() < created:
                issue = 'Invalid creation/assignment date sequence'
            else:
                days = (assigned.date() - created).days
                durations.append(days)
        elif awaiting:
            if not created:
                issue = 'Original creation date unavailable'
            elif created > today:
                issue = 'Creation date is in the future'
            else:
                days = (today - created).days
        else:
            issue = 'No recorded first assignment; not an active first-assignment queue entry'
        if issue:
            unknown_count += 1
        stage = STAGES.get(status, 'Historical / workflow unavailable')
        if record and not record.get('source_present'):
            stage += ' · Source missing'
        if legacy and assigned is None:
            stage += ' · First assignment unknown'
        rows.append(dict(so=number, customer=record.get('customer') or '—',
                         created_date=created.isoformat() if created else '—',
                         first_assigned_at=assigned.strftime('%Y-%m-%d %H:%M:%S UTC') if assigned is not None else '—',
                         days=days, awaiting=bool(awaiting), stage=stage, issue=issue,
                         blockers=record.get('blockers')))
    rows.sort(key=lambda row: (not row['awaiting'], -(row['days'] if row['days'] is not None else -1), row['so']))
    values = pd.Series(durations, dtype='float64')
    return dict(rows=rows, average=float(values.mean()) if len(values) else None,
                median=float(values.median()) if len(values) else None,
                p90=float(values.quantile(.9, interpolation='linear')) if len(values) else None,
                assigned_count=len(values), awaiting_count=awaiting_count,
                unknown_count=unknown_count, as_of=now.strftime('%Y-%m-%d %H:%M:%S UTC'))
