"""Preview/backfill the shared SO workflow. ALTER existing tables only.

python -m mrp_system.cli.setup_so_workflow [--apply]
No application GET or ordinary CLI run performs schema DDL.
"""
import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4
import pandas as pd
from sqlalchemy import text
from mrp_system.runtime.db_config import get_engine
from mrp_system.order_staging import cli_sources, staged_snapshot
from mrp_system.order_scope import is_drop_ship_order, is_nonproduction_item
from mrp_system.so_workflow import REVIEW, PLANNING, MES, CLOSED, order_signature

DDL = '''
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS workflow_status TEXT;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS returned_from_mes BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS approved_order_signature JSONB;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS mes_released_at TIMESTAMPTZ;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS review_incident_id UUID;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS material_review_id UUID;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE public.approved_work_orders ADD COLUMN IF NOT EXISTS reconciliation_details JSONB NOT NULL DEFAULT '{}';
ALTER TABLE public.mrp_review_history ADD COLUMN IF NOT EXISTS incident_id UUID;
ALTER TABLE public.mrp_review_history ADD COLUMN IF NOT EXISTS source_revision INTEGER;
ALTER TABLE public.mrp_review_history ADD COLUMN IF NOT EXISTS review_kind TEXT NOT NULL DEFAULT 'initial';
ALTER TABLE public.mrp_review_history ADD COLUMN IF NOT EXISTS mes_sent_at TIMESTAMPTZ;
'''


def preview(conn):
    rows = conn.execute(text('SELECT * FROM public.approved_work_orders ORDER BY sales_order')).mappings().all()
    reviews = {r['qb_num']: r for r in conn.execute(text('SELECT DISTINCT ON (qb_num) * FROM public.mrp_review_history ORDER BY qb_num, assessed_at DESC, id DESC')).mappings()}
    plans = {r['qb_num']: r for r in conn.execute(text('SELECT DISTINCT ON (qb_num) * FROM public.lt_planning_history ORDER BY qb_num, assigned_at DESC, id DESC')).mappings()}
    raw = pd.read_sql(text('SELECT * FROM public.open_sales_orders'), conn)
    if 'Ordered Qty' not in raw:
        raw['Ordered Qty'] = pd.to_numeric(raw['Qty(-)'], errors='raise') + pd.to_numeric(raw['Shipped Qty'], errors='raise')
    raw = raw.rename(columns={'Qty(-)': 'Qty', 'Name': 'Customer', 'P. O. #': 'Customer PO', 'Ship Date': 'Lead Time'})
    sources = cli_sources(raw)
    result = []
    for row in rows:
        so = row['sales_order']
        if row.get('workflow_status'):
            continue  # Rerunning setup must never reset operational state.
        prior = row['latest_snapshot'] or row['previous_snapshot'] or row['approved_snapshot'] or dict(row['metadata_fields'] or {}, items=row['items'] or [])
        incoming = staged_snapshot(sources[so], prior) if so in sources else None
        approved = row['approved_snapshot'] or {'items': row['items'] or []}
        baseline = order_signature(approved) if approved.get('items') else None
        # Older CLI approvals stored outstanding rather than ordered quantities.
        # Normalize only when that entire baseline exactly matches the currently
        # outstanding source; keep published WO requirements untouched.
        normalized_legacy = False
        if incoming and baseline is not None:
            outstanding = {'items': [dict(i, quantity=i['remaining_quantity'], ordered_quantity=i['remaining_quantity'])
                                     for i in incoming['items']]}
            if baseline == order_signature(outstanding) and baseline != order_signature(incoming):
                baseline = order_signature(incoming)
                normalized_legacy = True
        released = bool((row['is_active'] and row['approved_at']) or (so in plans and plans[so]['mes_sent_at']))
        status = MES if released else PLANNING if so in reviews else REVIEW
        returned = False
        reason = 'Existing MES handoff' if released else 'Material review saved' if so in reviews else 'Initial review required'
        if is_drop_ship_order(incoming or prior) or not any(not is_nonproduction_item(i.get('item')) for i in (incoming or prior).get('items', [])):
            status, reason = CLOSED, 'Excluded Drop Ship/non-production SO'
        elif incoming and baseline is not None and baseline != order_signature(incoming):
            status, returned, reason = REVIEW, released, 'Ordered item/quantity amendment (fulfillment quantities excluded)'
        elif not incoming:
            if not released and so not in reviews and so not in plans:
                status, reason = CLOSED, 'Legacy inactive and absent; retained database record, not an active SO'
            else:
                reason += '; source absent: disposition required, NOT automatically cancelled'
        if normalized_legacy:
            reason += '; legacy outstanding baseline normalized to original ordered quantity'
        released_at = (plans.get(so) or {}).get('mes_sent_at') or row['approved_at'] if released else None
        if released_at and released_at.tzinfo is None:
            released_at = released_at.replace(tzinfo=timezone.utc)
        result.append({'so': so, 'status': status, 'returned': returned, 'reason': reason,
                       'baseline': baseline, 'incoming': incoming, 'changed': incoming != row['latest_snapshot'],
                       'released_at': released_at, 'review_id': str(reviews[so]['id']) if so in reviews else None,
                       'incident': str(uuid4())})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--report', default='so_workflow_migration_preview.csv')
    parser.add_argument('--backup-dir', default='.')
    args = parser.parse_args()
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
            records = preview(conn)
            with Path(args.report).open('w', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=['so', 'status', 'returned', 'reason'])
                writer.writeheader()
                writer.writerows({k: row[k] for k in writer.fieldnames} for row in records)
            print('Preview:', dict(Counter(r['status'] for r in records)), 'Returned:', sum(r['returned'] for r in records))
            print('Report:', args.report)
            if not args.apply:
                print('Read-only preview. No schema or business records changed.')
                return
            if records:
                folder = Path(args.backup_dir)
                folder.mkdir(parents=True, exist_ok=True)
                backup = folder / ('so_workflow_before_migration_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json')
                saved = [dict(r) for r in conn.execute(text('SELECT * FROM public.approved_work_orders ORDER BY sales_order')).mappings()]
                backup.write_text(json.dumps(saved, default=str, ensure_ascii=False, indent=2), encoding='utf-8')
                print('Pre-migration backup:', backup)
            conn.exec_driver_sql(DDL)
            for row in records:
                conn.execute(text('''UPDATE public.approved_work_orders SET workflow_status=:status,
                    returned_from_mes=:returned, approved_order_signature=CAST(:baseline AS jsonb),
                    mes_released_at=:released, review_incident_id=CAST(:incident AS uuid),
                    material_review_id=CAST(:review AS uuid), is_active=(:status='released_to_mes'),
                    previous_snapshot=COALESCE(latest_snapshot, previous_snapshot),
                    latest_snapshot=CAST(:incoming AS json), google_present=:present,
                    source_revision=source_revision+:changed,
                    metadata_fields=CASE WHEN :status='released_to_mes' AND :present
                        THEN CAST(CAST(metadata_fields AS jsonb) || jsonb_build_object('ship_date', CAST(:ship AS text)) AS json)
                        ELSE metadata_fields END
                    WHERE sales_order=:so AND workflow_status IS NULL'''),
                    {'so': row['so'], 'status': row['status'], 'returned': row['returned'],
                     'baseline': json.dumps(row['baseline']) if row['baseline'] is not None else None,
                     'released': row['released_at'], 'incident': row['incident'], 'review': row['review_id'],
                     'incoming': json.dumps(row['incoming']) if row['incoming'] else None,
                     'present': row['incoming'] is not None, 'changed': int(row['changed']),
                     'ship': (row['incoming'] or {}).get('ship_date', '')})
            conn.exec_driver_sql("ALTER TABLE public.approved_work_orders ALTER COLUMN workflow_status SET DEFAULT 'needs_material_review'; ALTER TABLE public.approved_work_orders ALTER COLUMN workflow_status SET NOT NULL;")
            # A constraint is added once, without creating any new table.
            exists = conn.execute(text("SELECT 1 FROM pg_constraint WHERE conname='approved_so_workflow_status' AND conrelid='public.approved_work_orders'::regclass")).scalar()
            if not exists:
                conn.exec_driver_sql("ALTER TABLE public.approved_work_orders ADD CONSTRAINT approved_so_workflow_status CHECK (workflow_status IN ('needs_material_review','needs_production_planning','released_to_mes','closed'))")
            print('Applied to existing tables. Historical rows and saved WOs were not rewritten.')
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
