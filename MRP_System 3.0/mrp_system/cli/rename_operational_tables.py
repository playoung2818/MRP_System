"""Preview/apply operational table renames without copying or creating tables."""
from __future__ import annotations

import argparse
from sqlalchemy import text
from mrp_system.runtime.db_config import get_engine

RENAMES = (
    ('Item Info', 'item_info'),
    ('NT Shipping Schedule', 'hq_shipping_schedule'),
    ('Open_Purchase_Orders', 'open_purchase_orders'),
    ('WO Details', 'wo_details'),
    ('combined_wo_outgoing_form_filing', 'outgoing_records'),
)


def relation(conn, name):
    return conn.execute(text('''SELECT c.oid, c.relkind FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='public' AND c.relname=:name'''), {'name': name}).mappings().first()


def rename_tables(conn, *, apply=False):
    quote = conn.dialect.identifier_preparer.quote
    plan = []
    # Check every pair before issuing any ALTER, including both-name collisions.
    for old, new in RENAMES:
        source, target = relation(conn, old), relation(conn, new)
        if source and target:
            raise RuntimeError(f'Both public.{old} and public.{new} exist; refusing to merge tables.')
        if not source and not target:
            raise RuntimeError(f'Neither public.{old} nor public.{new} exists.')
        current = source or target
        if current['relkind'] != 'r':
            raise RuntimeError(f'Expected an ordinary table for {old} / {new}.')
        plan.append({'old': old, 'new': new, 'oid': current['oid'],
                     'status': 'pending' if source else 'already renamed'})
    if not apply:
        return plan

    conn.execute(text("SET LOCAL lock_timeout = '5s'"))
    conn.execute(text("SET LOCAL statement_timeout = '30s'"))
    # Keep row-count verification meaningful in the presence of other writers.
    tables = ', '.join('public.' + quote(row['old'] if row['status'] == 'pending' else row['new'])
                       for row in plan)
    conn.execute(text('LOCK TABLE ' + tables + ' IN ACCESS EXCLUSIVE MODE'))
    for row in plan:
        old, new = row['old'], row['new']
        current = old if row['status'] == 'pending' else new
        # Recheck identity after obtaining locks, before any rename.
        if relation(conn, current)['oid'] != row['oid']:
            raise RuntimeError(f'Table identity changed for {current}; retry migration.')
        before = conn.execute(text('SELECT count(*) FROM public.' + quote(current))).scalar_one()
        if row['status'] == 'pending':
            conn.execute(text(f'ALTER TABLE public.{quote(old)} RENAME TO {quote(new)}'))
            row['status'] = 'renamed'
        after = conn.execute(text('SELECT count(*) FROM public.' + quote(new))).scalar_one()
        if relation(conn, new)['oid'] != row['oid'] or before != after:
            raise RuntimeError(f'Identity/row count verification failed for {new}.')
        row['rows'] = after
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Apply renames in one transaction; default is preview.')
    args = parser.parse_args()
    engine = get_engine()
    try:
        with engine.begin() as conn:
            plan = rename_tables(conn, apply=args.apply)
        for row in plan:
            count = f"; {row['rows']} rows" if 'rows' in row else ''
            print(f"{row['old']} -> {row['new']}: {row['status']}{count}")
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
