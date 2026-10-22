"""Explicit, value-preserving rename of the legacy source-presence column.

Preview: python -m mrp_system.cli.rename_source_present
Apply:   python -m mrp_system.cli.rename_source_present --apply
No tables are created and no business rows are updated.
"""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import inspect, text

from mrp_system.runtime.db_config import get_engine


OLD = 'google_present'
NEW = 'source_present'


def rename_column(conn, apply=False, backup_dir=None):
    columns = {c['name'] for c in inspect(conn).get_columns('approved_work_orders', schema='public')}
    if OLD in columns and NEW in columns:
        raise RuntimeError('Both source-presence columns exist; resolve the conflict before migration. No columns changed.')
    if NEW in columns:
        return 'Already migrated: source_present exists.'
    if OLD not in columns:
        raise RuntimeError('No source-presence column found. Check the database/schema. No columns changed.')
    if not apply:
        return 'Would rename public.approved_work_orders.google_present to source_present; all values retained.'
    if backup_dir is None:
        raise ValueError('A backup directory is required when applying the rename.')
    conn.execute(text("SET LOCAL lock_timeout = '10s'"))
    conn.execute(text('LOCK TABLE public.approved_work_orders IN ACCESS EXCLUSIVE MODE'))
    rows = [dict(row) for row in conn.execute(text('SELECT * FROM public.approved_work_orders ORDER BY sales_order')).mappings()]
    folder = Path(backup_dir)
    folder.mkdir(parents=True, exist_ok=True)
    name = 'before_source_present_rename_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ') + '.json'
    with (folder / name).open('x', encoding='utf-8') as handle:
        json.dump(rows, handle, default=str, ensure_ascii=False, indent=2)
    conn.execute(text('ALTER TABLE public.approved_work_orders RENAME COLUMN google_present TO source_present'))
    return f'Renamed google_present to source_present. Backup: {folder / name}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--backup-dir', type=Path,
                        default=Path(__file__).resolve().parents[3] / 'reports' / 'private_backups')
    args = parser.parse_args()
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
            print(rename_column(conn, args.apply, args.backup_dir))
    finally:
        engine.dispose()


if __name__ == '__main__':
    main()
