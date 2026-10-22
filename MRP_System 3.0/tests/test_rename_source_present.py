"""Rename migration is explicit, idempotent and preserves both boolean values."""
import json
from unittest.mock import MagicMock, patch

import pytest

from mrp_system.cli.rename_source_present import rename_column


def inspector_for(columns):
    inspector = MagicMock()
    inspector.get_columns.return_value = [{'name': name} for name in columns]
    return inspector


def test_preview_performs_no_ddl_or_backup(tmp_path):
    conn = MagicMock()
    with patch('mrp_system.cli.rename_source_present.inspect', return_value=inspector_for(['google_present'])):
        assert 'Would rename' in rename_column(conn, backup_dir=tmp_path)
    conn.execute.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_apply_renames_without_updating_business_rows(tmp_path):
    conn = MagicMock()
    rows = [{'sales_order': 'SO-1', 'google_present': True},
            {'sales_order': 'SO-2', 'google_present': False}]
    conn.execute.return_value.mappings.return_value.__iter__.return_value = iter(rows)
    with patch('mrp_system.cli.rename_source_present.inspect', return_value=inspector_for(['google_present'])):
        assert 'Renamed' in rename_column(conn, apply=True, backup_dir=tmp_path)
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert statements[-1] == 'ALTER TABLE public.sales_order_workflow RENAME COLUMN google_present TO source_present'
    assert not any(sql.startswith(('UPDATE', 'DELETE', 'INSERT', 'CREATE')) for sql in statements)
    assert json.loads(next(tmp_path.glob('*.json')).read_text(encoding='utf-8')) == rows


def test_already_renamed_is_noop():
    conn = MagicMock()
    with patch('mrp_system.cli.rename_source_present.inspect', return_value=inspector_for(['source_present'])):
        assert 'Already migrated' in rename_column(conn, apply=True)
    conn.execute.assert_not_called()


@pytest.mark.parametrize('columns', [[], ['google_present', 'source_present']])
def test_missing_or_ambiguous_columns_abort_without_ddl(columns):
    conn = MagicMock()
    with patch('mrp_system.cli.rename_source_present.inspect', return_value=inspector_for(columns)):
        with pytest.raises(RuntimeError):
            rename_column(conn, apply=True)
    conn.execute.assert_not_called()


def test_apply_requires_backup_directory():
    conn = MagicMock()
    with patch('mrp_system.cli.rename_source_present.inspect', return_value=inspector_for(['google_present'])):
        with pytest.raises(ValueError, match='backup'):
            rename_column(conn, apply=True)
    conn.execute.assert_not_called()
