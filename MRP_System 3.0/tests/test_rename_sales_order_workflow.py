"""Explicit table rename preserves data and never creates a replacement table."""
import json
from unittest.mock import MagicMock, patch

import pytest

from mrp_system.cli.rename_sales_order_workflow import rename_table


def inspector_for(tables):
    inspector = MagicMock()
    inspector.get_table_names.return_value = tables
    return inspector


def test_preview_performs_no_ddl_or_backup(tmp_path):
    conn = MagicMock()
    with patch('mrp_system.cli.rename_sales_order_workflow.inspect', return_value=inspector_for(['approved_work_orders'])):
        assert 'Would rename' in rename_table(conn, backup_dir=tmp_path)
    conn.execute.assert_not_called()
    assert list(tmp_path.iterdir()) == []


def test_apply_renames_and_backs_up_without_changing_rows(tmp_path):
    conn = MagicMock()
    rows = [{'sales_order': 'SO-1', 'source_present': True, 'workflow_status': 'released_to_mes'},
            {'sales_order': 'SO-2', 'source_present': False, 'workflow_status': 'closed'}]
    conn.execute.return_value.mappings.return_value.__iter__.return_value = iter(rows)
    with patch('mrp_system.cli.rename_sales_order_workflow.inspect', return_value=inspector_for(['approved_work_orders'])):
        assert '2 rows retained' in rename_table(conn, apply=True, backup_dir=tmp_path)
    statements = [str(call.args[0]) for call in conn.execute.call_args_list]
    assert statements[-1] == 'ALTER TABLE public.approved_work_orders RENAME TO sales_order_workflow'
    assert not any(sql.startswith(('UPDATE', 'DELETE', 'INSERT', 'CREATE', 'DROP')) for sql in statements)
    assert json.loads(next(tmp_path.glob('*.json')).read_text(encoding='utf-8')) == rows


def test_already_renamed_is_noop():
    conn = MagicMock()
    with patch('mrp_system.cli.rename_sales_order_workflow.inspect', return_value=inspector_for(['sales_order_workflow'])):
        assert 'Already migrated' in rename_table(conn, apply=True)
    conn.execute.assert_not_called()


@pytest.mark.parametrize('tables', [[], ['approved_work_orders', 'sales_order_workflow']])
def test_missing_or_ambiguous_tables_abort(tables):
    conn = MagicMock()
    with patch('mrp_system.cli.rename_sales_order_workflow.inspect', return_value=inspector_for(tables)):
        with pytest.raises(RuntimeError):
            rename_table(conn, apply=True)
    conn.execute.assert_not_called()


def test_apply_requires_backup_directory():
    conn = MagicMock()
    with patch('mrp_system.cli.rename_sales_order_workflow.inspect', return_value=inspector_for(['approved_work_orders'])):
        with pytest.raises(ValueError, match='backup'):
            rename_table(conn, apply=True)
    conn.execute.assert_not_called()


def test_backup_failure_prevents_rename(tmp_path):
    conn = MagicMock()
    with patch('mrp_system.cli.rename_sales_order_workflow.inspect', return_value=inspector_for(['approved_work_orders'])), \
         patch('mrp_system.cli.rename_sales_order_workflow.json.dump', side_effect=OSError('Disk full')):
        with pytest.raises(OSError):
            rename_table(conn, apply=True, backup_dir=tmp_path)
    assert not any('ALTER TABLE' in str(call.args[0]) for call in conn.execute.call_args_list)
