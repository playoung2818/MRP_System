"""Rename safety and canonical table references; no database/table creation."""
import ast
from pathlib import Path
import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.dialects import postgresql
from mrp_system.cli.rename_operational_tables import RENAMES, rename_tables

ROOT = Path(__file__).resolve().parents[1]


class OperationalTableNamesTests(unittest.TestCase):
    def connection(self):
        conn = MagicMock()
        conn.dialect = postgresql.dialect()
        conn.execute.return_value.scalar_one.return_value = 12
        return conn

    def test_preview_does_not_change_tables(self):
        conn = self.connection()
        answers = [value for i in range(5) for value in ({'oid': i + 1, 'relkind': 'r'}, None)]
        with patch('mrp_system.cli.rename_operational_tables.relation', side_effect=answers):
            plan = rename_tables(conn)
        self.assertEqual(len(plan), 5)
        conn.execute.assert_not_called()

    def test_collision_aborts_before_any_ddl(self):
        conn = self.connection()
        with patch('mrp_system.cli.rename_operational_tables.relation',
                   return_value={'oid': 1, 'relkind': 'r'}):
            with self.assertRaisesRegex(RuntimeError, 'Both'):
                rename_tables(conn, apply=True)
        conn.execute.assert_not_called()

    def test_apply_preserves_identity_and_counts_without_creating_tables(self):
        conn = self.connection()
        answers = [value for i in range(5) for value in ({'oid': i + 1, 'relkind': 'r'}, None)]
        answers += [value for i in range(5) for value in
                    ({'oid': i + 1, 'relkind': 'r'}, {'oid': i + 1, 'relkind': 'r'})]
        with patch('mrp_system.cli.rename_operational_tables.relation', side_effect=answers):
            plan = rename_tables(conn, apply=True)
        sql = [str(call.args[0]) for call in conn.execute.call_args_list]
        self.assertEqual(sum(statement.startswith('ALTER TABLE') for statement in sql), 5)
        self.assertFalse(any(statement.startswith(('CREATE', 'INSERT', 'DELETE', 'DROP')) for statement in sql))
        self.assertTrue(all(row['rows'] == 12 and row['status'] == 'renamed' for row in plan))

    def test_already_renamed_is_safe_to_repeat(self):
        conn = self.connection()
        answers = [value for i in range(5) for value in (None, {'oid': i + 1, 'relkind': 'r'})]
        answers += [value for i in range(5) for value in
                    ({'oid': i + 1, 'relkind': 'r'}, {'oid': i + 1, 'relkind': 'r'})]
        with patch('mrp_system.cli.rename_operational_tables.relation', side_effect=answers):
            plan = rename_tables(conn, apply=True)
        self.assertTrue(all(row['status'] == 'already renamed' for row in plan))
        self.assertFalse(any(str(call.args[0]).startswith('ALTER TABLE')
                             for call in conn.execute.call_args_list))

    def test_runtime_config_targets_canonical_tables(self):
        tree = ast.parse((ROOT / 'mrp_system' / 'runtime' / 'config.py').read_text(encoding='utf-8'))
        constants = {node.targets[0].id: node.value.value for node in tree.body
                     if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name)
                     and isinstance(node.value, ast.Constant)}
        self.assertEqual(constants['TBL_POD'], 'open_purchase_orders')
        self.assertEqual(constants['TBL_Shipping'], 'hq_shipping_schedule')

    def test_active_sources_do_not_use_legacy_table_identifiers(self):
        # The explicit rename command intentionally retains legacy names.
        for folder in (ROOT / 'mrp_system', ROOT.parent / 'Webpage'):
            for path in folder.rglob('*.py'):
                if path.name == 'rename_operational_tables.py':
                    continue
                tree = ast.parse(path.read_text(encoding='utf-8-sig'))
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                        continue
                    for old, new in RENAMES:
                        self.assertNotIn('"' + old + '"', node.value, (path, old))
                        self.assertNotEqual(node.value, old, (path, old))


if __name__ == '__main__':
    unittest.main()
