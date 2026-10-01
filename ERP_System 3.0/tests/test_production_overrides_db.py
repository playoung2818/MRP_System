"""Production override readers must never create, migrate, or modify data."""
import unittest
from unittest.mock import MagicMock, patch
from types import SimpleNamespace
from erp_system import production_overrides as overrides


class ProductionReadOnlyTests(unittest.TestCase):
    def test_readers_use_only_selects_and_read_only_transactions(self):
        for loader, rows, expected in [
            (overrides.load_schedule, [SimpleNamespace(wo_number="SO-1", production_date="2026-10-01")], {"SO-1": "2026-10-01"}),
            (overrides.load_finished_goods, [SimpleNamespace(wo_number="SO-1")], {"SO-1"}),
        ]:
            engine = MagicMock()
            engine.dialect.name = "postgresql"
            conn = engine.connect.return_value.__enter__.return_value
            conn.execute.side_effect = [None, rows]
            schema = MagicMock()
            schema.has_table.return_value = True
            with patch.object(overrides, "inspect", return_value=schema):
                self.assertEqual(loader(engine), expected)
            statements = [str(call.args[0]).strip().upper() for call in conn.execute.call_args_list]
            self.assertEqual(statements[0], "SET TRANSACTION READ ONLY")
            self.assertTrue(all(s.startswith("SELECT ") for s in statements[1:]))
            engine.begin.assert_not_called()

    def test_missing_tables_are_not_created(self):
        for loader, expected in [(overrides.load_schedule, {}), (overrides.load_finished_goods, set())]:
            engine = MagicMock()
            engine.dialect.name = "postgresql"
            conn = engine.connect.return_value.__enter__.return_value
            schema = MagicMock()
            schema.has_table.return_value = False
            with patch.object(overrides, "inspect", return_value=schema):
                self.assertEqual(loader(engine), expected)
            self.assertEqual(conn.execute.call_count, 1)

    def test_write_functions_removed(self):
        for name in ["ensure_table", "save_schedule", "save_finished_goods"]:
            self.assertFalse(hasattr(overrides, name))
