"""Prevent the retired calculated-readiness snapshot from returning via ETL."""
import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class ReadinessRetirementTests(unittest.TestCase):
    def test_etl_has_no_retired_readiness_import_or_call(self):
        tree = ast.parse((ROOT / 'mrp_system' / 'cli' / 'etl.py').read_text(encoding='utf-8'))
        imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)]
        calls = [node.func.id for node in ast.walk(tree)
                 if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)]
        self.assertNotIn('mrp_system.ledger.material_readiness', imports)
        self.assertNotIn('refresh_material_readiness', calls)
        self.assertIn('stage_cli_orders', calls)
        self.assertIn('build_ledger_from_events', calls)

    def test_standalone_refresher_is_removed(self):
        for suffix in ('.py', '.sql'):
            self.assertFalse((ROOT / 'mrp_system' / 'ledger' / ('material_readiness' + suffix)).exists())


if __name__ == '__main__':
    unittest.main()
