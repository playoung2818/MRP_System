"""No live DB: CLI staging must not expose new SOs to MES."""
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import MagicMock

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from mrp_system.order_staging import cli_sources, staged_snapshot, stage_cli_orders, publish_completed_order


class StagingTests(unittest.TestCase):
    def source(self):
        return pd.DataFrame([{'QB Num': 'SO1', 'Item': 'A', 'Qty': 3,
                              'Customer': 'Customer', 'Lead Time': '2099-12-31', 'Inventory Site': 'WH01S-NTA'}])

    def test_cli_snapshot_preserves_placeholder_and_item_identity(self):
        first = staged_snapshot(cli_sources(self.source())['SO1'], None)
        second = staged_snapshot(cli_sources(self.source())['SO1'], first)
        self.assertEqual(first, second)
        self.assertEqual(first['ship_date'], '2099-12-31')
        changed = self.source()
        changed.loc[0, 'Qty'] = 4
        third = staged_snapshot(cli_sources(changed)['SO1'], first)
        self.assertEqual(third['items'][0]['id'], first['items'][0]['id'])

    def test_invalid_or_empty_source_does_not_start_transaction(self):
        engine = MagicMock()
        with self.assertRaises(ValueError):
            stage_cli_orders(engine, pd.DataFrame())
        engine.begin.assert_not_called()

    def test_new_cli_so_is_staged_inactive(self):
        engine = MagicMock()
        conn = engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.mappings.return_value.__iter__.return_value = iter([])
        self.assertEqual(stage_cli_orders(engine, self.source()), 1)
        statements = [str(c.args[0]) for c in conn.execute.call_args_list]
        self.assertIn('TRUE, FALSE', statements[-1])
        self.assertNotIn('production_overrides', ''.join(statements))

    def test_final_publication_activates_reviewed_content_and_preserves_allocations(self):
        conn = MagicMock()
        snapshot = staged_snapshot(cli_sources(self.source())['SO1'], None)
        result = MagicMock()
        result.mappings.return_value.first.return_value = {'google_present': True, 'latest_snapshot': snapshot, 'items': []}
        result.mappings.return_value.__iter__.return_value = iter([])
        conn.execute.return_value = result
        publish_completed_order(conn, 'SO1', notes='Shipment confirmed\nKeep with order')
        sql, params = conn.execute.call_args.args
        self.assertEqual(json.loads(params['metadata'])['notes'], 'Shipment confirmed\nKeep with order')
        self.assertIn('is_active=TRUE', str(sql))
        self.assertEqual(params['customer'], 'Customer')
        self.assertIn('"quantity": 3', params['items'])

    def test_publication_rejects_below_saved_wo_quantity(self):
        conn = MagicMock()
        snapshot = staged_snapshot(cli_sources(self.source())['SO1'], None)
        result = MagicMock()
        result.mappings.return_value.first.return_value = {'google_present': True, 'latest_snapshot': snapshot, 'items': []}
        result.mappings.return_value.__iter__.return_value = iter([{'items': [{'item': 'A', 'quantity': 4}]}])
        conn.execute.return_value = result
        with self.assertRaisesRegex(ValueError, 'saved WO allocations'):
            publish_completed_order(conn, 'SO1')
        self.assertFalse(any('UPDATE public.approved_work_orders' in str(c.args[0]) for c in conn.execute.call_args_list))


if __name__ == '__main__':
    unittest.main()
