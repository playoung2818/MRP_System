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

    def test_nonproduction_lines_are_excluded_from_cli_staging(self):
        rows = self.source()
        charge = dict(rows.iloc[0], Item=' Shipping Charge ', **{'Inventory Site': 'WH01S-NTA'})
        charge_only = dict(charge, **{'QB Num': 'SO-CHARGE'})
        service = dict(charge, Item=' Engineer Services ')
        service_only = dict(service, **{'QB Num': 'SO-SERVICE'})
        tariff = dict(charge, Item=' TaRiFf Charge ')
        tariff_only = dict(tariff, **{'QB Num': 'SO-TARIFF'})
        orders = cli_sources(pd.concat([rows, pd.DataFrame([charge, charge_only, service, service_only, tariff, tariff_only])], ignore_index=True))
        self.assertEqual(set(orders), {'SO1'})
        self.assertEqual([i['item'] for i in orders['SO1']['items']], ['A'])

    def test_publication_filters_legacy_and_manual_nonproduction_lines(self):
        conn = MagicMock()
        incoming = staged_snapshot(cli_sources(self.source())['SO1'], None)
        incoming['items'].extend([{'id': 'charge', 'item': 'Shipping Charge', 'quantity': 1},
                                  {'id': 'service', 'item': 'Engineer Services', 'quantity': 1},
                                  {'id': 'tariff', 'item': ' Tariff Charge ', 'quantity': 1}])
        conn.execute.return_value.mappings.return_value.first.return_value = {
            'source_present': True, 'latest_snapshot': incoming,
            'items': [{'id': 'manual-charge', 'item': ' shipping charge ', 'quantity': 1},
                      {'id': 'manual-service', 'item': ' ENGINEER SERVICES ', 'quantity': 1},
                      {'id': 'manual-tariff', 'item': 'TARIFF CHARGE', 'quantity': 1}]}
        conn.execute.return_value.mappings.return_value.__iter__.return_value = iter([])
        publish_completed_order(conn, 'SO1')
        params = conn.execute.call_args.args[1]
        self.assertEqual([i['item'] for i in json.loads(params['items'])], ['A'])
        self.assertEqual([i['item'] for i in json.loads(params['snapshot'])['items']], ['A'])

    def test_mixed_site_drop_ship_so_stages_only_warehouse_lines(self):
        rows = self.source()
        drop = dict(rows.iloc[0], Item='System', **{'Inventory Site': ' Drop ship '})
        warehouse = dict(rows.iloc[0], **{'QB Num': 'SO2'})
        rows = pd.concat([rows, pd.DataFrame([drop, warehouse])], ignore_index=True)
        sources = cli_sources(rows)
        self.assertEqual(set(sources), {'SO1', 'SO2'})
        self.assertNotIn('System', [i['item'] for i in sources['SO1']['items']])

    def test_previously_staged_drop_ship_so_is_deactivated(self):
        engine = MagicMock()
        conn = engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.mappings.return_value.__iter__.return_value = iter([{'sales_order': 'SO1'}])
        rows = self.source()
        rows['Inventory Site'] = 'Drop Ship'
        self.assertEqual(stage_cli_orders(engine, rows), 0)
        sql, params = conn.execute.call_args.args
        self.assertIn("workflow_status=CASE WHEN :excluded THEN 'closed'", str(sql))
        self.assertTrue(params['excluded'])
        self.assertEqual(params['so'], 'SO1')

    def test_publication_excludes_drop_ship_line_from_mixed_so(self):
        conn = MagicMock()
        incoming = {'inventory_site': 'WH01S-NTA', 'items': [
            {'id': 'a', 'item': 'CPU', 'quantity': 1, 'inventory_site': 'WH01S-NTA'},
            {'id': 'b', 'item': 'System', 'quantity': 1, 'inventory_site': 'DROP SHIP'}]}
        conn.execute.return_value.mappings.return_value.first.return_value = {
            'source_present': True, 'latest_snapshot': incoming, 'items': []}
        conn.execute.return_value.mappings.return_value.__iter__.return_value = iter([])
        publish_completed_order(conn, 'SO1')
        params = conn.execute.call_args.args[1]
        self.assertEqual([i['item'] for i in json.loads(params['items'])], ['CPU'])
        self.assertEqual([i['item'] for i in json.loads(params['snapshot'])['items']], ['CPU'])

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
        self.assertEqual(conn.execute.call_args.args[1]['status'], 'needs_material_review')
        self.assertNotIn('is_active', ''.join(statements))
        self.assertNotIn('production_overrides', ''.join(statements))

    def test_final_publication_activates_reviewed_content_and_preserves_allocations(self):
        conn = MagicMock()
        snapshot = staged_snapshot(cli_sources(self.source())['SO1'], None)
        result = MagicMock()
        result.mappings.return_value.first.return_value = {'source_present': True, 'latest_snapshot': snapshot, 'items': []}
        result.mappings.return_value.__iter__.return_value = iter([])
        conn.execute.return_value = result
        publish_completed_order(conn, 'SO1', notes='Shipment confirmed\nKeep with order')
        sql, params = conn.execute.call_args.args
        self.assertEqual(json.loads(params['metadata'])['notes'], 'Shipment confirmed\nKeep with order')
        self.assertIn("workflow_status='released_to_mes'", str(sql))
        self.assertNotIn('is_active', str(sql))
        self.assertEqual(params['customer'], 'Customer')
        self.assertIn('"quantity": 3', params['items'])

    def test_returned_publication_flags_conflicts_without_rewriting_wos(self):
        conn = MagicMock()
        snapshot = staged_snapshot(cli_sources(self.source())['SO1'], None)
        result = MagicMock()
        result.mappings.return_value.first.return_value = {'source_present': True, 'latest_snapshot': snapshot,
            'items': [], 'approved_order_signature': {'a': 5}}
        result.mappings.return_value.__iter__.return_value = iter([{'items': [{'item': 'A', 'quantity': 4}], 'is_shipped': False}])
        result.scalar_one_or_none.return_value = True
        conn.execute.return_value = result
        publish_completed_order(conn, 'SO1', allow_reconciliation=True)
        args = conn.execute.call_args.args[1]
        self.assertTrue(args['reconcile'])
        self.assertEqual(json.loads(args['details'])['allocations'], [{'item': 'a', 'ordered': 3, 'allocated': 4}])
        self.assertFalse(any(str(c.args[0]).lstrip().startswith(('UPDATE public."WO Details"', 'INSERT INTO public.production_overrides')) for c in conn.execute.call_args_list))

    def test_missing_identity_does_not_begin_transaction(self):
        engine = MagicMock()
        with self.assertRaises(ValueError):
            stage_cli_orders(engine, self.source().assign(**{'QB Num': None}))
        engine.begin.assert_not_called()

    def test_publication_rejects_below_saved_wo_quantity(self):
        conn = MagicMock()
        snapshot = staged_snapshot(cli_sources(self.source())['SO1'], None)
        result = MagicMock()
        result.mappings.return_value.first.return_value = {'source_present': True, 'latest_snapshot': snapshot, 'items': []}
        result.mappings.return_value.__iter__.return_value = iter([{'items': [{'item': 'A', 'quantity': 4}]}])
        conn.execute.return_value = result
        with self.assertRaisesRegex(ValueError, 'saved WO allocations'):
            publish_completed_order(conn, 'SO1')
        self.assertFalse(any('UPDATE public.approved_work_orders' in str(c.args[0]) for c in conn.execute.call_args_list))


if __name__ == '__main__':
    unittest.main()
