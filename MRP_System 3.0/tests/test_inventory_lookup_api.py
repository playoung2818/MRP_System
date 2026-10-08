"""Test the MRP lookup contract without importing the live server/database."""
import ast
import unittest
from datetime import datetime
from pathlib import Path
import pandas as pd
from flask import Flask, request, jsonify


class InventoryApiTests(unittest.TestCase):
    def setUp(self):
        app = Flask('inventory-api-test')
        self.calls = []
        self.receipt_calls = []
        def orders(item):
            self.calls.append(item)
            rows = [{'Item':'RAM','QB Num':'SO-20261234'}] if item == 'RAM' else []
            return ['Item','QB Num'], rows, {}
        def receipts(item):
            self.receipt_calls.append(item)
            return ['Date','Qty'], []
        self.ctx = dict(app=app, request=request, jsonify=jsonify,
            pd=pd, datetime=datetime,
            _LAST_LOAD_ERR=None, _LAST_LOADED_AT=datetime(2026,10,8,15),
            normalize_item=lambda value: 'RAM' if value == 'alias' else value,
            _ensure_loaded=lambda:None, _load_from_db=lambda force=False:None,
            _so_table_for_item=orders, _recent_receiving_summary_for_item=receipts,
            INVENTORY_STATUS=pd.DataFrame([{'Part_Number':'RAM','On Hand':20,'On Hand - WIP':13}]),
            SO_INV=pd.DataFrame([{'Item':'RAM','QB Num':'SO-20261234','On Hand':20,'On Hand - WIP':13}]),
            RECEIVING_LOG=pd.DataFrame())
        source = Path(__file__).resolve().parents[2] / 'Webpage' / 'server.py'
        names = {'_inventory_lookup_payload','api_inventory_lookup','_aggregate_metric','_coerce_total','_format_intish','_compute_on_hand_metrics'}
        functions = [node for node in ast.parse(source.read_text(encoding='utf-8-sig')).body if isinstance(node,ast.FunctionDef) and node.name in names]
        for node in functions:
            node.decorator_list = []
        exec(compile(ast.Module(body=functions,type_ignores=[]), '<inventory-api-test>', 'exec'), self.ctx)
        app.add_url_rule('/api/inventory-lookup', view_func=self.ctx['api_inventory_lookup'])
        self.client = app.test_client()

    def test_item_metrics_use_existing_mrp_values(self):
        response = self.client.get('/api/inventory-lookup?item=ram')
        self.assertEqual(response.status_code,200)
        self.assertEqual(response.json['on_hand'],20)
        self.assertEqual(response.json['on_hand_wip'],13)
        self.assertEqual(self.calls,['RAM'])

    def test_alias_uses_canonical_item_for_order_lookup(self):
        response = self.client.get('/api/inventory-lookup?item=alias')
        self.assertEqual(response.json['canonical_item'],'RAM')
        self.assertEqual(self.calls,['RAM'])
        self.assertEqual(self.receipt_calls,['alias'])

    def test_initial_page_data_lists_inventory(self):
        response = self.client.get('/api/inventory-lookup')
        self.assertEqual(response.json['inv_status_rows'][0]['Part_Number'],'RAM')
        self.assertEqual(response.json['so_rows'],[])

    def test_missing_wip_is_not_assumed_equal_to_on_hand(self):
        self.ctx['INVENTORY_STATUS'] = self.ctx['INVENTORY_STATUS'].drop(columns='On Hand - WIP')
        self.ctx['SO_INV'] = self.ctx['SO_INV'].drop(columns='On Hand - WIP')
        response = self.client.get('/api/inventory-lookup?item=RAM')
        self.assertIsNone(response.json['on_hand_wip'])

    def test_reload_refreshes_mrp_and_receiving_cache(self):
        refreshes=[]
        self.ctx['_load_from_db']=lambda force=False:refreshes.append(force)
        self.client.get('/api/inventory-lookup?item=RAM&reload=1')
        self.assertEqual(refreshes,[True])
        self.assertIsNone(self.ctx['RECEIVING_LOG'])

    def test_mrp_load_error_is_not_reported_as_zero(self):
        self.ctx['_LAST_LOAD_ERR']='Unavailable'
        response=self.client.get('/api/inventory-lookup?item=RAM')
        self.assertEqual(response.status_code,503)
        self.assertFalse(response.json['ok'])
        self.assertNotIn('on_hand',response.json)

    def test_old_inventory_page_is_removed(self):
        self.assertEqual(self.client.get('/inventory_count').status_code,404)
        self.assertNotIn('inventory_count', self.ctx)
        ui = Path(__file__).resolve().parents[2] / 'Webpage' / 'ui.py'
        source = ui.read_text(encoding='utf-8-sig')
        self.assertNotIn('INVENTORY_TPL', source)
        self.assertNotIn('href="/inventory_count"', source)


if __name__ == '__main__':
    unittest.main()
