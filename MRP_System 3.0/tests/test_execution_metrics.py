"""Physical MES execution must not be inferred from source backorders/invoices."""
import ast
from pathlib import Path
import unittest
from unittest.mock import MagicMock

import pandas as pd
from sqlalchemy import text, bindparam
from mrp_system.normalize.mrp_normalize import normalize_item

from mrp_system.ingest.mes import quantities_from_saved_wos
from mrp_system.transform.inventory import (
    EXECUTION_COLUMNS, build_wip_lookup, execution_metrics, transform_inventory,
)
from mrp_system.transform.sales_order import normalize_wo_number
from mrp_system.transform.structured import build_structured_df


class ExecutionMetricsTests(unittest.TestCase):
    def sales(self, ordered=17, outstanding=9, source_reduction=8):
        return pd.DataFrame([{'QB Num': 'SO-20261234', 'Item': 'ABC',
            'Inventory Site': 'WH01S-NTA', 'Name': 'Customer',
            'Ordered Qty': ordered, 'Qty(-)': outstanding,
            'Shipped Qty': source_reduction, 'partial': bool(source_reduction)}])

    def mes(self, wip=8, shipped=0):
        return pd.DataFrame([dict(sales_order='SO-20261234', item='ABC',
            inventory_site='WH01S-NTA', quantity=wip, shipped_quantity=shipped)])

    def test_seventeen_ordered_eight_wip_nine_awaiting_zero_shipped(self):
        row = execution_metrics(self.sales(), self.mes()).iloc[0]
        self.assertEqual(row['SO Ordered Qty'], 17)
        self.assertEqual(row['SO WIP Qty'], 8)
        self.assertEqual(row['Awaiting Pick Qty'], 9)
        self.assertEqual(row['Physically Shipped Qty'], 0)
        self.assertEqual(row['Source Outstanding Qty'], 9)
        self.assertEqual(row['Picked'], 'Partial')
        self.assertFalse(row['Execution Issue'])

    def test_source_outstanding_does_not_cap_wip(self):
        row = execution_metrics(self.sales(outstanding=5, source_reduction=12), self.mes(wip=12)).iloc[0]
        self.assertEqual(row['SO WIP Qty'], 12)
        self.assertEqual(row['Awaiting Pick Qty'], 5)
        self.assertEqual(build_wip_lookup(self.sales(outstanding=5), self.mes(wip=12)).iloc[0]['WIP_Qty'], 12)

    def test_only_explicit_mes_shipments_reduce_physical_balance(self):
        row = execution_metrics(self.sales(), self.mes(wip=5, shipped=3)).iloc[0]
        self.assertEqual(row['SO WIP Qty'], 5)
        self.assertEqual(row['Physically Shipped Qty'], 3)
        self.assertEqual(row['Awaiting Pick Qty'], 9)

    def test_source_reduction_does_not_claim_a_shipment(self):
        row = execution_metrics(self.sales(), self.mes(wip=0)).iloc[0]
        self.assertEqual(row['Physically Shipped Qty'], 0)
        self.assertEqual(row['Awaiting Pick Qty'], 17)
        self.assertEqual(row['Picked'], 'No')

    def test_excess_saved_quantities_flag_review_without_hiding_stock(self):
        row = execution_metrics(self.sales(), self.mes(wip=18)).iloc[0]
        self.assertTrue(row['Execution Issue'])
        self.assertEqual(row['SO WIP Qty'], 18)
        self.assertEqual(row['Awaiting Pick Qty'], 0)

    def test_duplicate_source_lines_are_aggregated_once(self):
        sales = pd.concat([self.sales(ordered=10, outstanding=6, source_reduction=4),
                           self.sales(ordered=7, outstanding=3, source_reduction=4)])
        rows = execution_metrics(sales, self.mes())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows.iloc[0]['SO Ordered Qty'], 17)
        self.assertEqual(rows.iloc[0]['SO WIP Qty'], 8)
        self.assertEqual(rows.iloc[0]['Awaiting Pick Qty'], 9)

    def test_raw_apostrophe_exclusion_is_preserved(self):
        records = [dict(sales_order="SO-20261234'", items=[dict(item='ABC', quantity=1)],
                        is_shipped=False, record_source='mes')]
        quantities = quantities_from_saved_wos(records, ['SO-20261234'])
        self.assertTrue(quantities.empty)
        row = execution_metrics(self.sales(), quantities).iloc[0]
        self.assertEqual(row['SO WIP Qty'], 0)
        self.assertEqual(records[0]['sales_order'], "SO-20261234'")

    def test_downstream_match_cannot_reactivate_an_excluded_reference(self):
        quantities = self.mes().assign(sales_order="SO-20261234'")
        row = execution_metrics(self.sales(), quantities).iloc[0]
        self.assertEqual(row['SO WIP Qty'], 0)
        self.assertEqual(row['Picked'], 'No')

    def test_db_adapter_matches_mes_v2_shipped_and_source_policy(self):
        records = [dict(sales_order='SO-20261234', items=[dict(item='ABC', quantity=qty)],
                        is_shipped=shipped, record_source=source)
                   for qty, shipped, source in [(8, False, 'mes'), (3, True, 'mes'), (99, False, 'legacy')]]
        rows = quantities_from_saved_wos(records, ['SO-20261234'])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows.iloc[0]['quantity'], 8)
        self.assertEqual(rows.iloc[0]['shipped_quantity'], 3)

    def test_current_mes_lookup_replaces_stale_spreadsheet_wip(self):
        inventory = pd.DataFrame([{'Part_Number': 'ABC', 'On Hand': 20, 'WIP_Qty': 2, 'WIP': 'stale'}])
        result = transform_inventory(inventory, build_wip_lookup(self.sales(), self.mes()))
        self.assertEqual(result.iloc[0]['WIP_Qty'], 8)
        self.assertEqual(result.iloc[0]['On Hand - WIP'], 12)
        result = transform_inventory(inventory, build_wip_lookup(self.sales(), self.mes(wip=0)))
        self.assertEqual(result.iloc[0]['WIP_Qty'], 0)
        self.assertEqual(result.iloc[0]['On Hand - WIP'], 20)

    def test_structured_keeps_order_wip_distinct_from_item_wip(self):
        inventory = pd.DataFrame([{'Part_Number': 'ABC', 'On Hand': 274, 'WIP_Qty': 43,
            'Available': 0, 'On PO': 0, 'Reorder Pt (Min)': 0, 'Sales/Week': 0}])
        output, _ = build_structured_df(self.sales(), self.mes(), inventory,
            pd.DataFrame(columns=['WO', 'Product Number']), pd.DataFrame(columns=['Name', 'Item', 'Qty(+)']))
        row = output.iloc[0]
        self.assertEqual(row['Qty(-)'], 9)
        self.assertEqual(row['Ordered Qty'], 17)
        self.assertEqual(row['SO WIP Qty'], 8)
        self.assertEqual(row['Picked_Qty'], 8)
        self.assertEqual(row['WIP_Qty'], 43)
        self.assertEqual(row['On Hand - WIP'], 231)
        self.assertEqual(row['Shipped Qty'], 8)  # legacy source value retained, not physical
        self.assertEqual(row['Physically Shipped Qty'], 0)

    def test_negative_free_stock_is_not_silently_hidden(self):
        inventory = pd.DataFrame([{'Part_Number': 'ABC', 'On Hand': 2, 'WIP_Qty': 8,
            'Available': 0, 'On PO': 0, 'Reorder Pt (Min)': 0, 'Sales/Week': 0}])
        output, _ = build_structured_df(self.sales(), self.mes(), inventory,
            pd.DataFrame(columns=['WO', 'Product Number']), pd.DataFrame(columns=['Name', 'Item', 'Qty(+)']))
        self.assertEqual(output.iloc[0]['On Hand - WIP'], -6)

    def item_table(self, sales):
        source = Path(__file__).resolve().parents[2] / 'Webpage' / 'server.py'
        node = next(n for n in ast.parse(source.read_text(encoding='utf-8-sig')).body
                    if isinstance(n, ast.FunctionDef) and n.name == '_so_table_for_item')
        sales = sales.assign(**{'On Hand - WIP': 231, 'Ship Date': '2026-11-13'})
        ctx = dict(pd=pd, SO_INV=sales, normalize_wo_number=normalize_wo_number,
                   EXECUTION_COLUMNS=EXECUTION_COLUMNS,
                   _execution_metrics_for_orders=lambda rows: execution_metrics(rows, self.mes()),
                   _to_date_str=lambda s: pd.to_datetime(s).dt.strftime('%Y-%m-%d'))
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<item-execution-table>', 'exec'), ctx)
        return ctx['_so_table_for_item']('ABC')

    def test_item_table_shows_physical_metrics_not_source_qty_as_picked(self):
        headers, rows, _ = self.item_table(self.sales())
        self.assertIn('Physically Shipped Qty', headers)
        self.assertNotIn('Qty(-)', headers)
        self.assertEqual(rows[0]['Ordered Qty'], '17')
        self.assertEqual(rows[0]['Picked (WIP)'], '8')
        self.assertEqual(rows[0]['Awaiting Pick Qty'], '9')
        self.assertEqual(rows[0]['Physically Shipped Qty'], '0')

    def test_item_table_collapses_duplicate_lines_without_doubling_wip(self):
        sales = pd.concat([self.sales(ordered=10, outstanding=6), self.sales(ordered=7, outstanding=3)])
        _, rows, _ = self.item_table(sales)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['Ordered Qty'], '17')
        self.assertEqual(rows[0]['Picked (WIP)'], '8')
        self.assertEqual(rows[0]['Awaiting Pick Qty'], '9')

    def test_item_table_refreshes_stale_persisted_execution_metrics(self):
        sales = self.sales().assign(**{'SO WIP Qty': 999, 'Picked': 'Picked'})
        _, rows, _ = self.item_table(sales)
        self.assertEqual(rows[0]['Picked (WIP)'], '8')
        self.assertEqual(rows[0]['Picked'], 'Partial')

    def test_web_legacy_site_inference_uses_all_source_sites(self):
        source = Path(__file__).resolve().parents[2] / 'Webpage' / 'server.py'
        node = next(n for n in ast.parse(source.read_text(encoding='utf-8-sig')).body
                    if isinstance(n, ast.FunctionDef) and n.name == '_execution_metrics_for_orders')
        records = [dict(sales_order='SO-20261234', record_source='mes', is_shipped=False,
                        items=[dict(item='ABC', quantity=8)])]
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.mappings.return_value = records
        all_sites = pd.concat([self.sales(), self.sales().assign(**{'Inventory Site': 'WH02D'})])
        ctx = dict(pd=pd, text=text, bindparam=bindparam, engine=engine,
                   normalize_item=normalize_item, normalize_wo_number=normalize_wo_number,
                   quantities_from_saved_wos=quantities_from_saved_wos,
                   execution_metrics=execution_metrics,
                   _read_table=lambda schema, table: all_sites.copy())
        exec(compile(ast.Module(body=[node], type_ignores=[]), '<execution-db-read>', 'exec'), ctx)
        with self.assertRaisesRegex(ValueError, 'Missing MES warehouse'):
            ctx['_execution_metrics_for_orders'](self.sales())
        for call in engine.connect.return_value.__enter__.return_value.execute.call_args_list:
            self.assertTrue(str(call.args[0]).startswith('SELECT '))


if __name__ == '__main__':
    unittest.main()
