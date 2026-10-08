import unittest
from unittest.mock import patch, Mock
import pandas as pd
import requests
from mrp_system.transform.inventory import build_wip_lookup, transform_inventory
from mrp_system.ingest.mes import fetch_mes_quantities


class MesWipTests(unittest.TestCase):
    def sales(self, qty=10, shipped=0):
        return pd.DataFrame([{'QB Num': 'SO-20261234', 'Item': 'ABC', 'Inventory Site': 'WH01S-NTA', 'Qty(-)': qty, 'Shipped Qty': shipped, 'partial': shipped > 0}])

    def mes(self, qty=7, site='', shipped=0):
        return pd.DataFrame([{'sales_order': 'SO-20261234', 'item': 'ABC', 'inventory_site': site, 'quantity': qty, 'shipped_quantity': shipped}])

    def test_sales_transform_excludes_subtotals_and_missing_so_numbers(self):
        from mrp_system.transform.sales_order import transform_sales_order
        from mrp_system.transform.shipment_reconciliation import partial_shipment_report
        raw = pd.DataFrame([
            {'Unnamed: 0':'RAM', 'Num':None, 'Qty':None, 'Backordered':None},
            {'Unnamed: 0':None, 'Num':'SO-20261234', 'Qty':10, 'Backordered':7},
            {'Unnamed: 0':'Total RAM', 'Num':None, 'Qty':10, 'Backordered':7},
            {'Unnamed: 0':'TOTAL', 'Num':'nan', 'Qty':'subtotal', 'Backordered':'subtotal'},
            {'Unnamed: 0':'total Accessory', 'Num':'SO-20269999', 'Qty':10, 'Backordered':7},
            {'Unnamed: 0':'Other', 'Num':' ', 'Qty':10, 'Backordered':7},
        ])
        raw['Inventory Site'] = 'WH01S-NTA'
        sales = transform_sales_order(raw)
        self.assertEqual(sales['QB Num'].tolist(), ['SO-20261234'])
        self.assertEqual(sales['Item'].tolist(), ['RAM'])
        report = partial_shipment_report(sales, pd.DataFrame(columns=self.mes().columns))
        self.assertEqual(report['QB Num'].tolist(), ['SO-20261234'])
        self.assertEqual(report.iloc[0]['Shipped Qty'], 3)

    def test_partial_wo_counts_seven(self):
        wip = build_wip_lookup(self.sales(), self.mes())
        self.assertEqual(wip.iloc[0]['WIP_Qty'], 7)
        inventory = transform_inventory(pd.DataFrame([{'Part_Number': 'ABC', 'On Hand': 20}]), wip)
        self.assertEqual(inventory.iloc[0]['On Hand - WIP'], 13)

    def test_marked_shipments_are_not_subtracted_twice(self):
        self.assertEqual(build_wip_lookup(self.sales(qty=7, shipped=3), self.mes(qty=4, shipped=3)).iloc[0]['WIP_Qty'], 4)

    def test_partial_report_disappears_after_reconciliation(self):
        from mrp_system.transform.shipment_reconciliation import partial_shipment_report
        sales = self.sales(qty=7, shipped=3)
        pending = partial_shipment_report(sales, self.mes(qty=3))
        self.assertEqual(pending.iloc[0]['Qty to reconcile'], 3)
        self.assertTrue(partial_shipment_report(sales, self.mes(qty=0, shipped=3)).empty)
        self.assertTrue(build_wip_lookup(sales, self.mes(qty=0, shipped=3)).empty)

    def test_overmarked_shipment_is_reported(self):
        from mrp_system.transform.shipment_reconciliation import partial_shipment_report
        pending = partial_shipment_report(self.sales(qty=7, shipped=3), self.mes(qty=0, shipped=5))
        self.assertEqual(pending.iloc[0]['Qty to reconcile'], -2)

    def test_drop_ship_is_excluded_from_partial_report(self):
        from mrp_system.transform.shipment_reconciliation import partial_shipment_report
        for site in ('Drop Ship', 'drop ship', ' Drop ship '):
            sales = self.sales(qty=7, shipped=3)
            sales['Inventory Site'] = site
            self.assertTrue(partial_shipment_report(sales, self.mes(site=site)).empty)

    def test_nonpartial_so_does_not_need_reconciliation(self):
        from mrp_system.transform.shipment_reconciliation import partial_shipment_report
        self.assertTrue(partial_shipment_report(self.sales(), self.mes()).empty)

    def test_empty_mes_quantities_count_zero(self):
        self.assertTrue(build_wip_lookup(self.sales(), pd.DataFrame(columns=self.mes().columns)).empty)

    def test_duplicate_so_lines_do_not_double_count(self):
        self.assertEqual(build_wip_lookup(pd.concat([self.sales(qty=5), self.sales(qty=5)]), self.mes()).iloc[0]['WIP_Qty'], 7)

    def test_missing_site_is_not_guessed(self):
        other = self.sales()
        other['Inventory Site'] = 'WH02D'
        with self.assertRaises(ValueError):
            build_wip_lookup(pd.concat([self.sales(), other]), self.mes())

    def test_api_failure_never_falls_back(self):
        with patch('mrp_system.ingest.mes.requests.get', side_effect=requests.ConnectionError('offline')) as get:
            with self.assertRaises(requests.ConnectionError):
                fetch_mes_quantities(['SO-20261234'])
            self.assertEqual(get.call_count, 1)

    def test_api_queries_only_current_open_sos(self):
        response = Mock()
        response.json.return_value = dict(schema_version=2, quantities=self.mes().to_dict('records'))
        with patch('mrp_system.ingest.mes.requests.get', return_value=response) as get:
            rows = fetch_mes_quantities(['SO-20261234'])
            self.assertEqual(len(rows), 1)
            self.assertEqual(get.call_args.kwargs['params'], [('sales_order', 'SO-20261234')])

    def test_no_open_sos_does_not_fetch_historical_wos(self):
        with patch('mrp_system.ingest.mes.requests.get') as get:
            self.assertTrue(fetch_mes_quantities([]).empty)
            get.assert_not_called()

    def test_old_api_version_is_rejected(self):
        response = Mock()
        response.json.return_value = dict(schema_version=1, quantities=[])
        with patch('mrp_system.ingest.mes.requests.get', return_value=response):
            with self.assertRaises(ValueError):
                fetch_mes_quantities(['SO-20261234'])

    def test_old_flag_contract_is_rejected(self):
        with self.assertRaises(KeyError):
            build_wip_lookup(self.sales(), pd.DataFrame([{'WO_Number':'SO-20261234', 'status':'Picked'}]))

    def test_structured_uses_mes_quantities(self):
        from mrp_system.transform.structured import build_structured_df
        inventory = pd.DataFrame([dict(Part_Number='ABC', WIP_Qty=7, **{'On Hand':20, 'Available':20, 'On PO':0, 'Reorder Pt (Min)':0, 'Sales/Week':0})])
        output, _ = build_structured_df(self.sales(), self.mes(), inventory,
            pd.DataFrame(columns=['WO','Product Number']), pd.DataFrame(columns=['Name','Item','Qty(+)']))
        self.assertEqual(output.iloc[0]['Picked'], 'Partial')
        self.assertEqual(output.iloc[0]['On Hand - WIP'], 13)


if __name__ == '__main__':
    unittest.main()
