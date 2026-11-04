"""Quotation blockers follow saved Planning reviews, not inventory statuses."""
from pathlib import Path
import runpy
import unittest
from unittest.mock import MagicMock


with_planning_blockers = runpy.run_path(str(
    Path(__file__).resolve().parents[2] / 'Webpage' / 'quotation_blockers.py'
))['with_planning_blockers']


class QuotationBlockersTests(unittest.TestCase):
    def test_saved_blockers_and_missing_reviews(self):
        engine = MagicMock()
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.mappings.return_value = [
            {'qb_num': 'SO-1', 'blockers': [{'item': 'B'}, {'item': 'A'}, {'item': 'B'}]},
            {'qb_num': 'SO-2', 'blockers': []},
        ]
        rows = [{'QB Num': number, 'Waiting_Item': 'old inventory status', 'Projected_Qty': '5'}
                for number in ['SO-1', 'SO-1', 'SO-2', 'SO-3', '']]
        result = with_planning_blockers(engine, rows)
        self.assertEqual([r['Waiting_Item'] for r in result],
                         ['{A, B}', '{A, B}', '{}', 'Not reviewed', '{}'])
        self.assertTrue(all(r['Projected_Qty'] == '5' for r in result))
        self.assertTrue(all(r['Waiting_Item'] == 'old inventory status' for r in rows))
        query, params = conn.execute.call_args.args
        self.assertEqual(params['numbers'], ['SO-1', 'SO-2', 'SO-3'])
        self.assertIn('ORDER BY qb_num, assessed_at DESC, id DESC', str(query))
        # A cached ledger must pick up a later Planning review on the next request.
        conn.execute.return_value.mappings.return_value = [{'qb_num': 'SO-1', 'blockers': []}]
        self.assertEqual(with_planning_blockers(engine, rows)[0]['Waiting_Item'], '{}')

    def test_inbound_rows_have_no_waiting_items(self):
        engine = MagicMock()
        rows = [{'Kind': kind, 'QB Num': 'PO-1', 'Waiting_Item': 'old value'}
                for kind in ['IN', 'in', ' IN ']]
        result = with_planning_blockers(engine, rows)
        self.assertEqual([row['Waiting_Item'] for row in result], ['', '', ''])
        engine.connect.assert_not_called()
        self.assertTrue(all(row['Waiting_Item'] == 'old value' for row in rows))

    def test_empty_ledger_does_not_query_database(self):
        engine = MagicMock()
        self.assertEqual(with_planning_blockers(engine, []), [])
        engine.connect.assert_not_called()


if __name__ == '__main__':
    unittest.main()
