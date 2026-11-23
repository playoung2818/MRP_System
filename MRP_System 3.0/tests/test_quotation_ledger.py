"""Confirmed-horizon minimum highlighting must not include pending receipts/demand."""
import sys
from pathlib import Path
import runpy
import unittest

import pandas as pd
from flask import Flask, render_template_string

WEB_ROOT = Path(__file__).resolve().parents[2] / "Webpage"
sys.path.insert(0, str(WEB_ROOT))
from quotation_ledger import confirmed_minimum_flags, overall_minimum_flags


class QuotationLedgerHighlightTests(unittest.TestCase):
    def test_pending_lower_balance_does_not_hide_confirmed_minimum(self):
        flags = confirmed_minimum_flags(
            pd.Series(['2026-11-19', '2026-12-08', '2099-12-31', '2100-01-05']),
            pd.Series([1, -2, -28, 0]))
        self.assertEqual(flags.tolist(), [False, True, False, False])

    def test_ties_and_date_boundary_preserve_nonsequential_index(self):
        dates = pd.Series(['2099-12-30', '2026-12-08', '2099-12-31'], index=[8, 3, 12])
        values = pd.Series(['2', '2', '2'], index=[8, 3, 12])
        flags = confirmed_minimum_flags(dates, values)
        self.assertEqual(flags.index.tolist(), [8, 3, 12])
        self.assertEqual(flags.tolist(), [True, True, False])

    def test_invalid_and_pending_only_have_no_minimum(self):
        flags = confirmed_minimum_flags(
            pd.Series(['Lead Time Pending', None, '2099-12-31', '2026-12-08']),
            pd.Series([-10, -20, -30, 'invalid']))
        self.assertEqual(flags.tolist(), [False] * 4)

    def test_empty_and_numeric_comparison_before_rounding(self):
        self.assertEqual(confirmed_minimum_flags(pd.Series([], dtype=str), pd.Series([], dtype=float)).tolist(), [])
        flags = confirmed_minimum_flags(pd.Series(['2026-12-08'] * 3), pd.Series(['10', '2.2', '2.1']))
        self.assertEqual(flags.tolist(), [False, False, True])

    def test_template_never_colors_pending_minimum_even_with_stale_flag(self):
        template = runpy.run_path(str(WEB_ROOT / 'ui.py'))['QUOTE_TPL']
        app = Flask(__name__)
        with app.test_request_context('/quotation_lookup'):
            html = render_template_string(template, ledger_columns=['Date', 'Projected_Qty'], ledger_rows=[
                {'Date': '2026-12-08', 'Projected_Qty': '-2', '_is_min_nav': True},
                {'Date': '2099-12-31', 'Projected_Qty': '-28', '_is_min_nav': True},
            ])
        self.assertEqual(html.count('class="cell-projected-min"'), 2)
        self.assertIn('Orange Projected_OnHand', html)
        self.assertIn('background:#fed7aa', html)

    def test_overall_minimum_includes_pending_and_all_ties(self):
        values = pd.Series(['2', '-2', '-28', '-28', None, 'invalid'], index=[9, 4, 7, 1, 2, 3])
        flags = overall_minimum_flags(values)
        self.assertEqual(flags.index.tolist(), values.index.tolist())
        self.assertEqual(flags.tolist(), [False, False, True, True, False, False])
        self.assertEqual(overall_minimum_flags(pd.Series(['invalid', None])).tolist(), [False, False])
        self.assertEqual(overall_minimum_flags(pd.Series([], dtype=float)).tolist(), [])

    def test_only_date_and_projected_cells_colored_with_green_priority(self):
        template = runpy.run_path(str(WEB_ROOT / 'ui.py'))['QUOTE_TPL']
        app = Flask(__name__)
        with app.test_request_context('/quotation_lookup'):
            html = render_template_string(template,
                ledger_columns=['Date', 'Name', 'Projected_Qty'], ledger_rows=[
                    {'Date': '2026-12-08', 'Name': 'Confirmed minimum', 'Projected_Qty': '-2',
                     '_is_min_nav': True, '_is_overall_min_nav': False},
                    {'Date': '2099-12-31', 'Name': 'Overall minimum', 'Projected_Qty': '-28',
                     '_is_min_nav': False, '_is_overall_min_nav': True},
                    {'Date': '2026-12-09', 'Name': 'Both minima', 'Projected_Qty': '-28',
                     '_is_min_nav': True, '_is_overall_min_nav': True},
                ])
        self.assertEqual(html.count('class="cell-projected-min"'), 2)
        self.assertEqual(html.count('class="cell-projected-overall-min"'), 4)
        self.assertIn('<td class="">Confirmed minimum</td>', html)
        self.assertIn('<td class="">Overall minimum</td>', html)
        self.assertIn('<td class="">Both minima</td>', html)
        self.assertNotIn('table-warning', html)


if __name__ == '__main__':
    unittest.main()
