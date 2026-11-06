"""Read-only coverage tests; do not import the database-backed server."""
from pathlib import Path
import runpy

import pandas as pd
from flask import Flask, render_template

ROOT = Path(__file__).resolve().parents[2]
coverage = runpy.run_path(str(ROOT / 'Webpage' / 'stock_coverage.py'))['stock_coverage_exceptions']


def test_strict_thresholds_and_deduplication():
    frame = pd.DataFrame([
        {'Part_Number': item, 'Available': available, 'On PO': 2, 'Sales/Week': 10}
        for item, available in [('LOW', 7), ('LOW', 7), ('LOW_EDGE', 8),
                                ('OK', 18), ('HIGH_EDGE', 38), ('HIGH', 39)]
    ])
    original = frame.copy(deep=True)
    rows = coverage(frame)
    assert [row['item'] for row in rows] == ['LOW', 'HIGH']
    assert [row['total'] for row in rows] == [9, 41]
    pd.testing.assert_frame_equal(frame, original)


def test_zero_sales_invalid_and_negative_values():
    frame = pd.DataFrame([
        {'Part_Number': item, 'Available': available, 'On PO': 0, 'Sales/Week': weekly}
        for item, available, weekly in [('ZERO', 0, 0), ('EXCESS', 2, 0),
            ('NEG_STOCK', -1, 0), ('MISSING', None, 10), ('INVALID', 'bad', 10),
            ('NEG_SALES', 1, -1), ('INF', float('inf'), 2), ('', 1, 10)]
    ])
    assert coverage(frame) == []
    assert coverage(None) == []
    assert coverage(pd.DataFrame({'Part_Number': ['A']})) == []


def test_minimum_weekly_sales_is_inclusive():
    frame = pd.DataFrame([
        {'Part_Number': item, 'Available': available, 'On PO': 0, 'Sales/Week': weekly}
        for item, available, weekly in [('BELOW_MIN', 10, 0.1999),
            ('MIN_LOW', 0, 0.2), ('MIN_HIGH', 1, 0.2), ('MIN_OK', 0.5, 0.2)]
    ])
    assert [row['item'] for row in coverage(frame)] == ['MIN_LOW', 'MIN_HIGH']


def test_detail_template_escapes_items_and_has_search_links():
    app = Flask(__name__, template_folder=str(ROOT / 'Webpage' / 'templates'))
    app.add_url_rule('/global_search', endpoint='global_search', view_func=lambda: '')
    rows = coverage(pd.DataFrame([{'Part_Number': '<part>', 'Available': 1,
                                  'On PO': 0, 'Sales/Week': 2}]))
    with app.test_request_context('/'):
        html = render_template('stock_coverage.html', rows=rows, loaded_at='test')
    assert '&lt;part&gt;' in html
    assert 'Below 1× weekly sales' in html
    assert '/global_search?q=' in html
