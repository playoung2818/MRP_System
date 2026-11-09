"""Pure comparison tests; no server imports or database writes."""
from pathlib import Path
import runpy
import pandas as pd

compare = runpy.run_path(str(Path(__file__).resolve().parents[2] / 'Webpage' / 'assigned_ship_dates.py'))['assigned_placeholder_ship_dates']


def test_only_assigned_orders_with_2099_dates_count_once():
    sales = pd.DataFrame([
        {'QB Num': so, 'Ship Date': date, 'Name': 'Customer'}
        for so, date in [('SO-20260001', '2099-12-31'), ('SO-20260001', '2099-07-04'),
            ('SO-20260001', '2026-12-01'), ('SO-20260002', '2099-12-31'),
            ('SO-20260003', '2026-12-01'), ('SO-20260004', None),
            ('SO-20260005', '2099-12-31'), ('SO-20260006', '2100-12-31')]
    ])
    assignments = pd.DataFrame([
        {'wo_number': so, 'production_date': date}
        for so, date in [('SO-20260001', '2026-11-10'), ('SO-20260003', '2026-11-10'),
                         ('SO-20260004', '2026-11-10'), ('SO-20260005', None),
                         ('SO-20260006', '2026-11-10')]
    ])
    original = sales.copy(deep=True)
    rows = compare(sales, assignments)
    assert len(rows) == 1
    assert rows[0]['so'] == 'SO-20260001'
    assert rows[0]['ship_dates'] == '2099-07-04, 2099-12-31'
    assert rows[0]['production_date'] == '2026-11-10'
    pd.testing.assert_frame_equal(sales, original)


def test_empty_or_missing_input_is_safe():
    assert compare(None, pd.DataFrame()) == []
    assert compare(pd.DataFrame({'Item': ['CPU']}), pd.DataFrame({'wo_number': ['X']})) == []
