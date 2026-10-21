"""Dashboard cutoff tests without importing the database-backed web server."""
import ast
from pathlib import Path

import pandas as pd


def dashboard_functions(ledger):
    source = Path(__file__).resolve().parents[2] / 'Webpage' / 'server.py'
    names = {'_negative_inventory_detail_rows', '_dashboard_alerts', '_format_intish'}
    functions = [node for node in ast.parse(source.read_text(encoding='utf-8-sig')).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    context = {'pd': pd, 'LEDGER': ledger, 'OPEN_PO': None, '_LAST_LOADED_AT': None,
               'PLACEHOLDER_DATE': pd.Timestamp('2099-12-31')}
    exec(compile(ast.Module(body=functions, type_ignores=[]), '<negative-inventory-test>', 'exec'), context)
    return context


def test_report_and_alert_exclude_dates_at_or_after_placeholder():
    ledger = pd.DataFrame([
        {'Item': 'KNOWN', 'Date': '2099-12-30', 'Projected_NAV': -2},
        {'Item': 'PLACEHOLDER', 'Date': '2099-12-31', 'Projected_NAV': -1},
        {'Item': 'LATER', 'Date': '2100-01-05', 'Projected_NAV': -5},
        {'Item': 'INVALID', 'Date': None, 'Projected_NAV': -1},
        {'Item': 'POSITIVE', 'Date': '2026-10-21', 'Projected_NAV': 1},
    ])
    original = ledger.copy(deep=True)
    context = dashboard_functions(ledger)
    _, rows = context['_negative_inventory_detail_rows']()
    assert rows == [{'Item': 'KNOWN', 'Date': '2099-12-30', 'Projected Qty': '-2'}]
    alerts = context['_dashboard_alerts']()
    assert alerts[0]['label'] == '1 items will go negative in the future'
    pd.testing.assert_frame_equal(ledger, original)


def test_unknown_date_negatives_do_not_create_dashboard_alert():
    context = dashboard_functions(pd.DataFrame([
        {'Item': 'RAM', 'Date': '2100-01-05', 'Projected_NAV': -5},
        {'Item': 'KIT', 'Date': '2099-12-31', 'Projected_NAV': -1},
    ]))
    assert context['_negative_inventory_detail_rows']()[1] == []
    assert context['_dashboard_alerts']() == [{'label': 'No active system alerts.', 'href': ''}]
