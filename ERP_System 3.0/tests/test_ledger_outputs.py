import pandas as pd

from erp_system.ledger.ledger import build_ledger_from_events
from erp_system.runtime.constants import PLACEHOLDER_DATE


def test_ledger_returns_balances_and_dated_so_shortages():
    today = pd.Timestamp.today().normalize()
    so = pd.DataFrame({"Item": ["TEST-PART"], "On Hand": [10]})
    events = pd.DataFrame([
        {"Item": "TEST-PART", "Date": today, "Delta": -12, "Kind": "OUT", "Source": "SO"},
        {"Item": "TEST-PART", "Date": today + pd.Timedelta(days=1), "Delta": 5, "Kind": "IN", "Source": "HQ"},
        {"Item": "TEST-PART", "Date": PLACEHOLDER_DATE, "Delta": -8, "Kind": "OUT", "Source": "SO"},
    ])
    ledger, violations = build_ledger_from_events(so, events)
    assert ledger["Projected_NAV"].tolist() == [10, -2, 3, -5]
    outbound = ledger.loc[ledger["Kind"].eq("OUT")]
    assert outbound["NAV_before"].tolist() == [10, 3]
    assert outbound["NAV_after"].tolist() == [-2, -5]
    assert violations["Date"].tolist() == [today]
