from unittest.mock import patch

import pandas as pd
from pandas.testing import assert_frame_equal

from mrp_system.ledger import events as event_module
from mrp_system.ledger import ledger as ledger_module


def test_raw_movements_are_normalized_once_after_opening_rows():
    today = pd.Timestamp.today().normalize()
    so = pd.DataFrame({
        "Item": ["TEST-PART"], "Ship Date": [today], "Qty(-)": [12],
        "QB Num": ["SO-TEST"], "P. O. #": [""], "Name": ["Test"],
    })
    sap = pd.DataFrame({
        "Item": ["TEST-PART"], "Date": [today], "Qty(+)": [5],
        "QB Num": ["POD-TEST"], "P. O. #": [""], "Name": ["Test"],
    })
    inventory = pd.DataFrame({"Part_Number": ["TEST-PART"], "On Hand": [10]})
    with patch.object(event_module, "POD_SITE", {}), patch.object(
        event_module, "_order_events", wraps=event_module._order_events
    ) as early_order, patch.object(
        ledger_module, "_order_events", wraps=ledger_module._order_events
    ) as final_order:
        movements = event_module.build_events(so, sap)
        ledger, violations = ledger_module.build_ledger_from_events(so, movements, inventory)
    early_order.assert_not_called()
    final_order.assert_called_once()
    assert "OPEN" in final_order.call_args.args[0]["Kind"].tolist()
    assert ledger["Kind"].astype(str).tolist() == ["OPEN", "IN", "OUT"]
    assert ledger["Projected_NAV"].tolist() == [10, 15, 3]
    assert violations.empty


def test_unsorted_events_cleaned_with_stable_same_day_priority():
    today = pd.Timestamp.today().normalize()
    so = pd.DataFrame({"Item": ["TEST-PART"], "On Hand": [0]})
    movements = pd.DataFrame([
        {"Item": "TEST-PART", "Date": today, "Delta": "-12", "Kind": "OUT", "Source": "SO", "QB Num": "SO-A"},
        {"Item": "TEST-PART", "Date": today, "Delta": "20", "Kind": "IN", "Source": "HQ"},
        {"Item": "TEST-PART", "Date": today, "Delta": 1, "Kind": "ADJ", "Source": "Test"},
        {"Item": "TEST-PART", "Date": today, "Delta": -2, "Kind": "OUT", "Source": "SO", "QB Num": "SO-B"},
        {"Item": "TEST-PART", "Date": "invalid", "Delta": 5, "Kind": "IN"},
        {"Item": "TEST-PART", "Date": today, "Delta": "invalid", "Kind": "IN"},
        {"Item": None, "Date": today, "Delta": 5, "Kind": "IN"},
        {"Item": "TEST-PART", "Date": today, "Delta": 0, "Kind": "OUT"},
    ])
    ledger, violations = ledger_module.build_ledger_from_events(so, movements)
    assert ledger["Kind"].astype(str).tolist() == ["OPEN", "IN", "ADJ", "OUT", "OUT"]
    assert ledger["Projected_NAV"].tolist() == [0, 20, 21, 9, 7]
    assert ledger.loc[ledger["Kind"].eq("OUT"), "QB Num"].tolist() == ["SO-A", "SO-B"]
    assert violations.empty

    # Reproduce the former pre-ordering passes and compare complete results.
    preordered = event_module._order_events(event_module._order_events(movements))
    old_ledger, old_violations = ledger_module.build_ledger_from_events(so, preordered)
    # Merging a null-item row before cleanup can promote Opening to float;
    # the row is discarded and all retained balances must remain identical.
    assert_frame_equal(ledger, old_ledger, check_dtype=False)
    assert_frame_equal(violations, old_violations, check_dtype=False)
