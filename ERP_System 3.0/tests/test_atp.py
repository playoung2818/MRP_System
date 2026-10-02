import pandas as pd

from erp_system.ledger.atp import (
    build_atp_view,
    build_adjusted_item_atp,
    earliest_assignment_date,
    earliest_atp_by_projected_nav,
    earliest_atp_strict,
)
from erp_system.runtime.constants import PLACEHOLDER_DATE


def test_point_in_time_availability_is_distinct_from_strict_atp():
    start = pd.Timestamp("2026-10-02")
    ledger = pd.DataFrame({
        "Item": ["CPU"] * 3,
        "Date": [start, start + pd.Timedelta(days=1), PLACEHOLDER_DATE],
        "Projected_NAV": [60, 40, 30],
    })
    assert earliest_atp_by_projected_nav(ledger, "CPU", 50, start) == start
    assert earliest_atp_strict(build_atp_view(ledger), "CPU", 50, start) is None
    assert earliest_atp_by_projected_nav(ledger, "MISSING", 1, start) is None
    assert earliest_atp_by_projected_nav(ledger, "CPU", "invalid", start) is None


def test_assignment_cutoff_modes_share_centralized_atp():
    start = pd.Timestamp("2026-10-02")
    ledger = pd.DataFrame({
        "Item": ["CPU"] * 4,
        "Date": [start, start, PLACEHOLDER_DATE, PLACEHOLDER_DATE],
        "Opening": [60] * 4,
        "Delta": [0, -50, -20, 100],
        "Kind": ["OPEN", "OUT", "OUT", "IN"],
        "QB Num": ["", "SO-TARGET", "SO-OTHER", "POD-UNKNOWN"],
    })
    adjusted = build_adjusted_item_atp(ledger, qb_num="SO-TARGET", item="CPU", from_date=start)
    assert adjusted["Projected_NAV"].tolist() == [60, 40]
    assert adjusted["FutureMin_NAV"].tolist() == [40, 40]
    kwargs = dict(qb_num="SO-TARGET", item="CPU", qty=50, from_date=start, cutoff=PLACEHOLDER_DATE)
    assert earliest_assignment_date(ledger, include_cutoff_in_check=True, **kwargs) is None
    assert earliest_assignment_date(ledger, include_cutoff_in_check=False, **kwargs) == start
