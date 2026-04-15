from __future__ import annotations

import pandas as pd

from erp_system.ledger.assignment_readiness import (
    build_assignment_readiness_reports,
    build_assignment_run_tables,
)


def test_assignment_readiness_marks_placeholder_so_ready_when_own_demand_is_removed() -> None:
    structured = pd.DataFrame(
        {
            "QB Num": ["SO-1"],
            "Name": ["Acme"],
            "P. O. #": ["PO-1"],
            "Order Date": ["2026-01-01"],
            "Ship Date": ["2099-07-04"],
            "Item": ["PART-1"],
            "Qty(-)": [5],
            "Component_Status": ["Available"],
        }
    )
    ledger = pd.DataFrame(
        {
            "Item": ["PART-1"],
            "Date": ["2099-07-04"],
            "Delta": [-5],
            "Kind": ["OUT"],
            "QB Num": ["SO-1"],
            "Opening": [10],
        }
    )

    summary_df, blocker_df = build_assignment_readiness_reports(
        structured,
        ledger,
        from_date=pd.Timestamp("2026-01-15"),
    )
    runs_df = build_assignment_run_tables(
        structured,
        ledger,
        from_date=pd.Timestamp("2026-01-15"),
        run_ts=pd.Timestamp("2026-01-15 08:00:00"),
    )

    assert blocker_df.empty
    assert len(summary_df) == 1
    assert bool(summary_df.loc[0, "Ready to be assigned"]) is True
    assert summary_df.loc[0, "Earliest Ready Date"] == "2026-01-15"

    assert set(runs_df["mode"]) == {"strict", "loose"}
    assert runs_df["is_ready"].tolist() == [True, True]
    assert set(runs_df["decision_status"]) == {"ready"}
