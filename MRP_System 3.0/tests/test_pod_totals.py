import pandas as pd

from mrp_system.ledger.events import build_events
from mrp_system.transform.pod import transform_pod


def raw_row(label=None, **overrides):
    row = {"Unnamed: 0": label, "Num": None, "Date": None, "Deliv Date": None,
           "Backordered": None, "Qty": None, "Rcv'd": None, "Amount": None,
           "Source Name": None, "Memo": None}
    row.update(overrides)
    return row


def test_totals_are_removed_and_do_not_leak_item_sections():
    raw = pd.DataFrame([
        raw_row("CPU"),
        raw_row(Num="POD-1 (note)", Date="2026-10-01", **{"Deliv Date": "2026-10-12", "Backordered": 5, "Qty": 5, "Rcv'd": 0, "Amount": 10, "Source Name": "Vendor"}),
        raw_row("Total\u00a0CPU", Backordered=5, Qty=5, **{"Rcv'd": 0, "Amount": 10}),
        # Enough populated cells to survive the legacy dropna(thresh=5).
        raw_row(Num="POD-MISSING-ITEM", Date="2026-10-01", Backordered=9, Qty=9, **{"Rcv'd": 0, "Amount": 18}),
        raw_row("SSD"),
        raw_row(Num="POD-2", Date="2026-10-01", **{"Deliv Date": "2026-10-13", "Backordered": 2, "Qty": 2, "Rcv'd": 0, "Amount": 20, "Source Name": "Vendor"}),
        raw_row("TOTAL SSD", Backordered=2, Qty=2, **{"Rcv'd": 0, "Amount": 20}),
    ])
    pod = transform_pod(raw)
    assert pod["QB Num"].tolist() == ["POD-1", "POD-2"]
    assert pod["Item"].tolist() == ["CPU", "SSD"]
    assert pod["Qty(+)"].sum() == 7


def test_memo_fallback_keeps_real_items_but_not_missing_identifiers():
    raw = pd.DataFrame([
        raw_row(Num="POD-1", Date="2026-10-01", Memo="SSD* more details", Backordered=2, Qty=2, Amount=10),
        raw_row(Num=None, Date="2026-10-01", Memo="CPU details", Backordered=5, Qty=5, Amount=10),
        raw_row(Num="nan", Date="2026-10-01", Memo="CPU details", Backordered=5, Qty=5, Amount=10),
    ])
    pod = transform_pod(raw)
    assert pod["QB Num"].tolist() == ["POD-1"]
    assert pod["Item"].tolist() == ["SSD"]


def test_event_builder_excludes_invalid_and_total_pod_items():
    so = pd.DataFrame(columns=["Ship Date", "Item", "Qty(-)", "QB Num", "P. O. #", "Name"])
    sap = pd.DataFrame(columns=["Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"])
    pod = pd.DataFrame({
        "Item": [None, float("nan"), "nan", "<NA>", "", "Total CPU", "CPU"],
        "Ship Date": ["2026-10-12"] * 7, "Qty(+)": [5] * 7,
        "QB Num": ["POD-TEST"] * 7,
    })
    events = build_events(so, sap, pod)
    assert events["Item"].tolist() == ["CPU"]
    assert events["Delta"].sum() == 5
