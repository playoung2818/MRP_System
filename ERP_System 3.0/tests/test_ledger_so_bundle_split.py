from __future__ import annotations

import pandas as pd

from erp_system.ledger.events import build_events


def test_build_events_does_not_split_so_items() -> None:
    so = pd.DataFrame(
        [
            {
                "Ship Date": "2026-07-10",
                "Item": "NRU-52S+-JON16-NS",
                "Qty(-)": 1,
                "QB Num": "SO-20260815",
                "P. O. #": "",
                "Name": "Customer",
            },
            {
                "Ship Date": "2026-07-11",
                "Item": "FLYC-300-EC-JON16-NS",
                "Qty(-)": 2,
                "QB Num": "SO-20260816",
                "P. O. #": "",
                "Name": "Customer",
            }
        ]
    )
    nav = pd.DataFrame(columns=["Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"])

    events = build_events(so, nav)

    outbound = events.loc[events["Source"].eq("SO")].sort_values("Item").reset_index(drop=True)
    assert outbound["Item"].tolist() == ["FLYC-300-EC-JON16-NS", "NRU-52S+-JON16-NS"]
    assert outbound["Delta"].tolist() == [-2, -1]
    assert outbound["Item_raw"].tolist() == ["FLYC-300-EC-JON16-NS", "NRU-52S+-JON16-NS"]
