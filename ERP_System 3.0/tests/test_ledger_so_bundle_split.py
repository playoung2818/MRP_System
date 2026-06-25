from __future__ import annotations

import pandas as pd

from erp_system.ledger.events import build_events


def test_build_events_splits_nru_52s_jetson_so_bundle() -> None:
    so = pd.DataFrame(
        [
            {
                "Ship Date": "2026-07-10",
                "Item": "NRU-52S+-JON16-NS",
                "Qty(-)": 1,
                "QB Num": "SO-20260815",
                "P. O. #": "",
                "Name": "Customer",
            }
        ]
    )
    nav = pd.DataFrame(columns=["Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"])

    events = build_events(so, nav)

    outbound = events.loc[events["Source"].eq("SO")].sort_values("Item").reset_index(drop=True)
    assert outbound["Item"].tolist() == ["GC-JETSON-NX16G-ORIN-NVIDIA", "NRU-52S+"]
    assert outbound["Delta"].tolist() == [-1, -1]
    assert outbound["Item_raw"].tolist() == ["NRU-52S+-JON16-NS", "NRU-52S+-JON16-NS"]
