from __future__ import annotations

import pandas as pd

from erp_system.ledger.events import expand_preinstalled_row
from erp_system.ledger.events import parse_description


def test_parse_description_splits_item_code_after_and() -> None:
    parent, components = parse_description(
        "SEMIL-1708-FF, including i7-9700E, DDR4-16GB-32-IK2 and M.280-SSD-1TB-PCIe4-TLCWT-IK1"
    )

    assert parent == "SEMIL-1708-FF"
    assert components == [
        "i7-9700E",
        "DDR4-16GB-32-IK2",
        "M.280-SSD-1TB-PCIe4-TLCWT-IK1",
    ]


def test_parse_description_keeps_natural_language_and() -> None:
    _, components = parse_description(
        "SEMIL-X, including Intel 8th-Gen Core in-vehicle controller with 4x M12 PoE+ ports, "
        "DIO, CAN bus and RAID, single-slot PCI Express Cassette."
    )

    assert "CAN bus and RAID" in components


def test_nru_preinstall_keeps_model_and_skips_first_included_component() -> None:
    row = pd.Series(
        {
            "Item": "NRU-52S+-JON16-NS",
            "Description": "NRU-52S+-JON16-NS, including GC-JETSON-NX16G-ORIN-NVIDIA, "
            "DDR4-8GB-WT32-SM, 2 x SSD-512GB-TLC5WT-TD1",
            "Qty(+)": 1,
        }
    )

    expanded = expand_preinstalled_row(row).sort_values("Item").reset_index(drop=True)

    assert expanded["Item"].tolist() == [
        "DDR4-8GB-WT32-SM",
        "NRU-52S+-JON16-NS",
        "SSD-512GB-TLC5WT-TD1",
    ]
    assert expanded["Qty(+)"].tolist() == [1.0, 1, 2.0]
