from __future__ import annotations

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

