from __future__ import annotations

import pandas as pd

from erp_system.ledger.events import expand_nav_preinstalled
from erp_system.ledger.events import expand_preinstalled_row
from erp_system.ledger.events import parse_description
from erp_system.transform.shipping import SHIPPING_MODEL_GROUP_MAPPINGS


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


def test_nuvo_716_variant_split_preserves_included_components() -> None:
    nav = pd.DataFrame(
        [
            {
                "QB Num": "POD-260859",
                "Item": "Nuvo-7162GC-PoE",
                "Description": "Nuvo-7162GC-PoE, including 2x DDR4-8GB-WT32-SM",
                "Ship Date": "2026-07-08",
                "Qty(+)": 1,
                "Pre/Bare": "Pre",
            }
        ]
    )

    expanded = expand_nav_preinstalled(nav)
    by_item = expanded.groupby("Item", as_index=False)["Qty(+)"].sum()

    assert dict(zip(by_item["Item"], by_item["Qty(+)"])) == {
        "CSM-7162GC": 1.0,
        "DDR4-8GB-WT32-SM": 2.0,
        "Nuvo-716xGC-PoE": 1.0,
    }


def test_configured_shipping_model_group_expands_to_inventory_items() -> None:
    nav = pd.DataFrame(
        [
            {
                "QB Num": "POD-GROUP",
                "Item": "nru-161v-awp-jon16-rc01",
                "Description": "",
                "Ship Date": "2026-07-10",
                "Qty(+)": 2,
                "Pre/Bare": "Bare",
            }
        ]
    )

    expanded = expand_nav_preinstalled(nav)

    assert dict(zip(expanded["Item"], expanded["Qty(+)"])) == {
        "NRU-161V-AWP": 2.0,
        "GC-Jetson-NX16G-Orin-Nvidia": 2.0,
        "M.242-SSD-256GB-P34-TLC5WT-TD1": 2.0,
    }


def test_configured_shipping_model_group_uses_item_quantity_multiplier() -> None:
    key = "TEST-GROUP-QTY"
    SHIPPING_MODEL_GROUP_MAPPINGS[key] = (
        ("TEST-PARENT", 1.0),
        ("TEST-ACCESSORY", 2.0),
    )
    try:
        nav = pd.DataFrame(
            [
                {
                    "QB Num": "POD-GROUP",
                    "Item": key,
                    "Description": "",
                    "Ship Date": "2026-07-10",
                    "Qty(+)": 3,
                    "Pre/Bare": "Bare",
                }
            ]
        )

        expanded = expand_nav_preinstalled(nav)

        assert dict(zip(expanded["Item"], expanded["Qty(+)"])) == {
            "TEST-PARENT": 3.0,
            "TEST-ACCESSORY": 6.0,
        }
    finally:
        SHIPPING_MODEL_GROUP_MAPPINGS.pop(key, None)


def test_pod_260978_flyc_300_group_parses_into_ledger_items() -> None:
    nav = pd.DataFrame(
        [
            {
                "QB Num": "POD-260978",
                "Item": "FLYC-300-JON16-IN01",
                "Description": (
                    "FLYC-300-EC, including GC-OrinNX16G-JetPack 6.2_FLYC-300, "
                    "M.230-SSD-1TB-PCIe4-TLC-TD"
                ),
                "Ship Date": "2026-07-10",
                "Qty(+)": 1,
                "Pre/Bare": "Pre",
            }
        ]
    )

    expanded = expand_nav_preinstalled(nav)

    assert expanded[["QB Num", "Parent_Item", "Item", "Qty_per_parent", "Qty(+)", "IsParent"]].to_dict("records") == [
        {
            "QB Num": "POD-260978",
            "Parent_Item": "FLYC-300-JON16-IN01",
            "Item": "FLYC-300-EC-JON16-NS",
            "Qty_per_parent": 1.0,
            "Qty(+)": 1.0,
            "IsParent": False,
        },
        {
            "QB Num": "POD-260978",
            "Parent_Item": "FLYC-300-JON16-IN01",
            "Item": "M.230-SSD-1TB-PCIe4-TLC-TD",
            "Qty_per_parent": 1.0,
            "Qty(+)": 1.0,
            "IsParent": False,
        },
    ]
