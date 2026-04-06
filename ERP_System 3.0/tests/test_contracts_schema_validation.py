from __future__ import annotations

import pandas as pd
import pytest

from erp_system.contracts import TABLE_CONTRACTS, ensure_contract_columns, validate_output_table
from erp_system.ingest.sources import validate_input_tables


def test_validate_inventory_status_contract_accepts_expected_columns() -> None:
    df = pd.DataFrame(
        {
            "Part_Number": ["POC-1"],
            "On Hand": ["5"],
            "On Sales Order": ["2"],
            "On PO": ["3"],
            "Available": ["6"],
        }
    )

    out = validate_output_table(df, TABLE_CONTRACTS["inventory_status"])

    assert list(out.columns) == ["Part_Number", "On Hand", "On Sales Order", "On PO", "Available"]
    assert out["Part_Number"].dtype.name.startswith("string")
    assert float(out.loc[0, "On Hand"]) == 5.0


def test_validate_inventory_status_contract_fails_when_required_column_missing() -> None:
    df = pd.DataFrame(
        {
            "Part_Number": ["POC-1"],
            "On Hand": [5],
            "On Sales Order": [2],
            "On PO": [3],
        }
    )

    with pytest.raises(ValueError, match="inventory_status: missing required columns"):
        validate_output_table(df, TABLE_CONTRACTS["inventory_status"])


def test_validate_open_purchase_orders_contract_fails_for_renamed_qty_column() -> None:
    df = pd.DataFrame(
        {
            "Order Date": ["2026-04-01"],
            "QB Num": ["POD-260001"],
            "Item": ["Nuvo-716xGC-PoE"],
            "QtyPlus": [2],
        }
    )

    with pytest.raises(ValueError) as exc_info:
        validate_output_table(df, TABLE_CONTRACTS["Open_Purchase_Orders"])

    assert "Qty(+)" in str(exc_info.value)
    assert "QtyPlus" in str(exc_info.value)


def test_ensure_contract_columns_adds_missing_structured_fields_for_consumers() -> None:
    df = pd.DataFrame({"QB Num": ["SO-1"], "Item": ["SSD-1TB"]})

    out = ensure_contract_columns(
        df,
        TABLE_CONTRACTS["wo_structured"],
        extra_columns=("Name", "Component_Status"),
    )

    for col in ("QB Num", "Item", "Qty(-)", "Ship Date", "Name", "Component_Status"):
        assert col in out.columns
    assert pd.isna(out.loc[0, "Qty(-)"])


def test_validate_input_tables_requires_shipping_model_name_and_pod_inventory_site() -> None:
    shipping_df = pd.DataFrame({"Ship Date": ["2026-04-06"]})
    pod_df = pd.DataFrame({"QB Num": ["POD-1"]})

    with pytest.raises(ValueError) as exc_info:
        validate_input_tables(shipping_df, pod_df)

    msg = str(exc_info.value)
    assert "Model Name" in msg
    assert "Inventory Site" in msg
