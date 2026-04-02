from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, NotRequired, TypedDict

import pandas as pd


COLUMN_TYPE_STRING: Literal["string"] = "string"
COLUMN_TYPE_NUMBER: Literal["number"] = "number"
COLUMN_TYPE_DATETIME: Literal["datetime"] = "datetime"
ColumnType = Literal["string", "number", "datetime"]


class InventoryStatusRow(TypedDict, total=False):
    Part_Number: str
    On_Hand: NotRequired[float]
    On_Sales_Order: NotRequired[float]
    On_PO: NotRequired[float]
    Available: NotRequired[float]
    WIP: NotRequired[str]
    WIP_Qty: NotRequired[float]
    On_Hand_minus_WIP: NotRequired[float]


class WOStructuredRow(TypedDict, total=False):
    Order_Date: NotRequired[str]
    Name: NotRequired[str]
    QB_Num: str
    Item: str
    Qty_out: float
    Ship_Date: NotRequired[str]
    Component_Status: NotRequired[str]
    Available: NotRequired[float]
    On_Hand: NotRequired[float]
    On_PO: NotRequired[float]


class NTShippingScheduleRow(TypedDict, total=False):
    SO_NO: NotRequired[str]
    QB_Num: str
    Item: str
    Description: NotRequired[str]
    Ship_Date: NotRequired[str]
    Qty_in: float
    Pre_Bare: NotRequired[str]
    Order_Qty: NotRequired[float]
    Reference: NotRequired[str]


class OpenPurchaseOrdersRow(TypedDict, total=False):
    Order_Date: NotRequired[str]
    QB_Num: str
    POD: NotRequired[str]
    Item: str
    Qty_in: float
    Name: NotRequired[str]
    Ship_Date: NotRequired[str]
    Source_Name: NotRequired[str]


class LedgerAnalyticsRow(TypedDict, total=False):
    Date: str
    Item: str
    Delta: float
    Kind: str
    Source: NotRequired[str]
    Item_raw: NotRequired[str]
    Opening: NotRequired[float]
    CumDelta: NotRequired[float]
    Projected_NAV: NotRequired[float]
    QB_Num: NotRequired[str]
    PO_Num: NotRequired[str]
    Name: NotRequired[str]


@dataclass(frozen=True)
class TableContract:
    table_name: str
    required_columns: tuple[str, ...]
    column_types: dict[str, ColumnType] = field(default_factory=dict)


TABLE_CONTRACTS: dict[str, TableContract] = {
    "inventory_status": TableContract(
        table_name="inventory_status",
        required_columns=("Part_Number", "On Hand", "On Sales Order", "On PO", "Available"),
        column_types={
            "Part_Number": COLUMN_TYPE_STRING,
            "On Hand": COLUMN_TYPE_NUMBER,
            "On Sales Order": COLUMN_TYPE_NUMBER,
            "On PO": COLUMN_TYPE_NUMBER,
            "Available": COLUMN_TYPE_NUMBER,
            "WIP": COLUMN_TYPE_STRING,
            "WIP_Qty": COLUMN_TYPE_NUMBER,
            "On Hand - WIP": COLUMN_TYPE_NUMBER,
        },
    ),
    "wo_structured": TableContract(
        table_name="wo_structured",
        required_columns=("QB Num", "Item", "Qty(-)", "Ship Date"),
        column_types={
            "Order Date": COLUMN_TYPE_DATETIME,
            "Name": COLUMN_TYPE_STRING,
            "QB Num": COLUMN_TYPE_STRING,
            "Item": COLUMN_TYPE_STRING,
            "Qty(-)": COLUMN_TYPE_NUMBER,
            "Ship Date": COLUMN_TYPE_DATETIME,
            "Component_Status": COLUMN_TYPE_STRING,
            "Available": COLUMN_TYPE_NUMBER,
            "On Hand": COLUMN_TYPE_NUMBER,
            "On PO": COLUMN_TYPE_NUMBER,
        },
    ),
    "NT Shipping Schedule": TableContract(
        table_name="NT Shipping Schedule",
        required_columns=("QB Num", "Item", "Ship Date", "Qty(+)", "Pre/Bare"),
        column_types={
            "SO NO.": COLUMN_TYPE_STRING,
            "QB Num": COLUMN_TYPE_STRING,
            "Item": COLUMN_TYPE_STRING,
            "Description": COLUMN_TYPE_STRING,
            "Ship Date": COLUMN_TYPE_DATETIME,
            "Qty(+)": COLUMN_TYPE_NUMBER,
            "Pre/Bare": COLUMN_TYPE_STRING,
            "Order Qty": COLUMN_TYPE_NUMBER,
            "Reference": COLUMN_TYPE_STRING,
        },
    ),
    "Open_Purchase_Orders": TableContract(
        table_name="Open_Purchase_Orders",
        required_columns=("Order Date", "QB Num", "Item", "Qty(+)"),
        column_types={
            "Order Date": COLUMN_TYPE_DATETIME,
            "QB Num": COLUMN_TYPE_STRING,
            "POD#": COLUMN_TYPE_STRING,
            "Item": COLUMN_TYPE_STRING,
            "Qty(+)": COLUMN_TYPE_NUMBER,
            "Name": COLUMN_TYPE_STRING,
            "Ship Date": COLUMN_TYPE_DATETIME,
            "Source Name": COLUMN_TYPE_STRING,
        },
    ),
    "ledger_analytics": TableContract(
        table_name="ledger_analytics",
        required_columns=("Date", "Item", "Delta", "Kind", "Projected_NAV"),
        column_types={
            "Date": COLUMN_TYPE_DATETIME,
            "Item": COLUMN_TYPE_STRING,
            "Delta": COLUMN_TYPE_NUMBER,
            "Kind": COLUMN_TYPE_STRING,
            "Source": COLUMN_TYPE_STRING,
            "Item_raw": COLUMN_TYPE_STRING,
            "Opening": COLUMN_TYPE_NUMBER,
            "CumDelta": COLUMN_TYPE_NUMBER,
            "Projected_NAV": COLUMN_TYPE_NUMBER,
            "QB Num": COLUMN_TYPE_STRING,
            "P. O. #": COLUMN_TYPE_STRING,
            "Name": COLUMN_TYPE_STRING,
        },
    ),
}


def _coerce_series(series: pd.Series, kind: ColumnType) -> pd.Series:
    if kind == COLUMN_TYPE_NUMBER:
        return pd.to_numeric(series, errors="coerce")
    if kind == COLUMN_TYPE_DATETIME:
        return pd.to_datetime(series, errors="coerce")
    return series.astype("string")


def validate_output_table(df: pd.DataFrame, contract: TableContract) -> pd.DataFrame:
    if df is None:
        raise TypeError(f"{contract.table_name}: expected DataFrame, got None")

    out = df.copy()
    missing = [col for col in contract.required_columns if col not in out.columns]
    if missing:
        available = ", ".join(map(str, out.columns.tolist()))
        raise ValueError(
            f"{contract.table_name}: missing required columns {missing}. "
            f"Available columns: [{available}]"
        )

    for col, kind in contract.column_types.items():
        if col in out.columns:
            out[col] = _coerce_series(out[col], kind)

    return out


def ensure_contract_columns(
    df: pd.DataFrame | None,
    contract: TableContract,
    *,
    extra_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    """
    Prepare a frame for downstream consumers by ensuring the canonical contract
    columns exist, plus any module-specific extras.
    Unlike validate_output_table, this is non-failing and is intended for
    read-side consumers that can operate with empty/default columns.
    """
    out = pd.DataFrame() if df is None else df.copy()
    for col in (*contract.required_columns, *extra_columns):
        if col not in out.columns:
            out[col] = pd.NA
    return out


__all__ = [
    "COLUMN_TYPE_DATETIME",
    "COLUMN_TYPE_NUMBER",
    "COLUMN_TYPE_STRING",
    "ensure_contract_columns",
    "TABLE_CONTRACTS",
    "TableContract",
    "validate_output_table",
]
