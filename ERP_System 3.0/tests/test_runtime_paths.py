from __future__ import annotations

import pandas as pd

from erp_system.ingest import sources
from erp_system.runtime.paths import BASE, WAREHOUSE_INV_FILE


def test_warehouse_inventory_file_uses_expected_daily_update_csv() -> None:
    assert WAREHOUSE_INV_FILE == BASE / "WH01S_04_28.CSV"


def test_extract_inputs_reads_warehouse_inventory_as_cp1252(mocker) -> None:
    read_csv = mocker.patch.object(pd, "read_csv", return_value=pd.DataFrame())
    mocker.patch.object(sources, "read_excel_safe", return_value=pd.DataFrame())

    sources.extract_inputs()

    read_csv.assert_any_call(str(WAREHOUSE_INV_FILE), encoding="cp1252")
