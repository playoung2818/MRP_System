"""PDF-only ordering preserves source row contents and unmatched line order."""
import inspect
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mrp_system.transform import item_order as ordering
from mrp_system.transform.structured import reorder_df_out_by_output


@pytest.fixture(autouse=True)
def isolated_normalizer(monkeypatch):
    aliases = {"a-alias": "A", "b-alias": "B"}
    monkeypatch.setattr(ordering, "normalize_item", lambda value: aliases.get(str(value).strip().casefold(), value))


def frame(items, order="SO-20261123"):
    return pd.DataFrame({"QB Num": [order] * len(items), "Item": items,
                         "Qty": list(range(1, len(items) + 1)), "line": list(range(len(items)))})


def test_pdf_reference_matches_aliases_and_repeated_occurrences():
    source = frame(["B", "A", "C", "A"])
    ref = pd.DataFrame({"QB Num": ["WO10-20261123"] * 3, "Item": ["A-alias", "B-alias", "A"]})
    result = reorder_df_out_by_output(ref, source)
    assert result["line"].tolist() == [1, 0, 3, 2]
    assert_frame_equal(result.sort_values("line").reset_index(drop=True), source)


def test_production_pdf_order_preserves_unmatched_rows():
    source = frame(["C", "A", "B", None, "Unknown"])
    pdf = pd.DataFrame({"QB Num": ["SO-20261123"] * 2, "Item": ["B", "A-alias"]})
    result = reorder_df_out_by_output(pdf, source)
    assert result["Item"].tolist() == ["B", "A", "C", None, "Unknown"]
    assert_frame_equal(result.sort_values("line").reset_index(drop=True), source)
    assert_frame_equal(reorder_df_out_by_output(pdf.iloc[:0], source), source)


def test_production_entrypoints_use_pdf_only():
    from mrp_system.transform.structured import build_structured_df
    assert list(inspect.signature(ordering.reorder_so_items).parameters) == ["df", "reference"]
    assert "item_order_model" not in inspect.signature(build_structured_df).parameters
    assert "reorder_df_out_by_output(pdf_ref, df_out)" in inspect.getsource(build_structured_df)
    etl_source = (Path(__file__).resolve().parents[1] / "mrp_system" / "cli" / "etl.py").read_text()
    assert "ItemOrderModel" not in etl_source
    assert "item_order_model" not in etl_source


def test_no_reference_keeps_source_order():
    source = frame(["C", "A", "B"])
    assert_frame_equal(ordering.reorder_so_items(source), source)


def test_reference_for_another_so_does_not_change_order():
    source = frame(["C", "A", "B"])
    reference = frame(["B", "A", "C"], order="SO-20261124")
    assert_frame_equal(ordering.reorder_so_items(source, reference), source)


def test_empty_source_is_preserved():
    source = frame([])
    assert_frame_equal(ordering.reorder_so_items(source), source)
