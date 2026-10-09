import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mrp_system.transform import item_order as ordering
from mrp_system.transform.structured import reorder_df_out_by_output


@pytest.fixture(autouse=True)
def isolated_normalizer(monkeypatch):
    aliases = {"a-alias": "A", "b-alias": "B"}
    monkeypatch.setattr(ordering, "normalize_item", lambda value: aliases.get(str(value).strip().casefold(), value))
    monkeypatch.setattr(ordering, "alias_fingerprint", lambda: "test-aliases")


def model_for(sequence=("a", "b", "c"), count=3):
    model = ordering.ItemOrderModel.train([(f"SO-{i}", list(sequence)) for i in range(count)])
    model.metadata["alias_fingerprint"] = "test-aliases"
    return model


def frame(items, order="SO-20261123"):
    return pd.DataFrame({"QB Num": [order] * len(items), "Item": items,
                         "Qty": list(range(1, len(items) + 1)), "line": list(range(len(items)))})


def test_training_uses_array_order_and_deduplicates_releases():
    records = [
        {"sales_order": "WO10-20261123", "items": [{"item": "A-alias", "id": 9}, {"item": "B", "id": 1}]},
        {"sales_order": "SO-20261123", "items": [{"item": "A"}, {"item": "B"}]},
        {"sales_order": "SO-20261124", "items": '[{"item": "A"}, {"item": "B-alias"}]'},
        {"sales_order": "SO-20261125", "items": "bad JSON"},
    ]
    sequences = ordering.training_sequences(records)
    assert sequences == [("SO-20261123", ["a", "b"]), ("SO-20261124", ["a", "b"])]
    model = ordering.ItemOrderModel.train(sequences)
    assert model.votes["a"]["b"] == 2


def test_one_so_cannot_dominate_votes_with_many_releases():
    model = ordering.ItemOrderModel.train([
        ("SO-1", ["a", "b"]), ("SO-1", ["a", "b", "c"]),
        ("SO-2", ["b", "a"]),
    ])
    assert model.votes["a"]["b"] == 1
    assert model.votes["b"]["a"] == 1


def test_conflicting_releases_from_same_so_do_not_vote():
    model = ordering.ItemOrderModel.train([("SO-1", ["a", "b"]), ("SO-1", ["b", "a"])])
    assert model.votes == {}
    assert model.predict(["b", "a"]) == ["b", "a"]


def test_pdf_reference_matches_aliases_and_repeated_occurrences():
    source = frame(["B", "A", "C", "A"])
    ref = pd.DataFrame({"QB Num": ["WO10-20261123"] * 3, "Item": ["A-alias", "B-alias", "A"]})
    result = reorder_df_out_by_output(ref, source)
    assert result["line"].tolist() == [1, 0, 3, 2]
    assert_frame_equal(result.sort_values("line").reset_index(drop=True), source)


def test_exact_wo_order_overrides_pdf_and_global_model():
    model = model_for()
    model.so_sequences["SO-20261123"] = [["c", "a", "b"]]
    source = frame(["A", "B", "C"])
    pdf = pd.DataFrame({"QB Num": ["SO-20261123"] * 3, "Item": ["B", "A", "C"]})
    result = ordering.reorder_so_items(source, pdf, model)
    assert result["Item"].tolist() == ["C", "A", "B"]


def test_best_matching_release_selected_newest_wins_ties():
    model = model_for()
    model.so_sequences["SO-20261123"] = [["b"], ["c", "a", "b"], ["a", "b", "c"]]
    assert model.reference("SO-20261123", ["a", "b", "c"]) == ["c", "a", "b"]


def test_model_orders_unseen_so_without_changing_data():
    model = model_for()
    source = frame(["C", "Unknown1", "B-alias", "A", "B", "Unknown2", None])
    result = ordering.reorder_so_items(source, model=model)
    assert result["line"].tolist() == [3, 2, 4, 0, 1, 5, 6]
    assert_frame_equal(result.sort_values("line").reset_index(drop=True), source)


def test_weak_or_conflicting_evidence_preserves_original_order():
    assert model_for(count=1).predict(["c", "b", "a"]) == ["c", "b", "a"]
    model = ordering.ItemOrderModel.train([(f"SO-{i}", ["a", "b"] if i % 2 else ["b", "a"]) for i in range(6)])
    assert model.predict(["b", "a"]) == ["b", "a"]


def test_cycle_resolution_is_deterministic_and_never_drops_items():
    model = ordering.ItemOrderModel(votes={"a": {"b": 5}, "b": {"c": 5}, "c": {"a": 5}})
    assert model.predict(["c", "b", "a"]) == model.predict(["c", "b", "a"])
    assert set(model.predict(["c", "b", "a"])) == {"a", "b", "c"}


def test_production_pdf_reorder_never_loads_or_uses_model(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("Production must not use the learned model")
    monkeypatch.setattr(ordering.ItemOrderModel, "load", forbidden)
    monkeypatch.setattr(ordering.ItemOrderModel, "reference", forbidden)
    monkeypatch.setattr(ordering.ItemOrderModel, "predict", forbidden)
    source = frame(["C", "A", "B"])
    pdf = pd.DataFrame({"QB Num": ["SO-20261123"] * 2, "Item": ["B", "A-alias"]})
    assert reorder_df_out_by_output(pdf, source)["Item"].tolist() == ["B", "A", "C"]
    assert_frame_equal(reorder_df_out_by_output(pdf.iloc[:0], source), source)


def test_production_entrypoints_have_no_model_configuration():
    import inspect
    from pathlib import Path
    from mrp_system.transform.structured import build_structured_df
    assert "item_order_model" not in inspect.signature(build_structured_df).parameters
    assert "reorder_df_out_by_output(pdf_ref, df_out)" in inspect.getsource(build_structured_df)
    etl_source = (Path(__file__).resolve().parents[1] / "mrp_system" / "cli" / "etl.py").read_text()
    assert "ItemOrderModel" not in etl_source
    assert "item_order_model" not in etl_source


def test_no_reference_or_model_keeps_source_order():
    source = frame(["C", "A", "B"])
    assert_frame_equal(ordering.reorder_so_items(source), source)


def test_snapshot_roundtrip_and_changed_aliases_rejected(tmp_path, monkeypatch):
    model = model_for()
    path = tmp_path / "model.json"
    model.save(path)
    loaded = ordering.ItemOrderModel.load(path)
    assert loaded.predict(["c", "b", "a"]) == ["a", "b", "c"]
    assert loaded.so_sequences == model.so_sequences
    monkeypatch.setattr(ordering, "alias_fingerprint", lambda: "changed")
    with pytest.raises(ValueError, match="aliases changed"):
        ordering.ItemOrderModel.load(path)


def test_invalid_snapshot_is_rejected(tmp_path):
    import json
    path = tmp_path / "model.json"
    model_for().save(path)
    payload = json.loads(path.read_text())
    payload["votes"] = {"a": {"b": "invalid"}}
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="votes"):
        ordering.ItemOrderModel.load(path)
