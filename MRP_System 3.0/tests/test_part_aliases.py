from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from mrp_system.normalize import part_aliases
from mrp_system.normalize.mrp_normalize import normalize_item


def mock_engine(rows):
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.mappings.return_value.all.return_value = rows
    return engine, conn


@pytest.fixture(autouse=True)
def isolated_alias_cache(monkeypatch):
    monkeypatch.setattr(part_aliases, "_aliases", None)


def test_many_aliases_share_canonical_with_case_insensitive_lookup():
    engine, conn = mock_engine([
        {"alias_part": " Vendor CPU ", "canonical_part": "CPU-CANONICAL"},
        {"alias_part": "QB CPU", "canonical_part": "CPU-CANONICAL"},
    ])
    assert part_aliases.refresh_part_number_aliases(engine) == 2
    assert normalize_item(" vendor cpu ") == "CPU-CANONICAL"
    assert normalize_item("QB CPU") == "CPU-CANONICAL"
    assert normalize_item("cpu-canonical") == "CPU-CANONICAL"
    assert normalize_item("Unknown") == "Unknown"
    assert normalize_item(None) is None
    assert pd.isna(normalize_item(float("nan")))
    assert conn.execute.call_count == 1
    sql = str(conn.execute.call_args.args[0])
    assert "alias_part, canonical_part" in sql
    assert "active" not in sql
    engine.dispose.assert_not_called()  # Caller owns this engine.


def test_lazy_load_queries_once_and_disposes_owned_engine():
    engine, conn = mock_engine([{"alias_part": "Alias", "canonical_part": "Correct"}])
    with patch("mrp_system.runtime.db_config.get_engine", return_value=engine) as get_engine:
        assert normalize_item("Alias") == "Correct"
        assert normalize_item("Alias") == "Correct"
        assert normalize_item("Unknown") == "Unknown"
    get_engine.assert_called_once()
    conn.execute.assert_called_once()
    engine.dispose.assert_called_once()


def test_database_alias_has_priority_over_regex_fallback():
    engine, _ = mock_engine([{"alias_part": "GC-AGXORIN64G-JP6.0", "canonical_part": "DB-CANONICAL"}])
    part_aliases.refresh_part_number_aliases(engine)
    assert normalize_item("GC-AGXORIN64G-JP6.0") == "DB-CANONICAL"


def test_refresh_updates_cached_values_without_modifying_source():
    first, _ = mock_engine([{"alias_part": "Alias", "canonical_part": "First"}])
    second, _ = mock_engine([{"alias_part": "Alias", "canonical_part": "Second"}])
    part_aliases.refresh_part_number_aliases(first)
    assert normalize_item("Alias") == "First"
    part_aliases.refresh_part_number_aliases(second)
    assert normalize_item("Alias") == "Second"


def test_conflicting_alias_is_rejected_and_previous_cache_preserved():
    good, _ = mock_engine([{"alias_part": "Alias", "canonical_part": "Correct"}])
    bad, _ = mock_engine([
        {"alias_part": "Alias", "canonical_part": "Correct"},
        {"alias_part": "ALIAS", "canonical_part": "Wrong"},
    ])
    part_aliases.refresh_part_number_aliases(good)
    with pytest.raises(ValueError, match="conflicting"):
        part_aliases.refresh_part_number_aliases(bad)
    assert normalize_item("Alias") == "Correct"


def test_database_failure_does_not_silently_fall_back():
    engine, conn = mock_engine([])
    conn.execute.side_effect = RuntimeError("unavailable")
    with patch("mrp_system.runtime.db_config.get_engine", return_value=engine):
        with pytest.raises(RuntimeError, match="unavailable"):
            normalize_item("Alias")
    assert part_aliases._aliases is None
    engine.dispose.assert_called_once()


def test_invalid_rows_and_canonical_alias_chains_rejected():
    with pytest.raises(ValueError, match="require"):
        part_aliases._build_alias_map([{"alias_part": None, "canonical_part": "Correct"}])
    with pytest.raises(ValueError, match="also an alias"):
        part_aliases._build_alias_map([
            {"alias_part": "A", "canonical_part": "B"},
            {"alias_part": "B", "canonical_part": "C"},
        ])
