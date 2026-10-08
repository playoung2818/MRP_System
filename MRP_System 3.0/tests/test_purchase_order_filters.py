from datetime import date
from unittest.mock import MagicMock, patch

from flask import Flask

from mrp_system import purchase_order_lookup as po


def test_source_names_are_exact_parameterized_choices_combined_with_dates():
    name = "Vendor's %_ Parts"
    where, params, sort_col, direction, page = po.build_query({
        "source_names": [name, "Other vendor", name],
        "source_name": "Parts", "deliv_date": ["2026-10-12"],
        "sort": "source_names", "direction": "desc", "page": "2",
    })
    assert name not in where
    assert 'btrim("Source Name") = :source_names_0' in where
    assert 'btrim("Source Name") = :source_names_1' in where
    assert "source_names_2" not in params
    assert params["source_names_0"] == name
    assert params["deliv_date_0"] == date(2026, 10, 12)
    assert '"Source Name" ILIKE :source_name' in where
    assert (sort_col, direction, page) == ("Source Name", "desc", 2)


def test_source_names_blank_none_and_clear():
    where, _, *_ = po.build_query({"source_names": ["__blank__"]})
    assert 'coalesce(btrim("Source Name"), \'\') = \'\'' in where
    where, _, *_ = po.build_query({"source_names": ["__none__"]})
    assert "FALSE" in where
    where, params, *_ = po.build_query({"source_names": []})
    assert where == ""
    assert params == {}


def test_source_dropdown_options_and_pagination_preserve_filters():
    app = Flask(__name__)
    app.register_blueprint(po.purchase_orders)
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    queries = []

    def execute(statement, params=None):
        sql = str(statement)
        queries.append((sql, params or {}))
        result = MagicMock()
        if "SELECT DISTINCT" in sql:
            values = ["Vendor A", None] if 'btrim("Source Name")' in sql.split(" AS value")[0] else [date(2026, 10, 12)]
            result.scalars.return_value.all.return_value = values
        elif "COUNT(*)" in sql:
            result.scalar_one.return_value = 101
        else:
            result.mappings.return_value = []
        return result

    conn.execute.side_effect = execute
    with patch.object(po, "get_engine", return_value=engine):
        response = app.test_client().get(
            "/purchase_orders?source_names=Vendor+A&source_names=Vendor+B&deliv_date=2026-10-12&sort=source_names"
        )
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'data-open-filter="source_names"' in html
    assert 'id="menu-source_names"' in html
    assert "Search source names" in html
    assert "Sort A to Z" in html
    assert "(Blanks)" in html
    assert 'value="Vendor B" checked' in html
    assert "source_names=Vendor+A" in html and "source_names=Vendor+B" in html
    source_query, source_params = next((sql, params) for sql, params in queries if sql.startswith('SELECT DISTINCT nullif(btrim("Source Name")'))
    assert "source_names_0" not in source_params
    assert source_params["deliv_date_0"] == date(2026, 10, 12)
    row_query, row_params = queries[-1]
    assert 'ORDER BY "Source Name" ASC' in row_query
    assert row_params["source_names_1"] == "Vendor B"
    engine.dispose.assert_called_once()
