"""Exercise web ATP loading without starting the server or touching its database."""
import ast
from datetime import datetime
from pathlib import Path

import pandas as pd

from erp_system.ledger.atp import build_atp_view, earliest_atp_strict


def _functions(source=None):
    source = source or Path(__file__).resolve().parents[2] / "Webpage" / "server.py"
    tree = ast.parse(Path(source).read_text(encoding="utf-8-sig"))
    return [node for node in tree.body if isinstance(node, ast.FunctionDef)
            and node.name in {"_load_from_db", "_lookup_earliest_atp_date"}]


def check_cached_ledger_atp(source=None):
    future = pd.Timestamp.today().normalize() + pd.Timedelta(days=30)
    ledger = pd.DataFrame({"Item": ["PART"], "Date": [future], "Projected_NAV": [5.0]})
    reads, builds = [], []

    def read_table(schema, table):
        assert table != "item_atp"
        reads.append(table)
        return ledger.copy() if table == "ledger_analytics" else pd.DataFrame()

    def build(frame):
        builds.append(1)
        return build_atp_view(frame)

    ctx = dict(pd=pd, datetime=datetime, _read_table=read_table, build_atp_view=build,
               earliest_atp_strict=earliest_atp_strict,
               _safe_date_col=lambda *args: None,
               _build_final_sales_order_from_db=lambda: pd.DataFrame(),
               _build_runtime_indexes=lambda *args: ({}, {}, {}),
               _build_global_search_index=lambda *args: [],
               _build_quote_item_summaries=lambda *args: [])
    ctx.update({name: None for name in ["SO_INV", "SAP", "OPEN_PO", "FINAL_SO", "LEDGER", "ITEM_ATP"]})
    exec(compile(ast.Module(body=_functions(source), type_ignores=[]), "<web-atp-test>", "exec"), ctx)
    ctx["_load_from_db"]()
    assert ctx["_LAST_LOAD_ERR"] is None
    assert len(builds) == 1
    assert ctx["_lookup_earliest_atp_date"]("PART", 1) == future.to_pydatetime()
    assert ctx["_lookup_earliest_atp_date"]("PART", 6) is None
    assert ctx["_lookup_earliest_atp_date"]("MISSING", 1) is None
    ctx["_load_from_db"]()
    assert len(builds) == 1
    ctx["QUOTATION_VIEW_CACHE"] = {"old": {}}
    ctx["_load_from_db"](force=True)
    assert len(builds) == 2
    assert ctx["QUOTATION_VIEW_CACHE"] == {}
    ctx["ITEM_ATP"] = pd.DataFrame()
    assert ctx["_lookup_earliest_atp_date"]("PART", 1) is None
    assert len(builds) == 2


def test_cached_ledger_atp():
    check_cached_ledger_atp()
