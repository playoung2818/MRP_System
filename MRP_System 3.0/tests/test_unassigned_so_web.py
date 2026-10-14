"""Exercise the browser report without connecting to the database."""
import importlib.util
from pathlib import Path
import runpy
import unittest

import pandas as pd
from flask import Flask, render_template_string

WEB = Path(__file__).resolve().parents[2] / "Webpage"
spec = importlib.util.spec_from_file_location("unassigned_so", WEB / "unassigned_so.py")
report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(report)


class UnassignedSOTests(unittest.TestCase):
    def setUp(self):
        self.source = pd.DataFrame([
            {"QB Num": "SO-2", "Ship Date": "2099-12-31", "Name": "Acme", "Item": "Part B", "Component_Status": "Shortage", "Sales/Week": 5, "Available + On PO": 2},
            {"QB Num": "SO-1", "Ship Date": "2026-07-04", "Name": "Other", "Item": "Part A", "Sales/Week": 2, "Available + On PO": 2},
            {"QB Num": "SO-2", "Ship Date": "2099-12-31", "Item": "Part C"},
            {"QB Num": "SO-3", "Ship Date": "2026-05-01"},
            {"QB Num": None, "Ship Date": "2099-12-31"},
            {"QB Num": "SO-4", "Ship Date": None},
        ])

    def test_selection_bands_and_excel_highlights(self):
        original = self.source.copy(deep=True)
        count, rows = report.report_rows(self.source)
        self.assertEqual(count, 2)
        self.assertEqual([r["cells"]["Item"] for r in rows], ["Part A", "Part B", "Part C"])
        self.assertEqual([r["band"] for r in rows], [True, False, False])
        self.assertTrue(rows[1]["shortage"])
        self.assertTrue(rows[1]["low_cover"])
        self.assertFalse(rows[0]["low_cover"])
        self.assertEqual(rows[2]["cells"]["Sales/Week"], "")
        pd.testing.assert_frame_equal(original, self.source)

    def test_literal_case_insensitive_search_and_empty_input(self):
        count, rows = report.report_rows(self.source, "acme")
        self.assertEqual((count, len(rows)), (1, 1))
        self.assertEqual(report.report_rows(self.source, "[")[0], 0)
        self.assertEqual(report.report_rows(None), (0, []))
        self.assertEqual(report.report_rows(pd.DataFrame()), (0, []))

    def test_template_highlights_links_and_escaping(self):
        app = Flask(__name__)
        app.add_url_rule("/", endpoint="index", view_func=lambda: "")
        app.add_url_rule("/dashboard/unassigned_so", endpoint="dashboard_unassigned_so", view_func=lambda: "")
        self.source.loc[0, "Name"] = "<script>bad</script>"
        count, rows = report.report_rows(self.source)
        template = runpy.run_path(str(WEB / "ui.py"))["UNASSIGNED_SO_TPL"]
        with app.test_request_context():
            html = render_template_string(template, rows=rows, count=count, total_count=count,
                                          columns=report.COLUMNS, numeric_cols=report.NUMERIC_COLUMNS,
                                          query="", loaded_at="test")
        self.assertIn("&lt;script&gt;bad&lt;/script&gt;", html)
        self.assertIn("low-cover", html)
        self.assertIn("/?so=SO-2", html)


if __name__ == "__main__":
    unittest.main()
