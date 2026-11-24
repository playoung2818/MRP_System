"""Homepage item price-history queries against an in-memory stand-in for public.item_cost."""
from datetime import date
from pathlib import Path
import sys
import unittest

from sqlalchemy import create_engine, event, text

WEB_ROOT = Path(__file__).resolve().parents[2] / "Webpage"
sys.path.insert(0, str(WEB_ROOT))
import item_cost_history as history  # noqa: E402


def make_engine():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "connect")
    def attach(conn, _):
        conn.execute("ATTACH DATABASE ':memory:' AS public")

    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE public.item_cost (id INTEGER PRIMARY KEY, item TEXT, order_date DATE,
            pod_number TEXT, qty NUMERIC, amount NUMERIC, unit_cost NUMERIC, vendor TEXT,
            first_seen DATE, last_seen DATE)"""))
        rows = [
            ("PART-A", date(2026, 3, 1), "POD-2", 2, 100, 50, "Vendor"),
            ("PART-A", date(2026, 1, 1), "POD-1", 1, 40, 40, None),
            ("PART-A", date(2026, 2, 1), "POD-Z", 1, 0, 0, "Free sample"),
            ("PART-AB", date(2026, 1, 5), "POD-3", 1, 10, 10, "Vendor"),
            ("OTHER_PART", date(2026, 1, 5), "POD-4", 1, 10, 10, "Vendor"),
            # Revised price on one POD: 1,632 seen first, replaced by 1,568.
            ("PART-R", date(2025, 10, 10), "POD-9", 20, 32640, 1632, "Vendor"),
            ("PART-R", date(2025, 10, 10), "POD-9", 20, 31360, 1568, "Vendor"),
            # Two lines seen in the same snapshots: newest row wins the tie.
            ("PART-S", date(2025, 10, 23), "POD-8", 1, 5000, 5000, "Vendor"),
            ("PART-S", date(2025, 10, 23), "POD-8", 1, 2050, 2050, "Vendor"),
        ]
        seen = {6: (date(2025, 10, 10), date(2025, 10, 13)), 7: (date(2025, 10, 14), date(2025, 10, 16))}
        conn.execute(text("""INSERT INTO public.item_cost (item, order_date, pod_number, qty, amount, unit_cost,
            vendor, first_seen, last_seen) VALUES (:i,:d,:p,:q,:a,:u,:v,:f,:l)"""),
                     [dict(zip("idpqauv", r), f=seen.get(n, (date(2026, 1, 1),) * 2)[0],
                           l=seen.get(n, (date(2026, 1, 1),) * 2)[1]) for n, r in enumerate(rows, 1)])
    return engine


class ItemCostHistoryTests(unittest.TestCase):
    def setUp(self):
        self.engine = make_engine()

    def test_history_is_case_insensitive_sorted_and_skips_zero_amounts(self):
        result = history.item_history(self.engine, " part-a ")
        self.assertEqual(result["item"], "PART-A")
        self.assertEqual([p["pod"] for p in result["points"]], ["POD-1", "POD-2"])
        self.assertEqual(result["points"][1], {"date": "2026-03-01", "pod": "POD-2", "qty": 2.0,
                                               "unit_cost": 50.0, "vendor": "Vendor"})
        self.assertEqual(result["points"][0]["vendor"], "")

    def test_only_latest_price_version_per_pod(self):
        revised = history.item_history(self.engine, "PART-R")["points"]
        self.assertEqual([(p["pod"], p["unit_cost"]) for p in revised], [("POD-9", 1568.0)])
        same_window = history.item_history(self.engine, "PART-S")["points"]
        self.assertEqual([(p["pod"], p["unit_cost"]) for p in same_window], [("POD-8", 2050.0)])

    def test_normalized_fallback_and_missing_item(self):
        self.assertEqual(history.item_history(self.engine, "alias", lambda v: "PART-AB")["item"], "PART-AB")
        self.assertEqual(history.item_history(self.engine, "nothing"), {"item": None, "points": []})
        self.assertEqual(history.item_history(self.engine, "  "), {"item": None, "points": []})
        broken = history.item_history(self.engine, "nothing", lambda v: 1 / 0)
        self.assertIsNone(broken["item"])

    def test_suggestions_rank_prefix_and_treat_wildcards_literally(self):
        items = [row["item"] for row in history.suggest_items(self.engine, "part")]
        self.assertEqual(items[:2], ["PART-A", "PART-AB"])  # prefix matches, newest purchase first
        self.assertIn("OTHER_PART", items)
        self.assertEqual(history.suggest_items(self.engine, ""), [])
        self.assertEqual([r["item"] for r in history.suggest_items(self.engine, "R_P")], ["OTHER_PART"])
        self.assertEqual(history.suggest_items(self.engine, "%"), [])


if __name__ == "__main__":
    unittest.main()
