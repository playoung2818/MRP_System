"""Item price-history loader: file dating, report parsing, snapshot deduplication."""
from datetime import date
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import pandas as pd

from mrp_system.cli import load_item_cost as loader

HEADER = ["", "Date", "Num", "Name", "Source Name", "Memo", "Deliv Date", "Qty", "Rcv'd",
          "Backordered", "Amount", "Open Balance", "Inventory Site"]


def report(rows):
    return pd.DataFrame(rows, columns=HEADER)


def line(date_, pod, qty, amount, open_balance):
    return ["", date_, pod, "Vendor A", "Vendor A", "PART-1 description", "12/01/2026",
            qty, 0, qty, amount, open_balance, "WH01S-NTA"]


class LoadItemCostTests(unittest.TestCase):
    def test_snapshot_date_explicit_and_inferred_year(self):
        self.assertEqual(loader.snapshot_date("POD_11_23_2026.CSV", None), date(2026, 11, 23))
        self.assertEqual(loader.snapshot_date("POD_01_05.CSV", pd.Timestamp("2025-12-30")), date(2026, 1, 5))
        self.assertEqual(loader.snapshot_date("POD_12_31.CSV", pd.Timestamp("2025-12-30")), date(2025, 12, 31))
        self.assertIsNone(loader.snapshot_date("POD_01_05.CSV", None))
        self.assertIsNone(loader.snapshot_date("POD_Watchlist_2_5_2026.xlsx", None))

    def test_collect_parses_sections_dedupes_and_uses_full_line_amount(self):
        first = report([
            ["Inventory"] + [None] * 12,
            ["PART-1"] + [None] * 12,
            line("11/01/2026", "POD-1", 9, 1611, 1611),
            ["Total PART-1"] + [None] * 12,
        ])
        second = report([
            ["PART-1"] + [None] * 12,
            line("11/01/2026", "POD-1", 9, 1611, 716),  # partly received; same line
            ["Total PART-1"] + [None] * 12,
        ])
        with tempfile.TemporaryDirectory() as tmp, \
                mock.patch("mrp_system.transform.pod.normalize_item", new=lambda v: v):
            folder = Path(tmp)
            first.to_csv(folder / "POD_11_02_2026.CSV", index=False)
            second.to_csv(folder / "POD_11_09_2026.CSV", index=False)
            pd.DataFrame({"Reorder Pt (Min)": [1]}).to_csv(folder / "POD_12_05.CSV", index=False)
            (folder / "POD_Watchlist_2_5_2026.xlsx").write_bytes(b"ignored")
            result, skipped = loader.collect(folder)
        self.assertEqual(len(result), 1)
        row = result.iloc[0]
        self.assertEqual((row["pod_number"], row["item"]), ("POD-1", "PART-1"))
        self.assertEqual(row.unit_cost, 179)
        self.assertEqual((row.first_seen, row.last_seen, row.snapshot_count),
                         (date(2026, 11, 2), date(2026, 11, 9), 2))
        self.assertEqual([name for name, _ in skipped], ["POD_12_05.CSV"])


if __name__ == "__main__":
    unittest.main()
