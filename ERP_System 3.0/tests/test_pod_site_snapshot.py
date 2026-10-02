"""POD site refresh must write data, not modify Python source."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from erp_system.normalize import erp_normalize as normalization
from erp_system.ledger import events


class PodSiteSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "data" / "pod_site.json"
        original = normalization.POD_SITE.copy()
        self.addCleanup(self.restore_map, original)
        patcher = patch.object(normalization, "POD_SITE_PATH", self.path)
        patcher.start()
        self.addCleanup(patcher.stop)

    @staticmethod
    def restore_map(original):
        normalization.POD_SITE.clear()
        normalization.POD_SITE.update(original)

    def test_missing_snapshot_is_empty(self):
        self.assertEqual(normalization.load_pod_site(), {})

    def test_refresh_preserves_source_and_updates_imported_dictionary(self):
        source = Path(normalization.__file__)
        before = source.read_bytes()
        reference = events.POD_SITE
        raw = pd.DataFrame({
            "Num": [" POD-1 (memo)", "POD-2", "POD-3", "POD-3"],
            "Inventory Site": [" Drop Ship ", "WH01S-NTA", "WH01X-NTA", "Drop Ship"],
        })
        expected = {"POD-1": "Drop Ship", "POD-3": "Drop Ship"}
        self.assertEqual(normalization.refresh_pod_site(raw), expected)
        self.assertEqual(source.read_bytes(), before)
        self.assertIs(events.POD_SITE, reference)
        self.assertEqual(reference, expected)
        self.assertEqual(normalization.load_pod_site(), expected)
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_empty_refresh_clears_previous_snapshot(self):
        normalization.refresh_pod_site(pd.DataFrame({"POD#": ["POD-1"], "Inventory Site": ["Drop Ship"]}))
        normalization.refresh_pod_site(pd.DataFrame())
        self.assertEqual(normalization.load_pod_site(), {})
        self.assertEqual(events.POD_SITE, {})

    def test_failed_replace_preserves_snapshot_and_memory(self):
        normalization.refresh_pod_site(pd.DataFrame({"POD#": ["POD-1"], "Inventory Site": ["Drop Ship"]}))
        before = self.path.read_bytes()
        with patch.object(normalization.os, "replace", side_effect=OSError("write failed")):
            with self.assertRaises(OSError):
                normalization.refresh_pod_site(pd.DataFrame())
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(events.POD_SITE, {"POD-1": "Drop Ship"})
        self.assertEqual(list(self.path.parent.iterdir()), [self.path])

    def test_invalid_snapshot_is_not_silently_accepted(self):
        self.path.parent.mkdir(parents=True)
        for invalid in ([], {"POD-1": 42}):
            self.path.write_text(json.dumps(invalid), encoding="utf-8")
            with self.assertRaises(ValueError):
                normalization.load_pod_site()
        self.path.write_text("not JSON", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            normalization.load_pod_site()

    def test_new_exclusion_applies_to_current_ledger_build(self):
        normalization.refresh_pod_site(pd.DataFrame({"POD#": ["POD-1"], "Inventory Site": ["Drop Ship"]}))
        so = pd.DataFrame(columns=["Ship Date", "Item", "Qty(-)", "QB Num", "P. O. #", "Name"])
        sap = pd.DataFrame(columns=["Date", "Item", "Qty(+)", "QB Num", "P. O. #", "Name"])
        pod = pd.DataFrame({
            "Ship Date": ["2026-10-12", "2026-10-12"],
            "Item": ["CPU", "CPU"], "Qty(+)": [50, 10],
            "QB Num": ["POD-1", "POD-2"], "P. O. #": ["", ""], "Name": ["", ""],
        })
        result = events.build_events(so, sap, pod)
        self.assertEqual(result["QB Num"].tolist(), ["POD-2"])


if __name__ == "__main__":
    unittest.main()
