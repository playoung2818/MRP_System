import json
from pathlib import Path
import tempfile
import unittest

from mrp_system.quotation_cards import load_item_cards


class QuotationCardsTests(unittest.TestCase):
    def test_manual_edits_order_deduplication_and_limit(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "items.json"
            path.write_text(json.dumps({"items": {"A": ["a", "B", "b", "C", "D", "E", "F", "G"]}}))
            inventory = [{"item": "A", "on_hand": 0, "available": -2}]
            cards, error = load_item_cards("a", inventory, path)
            self.assertIsNone(error)
            self.assertEqual([r["item"] for r in cards], ["A", "B", "C", "D", "E", "F"])
            self.assertEqual(cards[0]["on_hand"], 0)
            self.assertEqual(cards[0]["available"], -2)
            self.assertIsNone(cards[1]["on_hand"])
            path.write_text(json.dumps({"items": {"A": [{"item": "Z", "frequency": 1}]}}))
            self.assertEqual(load_item_cards("A", inventory, path)[0][1]["item"], "Z")

    def test_invalid_missing_and_unknown_entries(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "items.json"
            cards, error = load_item_cards("A", [], path)
            self.assertEqual(len(cards), 1)
            self.assertIsNotNone(error)
            path.write_text("{broken")
            self.assertIsNotNone(load_item_cards("A", [], path)[1])
            path.write_text('{"items": {}}')
            self.assertEqual(load_item_cards("A", [], path), ([{
                "item": "A", "selected": True, "on_hand": None, "available": None,
            }], None))
            self.assertEqual(load_item_cards("", [], path), ([], None))
