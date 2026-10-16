"""Combined SO/material review: no live database writes."""
from copy import deepcopy
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock

from flask import Flask

WEB = Path(__file__).resolve().parents[2] / 'Webpage'
sys.path.insert(0, str(WEB))
from so_review import create_so_review
from so_review.service import snapshot, differences, _normalize_review_rows
from so_review.combined import content_context, review_content


class SnapshotTests(unittest.TestCase):
    def test_reorder_and_quantity_edit_preserve_identity(self):
        source = {'sales_order': 'SO1', 'items': [{'item': 'A', 'quantity': 2}, {'item': 'B', 'quantity': 3}]}
        first = snapshot(source)
        source['items'] = [{'item': 'B', 'quantity': 4}, {'item': 'A', 'quantity': 2}]
        second = snapshot(source, first)
        self.assertEqual(second['items'][0]['id'], first['items'][1]['id'])
        self.assertTrue(any('quantity' in d for d in differences(first, second)))

    def test_invalid_manual_quantity_rejected(self):
        with self.assertRaises(ValueError):
            _normalize_review_rows([{'item': 'A', 'quantity': '1.5'}])


class CombinedReviewTests(unittest.TestCase):
    def setUp(self):
        snap = snapshot({'sales_order': 'SO1', 'customer': 'Customer', 'items': [
            {'item': 'A', 'quantity': 2}, {'item': 'B', 'quantity': 3}]})
        self.row = {'sales_order': 'SO1', 'latest_snapshot': snap, 'previous_snapshot': None,
                    'approved_snapshot': None, 'items': [], 'source_revision': 1, 'revision': 0,
                    'google_present': True}
        self.conn = MagicMock()
        self.result = MagicMock()
        self.result.mappings.return_value.first.return_value = self.row
        self.result.mappings.return_value.__iter__.return_value = iter([])
        self.conn.execute.return_value = self.result

    def payload(self):
        return {'source_revision': 1, 'revision': 0, 'items': deepcopy(self.row['latest_snapshot']['items'])}

    def test_combined_context_contains_source_diffs_and_item_order(self):
        context = content_context(self.row)
        self.assertEqual(context['items'][0]['item'], 'A')
        self.assertTrue(context['changes'])
        self.assertEqual(context['source_revision'], 1)

    def test_save_reorders_so_but_keeps_it_private_until_production(self):
        payload = self.payload()
        payload['items'].reverse()
        review_content(self.conn, 'SO1', payload)
        sql, params = self.conn.execute.call_args.args
        self.assertIn('is_active=FALSE', str(sql))
        self.assertLess(params['items'].index('"B"'), params['items'].index('"A"'))
        self.conn.commit.assert_not_called()  # Caller commits material + SO together.

    def test_drop_ship_so_cannot_be_reviewed(self):
        self.row['latest_snapshot']['items'][1]['inventory_site'] = ' Drop ship '
        self.assertIsNone(content_context(self.row))
        with self.assertRaisesRegex(ValueError, 'Drop Ship'):
            review_content(self.conn, 'SO1', self.payload())
        self.assertFalse(any(str(c.args[0]).lstrip().startswith('UPDATE') for c in self.conn.execute.call_args_list))

    def test_stale_so_revision_is_rejected(self):
        payload = self.payload()
        payload['source_revision'] = 0
        with self.assertRaisesRegex(ValueError, 'content changed'):
            review_content(self.conn, 'SO1', payload)
        self.assertFalse(any(str(c.args[0]).lstrip().startswith('UPDATE') for c in self.conn.execute.call_args_list))

    def test_source_line_tampering_is_rejected(self):
        payload = self.payload()
        payload['items'][0]['quantity'] = 99
        with self.assertRaisesRegex(ValueError, 'read-only'):
            review_content(self.conn, 'SO1', payload)

    def test_saved_wo_allocations_are_preserved(self):
        self.result.mappings.return_value.__iter__.return_value = iter([{'items': [{'item': 'A', 'quantity': 3}]}])
        with self.assertRaisesRegex(ValueError, 'saved WO allocations'):
            review_content(self.conn, 'SO1', self.payload())

    def test_old_review_pages_redirect_to_combined_planning(self):
        app = Flask(__name__)
        app.secret_key = 'test'
        app.add_url_rule('/planning', endpoint='lt_planning.workspace', view_func=lambda: '')
        app.register_blueprint(create_so_review(MagicMock()))
        client = app.test_client()
        response = client.get('/sales_orders/SO1')
        self.assertEqual(response.status_code, 302)
        self.assertIn('needs_material_review', response.location)
        self.assertIn('so=SO1', response.location)
        self.assertEqual(client.post('/sales_orders/SO1').status_code, 410)


if __name__ == '__main__':
    unittest.main()
