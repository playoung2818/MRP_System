"""Planning tests: no live database, no source-system writes."""
from datetime import date, datetime, timezone
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pandas as pd
from flask import Flask

WEB = Path(__file__).resolve().parents[2] / "Webpage"
sys.path.insert(0, str(WEB))
from lt_materials import material_context, validate_blockers
from lt_planning import create_lt_planning


def frames(opening=2, inbound_date="2099-12-31", inbound_qty=20):
    so = pd.DataFrame([{"QB Num": "SO-1", "Item": "Part-A", "Qty(-)": 10,
                        "Ship Date": "2099-12-31", "Name": "Acme <script>"}])
    ledger = pd.DataFrame([
        {"Item": "PART-A", "Date": "2026-01-01", "Opening": opening, "Delta": 0, "Kind": "OPEN", "QB Num": ""},
        {"Item": "PART-A", "Date": inbound_date, "Opening": opening, "Delta": inbound_qty, "Kind": "IN", "QB Num": "PO-1"},
        {"Item": "PART-A", "Date": "2099-12-31", "Opening": opening, "Delta": -10, "Kind": "OUT", "QB Num": "SO-1"},
    ])
    pods = pd.DataFrame([{"Item": "PART-A", "Ship Date": inbound_date, "Qty(+)": inbound_qty}])
    return so, ledger, pods


class MaterialSuggestionTests(unittest.TestCase):
    def test_unknown_date_supply_not_usable_and_own_demand_removed(self):
        so, ledger, pods = frames()
        fp, rows = material_context(so, ledger, pods, "SO-1", "2026-05-01")
        self.assertEqual(rows[0]["uncovered_qty"], 8)
        self.assertEqual(rows[0]["required_qty"], 10)
        self.assertEqual(rows[0]["reason"], "POD ship date unknown")
        self.assertTrue(rows[0]["blocking"])
        self.assertEqual(fp, material_context(so.iloc[::-1], ledger.iloc[::-1], pods, "SO-1", "2026-05-01")[0])
        so.loc[0, "Qty(-)"] = 11
        self.assertNotEqual(fp, material_context(so, ledger, pods, "SO-1", "2026-05-01")[0])

    def test_confirmed_supply_and_competing_demand(self):
        so, ledger, pods = frames(inbound_date="2026-05-10", inbound_qty=10)
        _, rows = material_context(so, ledger, pods, "SO-1", "2026-05-01")
        self.assertEqual(rows[0]["ready_date"], "2026-05-10")
        self.assertFalse(rows[0]["blocking"])
        ledger.loc[len(ledger)] = {"Item": "PART-A", "Date": "2026-06-01", "Opening": 2, "Delta": -5, "Kind": "OUT", "QB Num": "SO-OTHER"}
        _, rows = material_context(so, ledger, pods, "SO-1", "2026-05-01")
        self.assertEqual(rows[0]["uncovered_qty"], 3)
        self.assertEqual(rows[0]["reason"], "Confirmed supply insufficient")

    def test_no_pod_and_repeated_component_totals(self):
        so, ledger, pods = frames(inbound_qty=0)
        so = pd.concat([so, so], ignore_index=True)
        _, rows = material_context(so, ledger, pods, "SO-1", "2026-05-01")
        self.assertEqual(rows[0]["required_qty"], 20)
        self.assertEqual(rows[0]["uncovered_qty"], 18)
        self.assertEqual(rows[0]["reason"], "No open POD")

    def test_manual_validation(self):
        row = {"item": "PART-A", "required_qty": 10, "uncovered_qty": 8, "reason": "No open POD"}
        self.assertEqual(validate_blockers([row], False, {"PART-A": 10}), [row])
        self.assertEqual(validate_blockers([], True, {"PART-A": 10}), [])
        for invalid in ([{**row, "uncovered_qty": float("nan")}], [row, row], [{**row, "required_qty": 11}], [{**row, "item": "UNKNOWN"}]):
            with self.assertRaises(ValueError):
                validate_blockers(invalid, False, {"PART-A": 10})
        with self.assertRaises(ValueError):
            validate_blockers([], False, {"PART-A": 10})
        with self.assertRaises(ValueError):
            validate_blockers([row], True, {"PART-A": 10})


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        clock = patch('lt_planning.date', wraps=date)
        mocked_date = clock.start()
        mocked_date.today.return_value = date(2026, 7, 1)
        self.addCleanup(clock.stop)
        self.mes_approved = True
        publisher = patch('lt_planning.publish_to_mes')
        self.publish = publisher.start()
        self.addCleanup(publisher.stop)
        content_reviewer = patch('lt_planning.review_content')
        self.content_review = content_reviewer.start()
        self.addCleanup(content_reviewer.stop)
        snapshot = {'sales_order': 'SO-1', 'customer': 'Acme', 'customer_po': 'PO1',
                    'ship_date': '2099-12-31', 'terms': '', 'items': [
                        {'id': 'item-a', 'item': 'PART-A', 'quantity': 10, 'configuration': '', 'inventory_site': ''}]}
        self.staged = {'sales_order': 'SO-1', 'latest_snapshot': snapshot, 'previous_snapshot': None,
                       'approved_snapshot': None, 'items': [], 'source_revision': 1, 'revision': 0}
        self.engine = MagicMock()
        self.conn = self.engine.begin.return_value.__enter__.return_value
        self.read_conn = self.engine.connect.return_value.__enter__.return_value
        self.review = None
        self.plan = None
        self.writes = []
        self.read_conn.execute.side_effect = self.execute
        self.conn.execute.side_effect = self.execute
        self.so, self.ledger, self.pods = frames()
        self.app = Flask(__name__)
        self.app.secret_key = "test-only"
        self.app.add_url_rule("/", endpoint="index", view_func=lambda: "")
        self.source = MagicMock(side_effect=lambda force: (self.so, self.ledger, self.pods, "test", None))
        self.app.register_blueprint(create_lt_planning(self.engine, self.source))
        self.client = self.app.test_client()
        with self.client.session_transaction() as s:
            s["lt_csrf"] = "token"

    def execute(self, statement, params=None):
        sql = str(statement)
        result = MagicMock()
        result.scalar_one_or_none.return_value = None
        result.mappings.return_value.first.return_value = None
        if 'SELECT * FROM public.approved_work_orders' in sql:
            result.mappings.return_value.first.return_value = self.staged
        if 'SELECT sales_order, latest_snapshot' in sql:
            result.mappings.return_value.all.return_value = [self.staged]
        if "FROM public.approved_work_orders" in sql:
            result.scalar_one_or_none.return_value = "SO-1" if self.mes_approved else None
        if "INSERT" in sql:
            self.writes.append((sql, params))
        if "SELECT * FROM public.mrp_review_history" in sql:
            result.mappings.return_value.first.return_value = self.review
            result.mappings.return_value.all.return_value = [self.review] if self.review else []
        if "SELECT * FROM public.lt_planning_history" in sql:
            result.mappings.return_value.first.return_value = self.plan
            result.mappings.return_value.all.return_value = [self.plan] if self.plan else []
        if "SELECT DISTINCT ON" in sql:
            row = self.review if "mrp_review_history" in sql else self.plan
            result.mappings.return_value.all.return_value = [row] if row else []
        return result

    def payload(self, **extra):
        fp, _ = material_context(self.so, self.ledger, self.pods, "SO-1")
        return {"submission_id": str(uuid4()), "so": "SO-1", "actor": "Planner", "fingerprint": fp,
                "approved": True, "no_blockers": True, "blockers": [], "review_id": "", **extra}

    def post(self, action="review", **extra):
        return self.client.post(f"/lt_planning/{action}", json=self.payload(**extra), headers={"X-CSRF-Token": "token"})

    def existing_review(self, blockers=None):
        fp, suggestions = material_context(self.so, self.ledger, self.pods, "SO-1")
        self.review = {"id": uuid4(), "qb_num": "SO-1", "source_fingerprint": fp, "blockers": blockers or [],
                       "reviewed_by": "Planner", "assessed_at": datetime.now(timezone.utc), "notes": "",
                       "run_id": "test", "no_blockers": not blockers, "system_suggestions": suggestions}

    def test_csrf_required_but_review_checkbox_not_required(self):
        response = self.client.post("/lt_planning/review", json=self.payload())
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.writes, [])
        self.assertEqual(self.post(approved=False).status_code, 200)
        self.assertEqual(len(self.writes), 1)

    def test_manual_review_saved_once_as_json(self):
        response = self.post(actor="")
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertEqual(len(self.writes), 1)
        self.assertEqual(self.writes[0][1]["actor"], "Manual review")
        self.assertEqual(self.writes[0][1]["blockers"], "[]")
        self.assertTrue(self.writes[0][1]["none"])
        self.content_review.assert_called_once()
        self.source.assert_called_with(True)

    def test_optional_pod_number_is_saved_and_displayed_with_blocker(self):
        blocker = {'item': 'PART-A', 'required_qty': 10, 'uncovered_qty': 8,
                   'reason': 'POD ship date unknown', 'pod_num': '  POD-123  '}
        response = self.post(no_blockers=False, blockers=[blocker])
        self.assertEqual(response.status_code, 200)
        import json
        saved = json.loads(self.writes[0][1]['blockers'])
        self.assertEqual(saved[0]['pod_num'], 'POD-123')
        self.existing_review(saved)
        self.review['source_fingerprint'] = 'old'
        html = self.client.get('/planning?status=needs_production_planning').get_data(as_text=True)
        self.assertIn('POD# POD-123', html)
        response = self.post('assign', review_id=str(self.review['id']), production_start='2026-07-10',
                             material_update_approved=True, blocker_override=True, notes='Approved exception')
        self.assertEqual(response.status_code, 200)
        refreshed = json.loads(self.writes[-3][1]['blockers'])
        self.assertEqual(refreshed[0]['pod_num'], 'POD-123')

    def test_invalid_pod_number_is_rejected(self):
        response = self.post(no_blockers=False, blockers=[{'item': 'PART-A', 'required_qty': 10,
            'uncovered_qty': 8, 'reason': 'No open POD', 'pod_num': 'X' * 201}])
        self.assertEqual(response.status_code, 400)
        self.assertIn('POD#', response.json['error'])
        self.assertEqual(self.writes, [])

    def test_drop_ship_snapshot_hides_entire_so_in_all_stages(self):
        self.staged['latest_snapshot']['items'].append({'id': 'drop', 'item': 'System',
            'quantity': 1, 'inventory_site': ' Drop Ship '})
        self.existing_review()
        self.plan = {'id': uuid4(), 'qb_num': 'SO-1', 'planned_production_start': date(2026, 7, 10),
                     'assigned_at': datetime.now(timezone.utc), 'notes': ''}
        for status in ('needs_material_review', 'needs_production_planning', 'planning_complete'):
            html = self.client.get('/planning', query_string={'status': status, 'so': 'SO-1'}).get_data(as_text=True)
            self.assertNotIn('<strong>SO-1</strong>', html)
            self.assertNotIn('id="review-form"', html)
            self.assertIn('No sales orders match this view.', html)
        # After the next CLI excludes it, the retained last source snapshot
        # still prevents historical planning rows from restoring this SO.
        self.staged['previous_snapshot'] = self.staged['latest_snapshot']
        self.staged['latest_snapshot'] = None
        self.staged['approved_snapshot'] = {'inventory_site': 'WH01S-NTA', 'items': []}
        html = self.client.get('/planning?status=planning_complete').get_data(as_text=True)
        self.assertNotIn('<strong>SO-1</strong>', html)

    def test_orders_sorted_by_ship_date_then_so_with_missing_dates_last(self):
        base = self.so.iloc[0].to_dict()
        dates = [('SO-A', '2099-12-31'), ('SO-Z', '2026-07-04'),
                 ('SO-B', '2026-07-04'), ('SO-C', '2026-12-31'), ('SO-1', None)]
        self.so = pd.DataFrame([{**base, 'QB Num': number, 'Ship Date': day} for number, day in dates])
        def execute(statement, params=None):
            result = self.execute(statement, params)
            if 'SELECT sales_order FROM public.approved_work_orders' in str(statement):
                result.scalars.return_value = self.so['QB Num'].tolist()
            return result
        self.read_conn.execute.side_effect = execute
        html = self.client.get('/planning').get_data(as_text=True)
        expected = ['SO-B', 'SO-Z', 'SO-C', 'SO-A', 'SO-1']
        positions = [html.index('<strong>' + number + '</strong>') for number in expected]
        self.assertEqual(positions, sorted(positions))

    def test_stale_data_and_concurrent_review_rejected(self):
        self.assertEqual(self.post(fingerprint="old").status_code, 400)
        self.existing_review()
        self.assertEqual(self.post().status_code, 400)
        self.assertEqual(self.writes, [])

    def test_assignment_requires_current_review_and_records_link(self):
        dates = {"production_start": "2026-07-10"}
        self.assertEqual(self.post("assign", **dates).status_code, 400)
        self.existing_review()
        response = self.post("assign", actor="", review_id=str(self.review["id"]), **dates)
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertEqual(self.writes[-1][1]["review"], str(self.review["id"]))
        self.assertEqual(self.writes[-1][1]["actor"], "Manual assignment")
        self.assertIn("lt_planning_history", self.writes[-1][0])
        self.assertIsNone(self.writes[-1][1]["ship"])
        self.assertIn("public.production_overrides", self.writes[-2][0])
        self.assertIn("mes_sent_at", self.writes[-1][0])

    def test_assign_button_accepts_blockers_without_checkboxes_or_notes(self):
        self.existing_review([{"item": "PART-A", "required_qty": 10, "uncovered_qty": 8, "reason": "No open POD"}])
        fields = {"review_id": str(self.review["id"]), "production_start": "2026-07-10", "ship_date": "2026-07-15"}
        self.assertEqual(self.post("assign", **fields, approved=False, notes='').status_code, 200)
        self.assertTrue(self.writes[-1][1]['override'])
        self.assertEqual(self.writes[-1][1]['notes'], '')
        self.assertIsNone(self.writes[-1][1]["ship"])  # Browser-supplied dates are ignored.
        for invalid in ('2026-06-30', '2026-07-11', ''):
            self.assertEqual(self.post('assign', **{**fields, 'production_start': invalid}).status_code, 400)
        html = self.client.get('/planning?status=needs_production_planning').get_data(as_text=True)
        self.assertNotIn('name="blocker_override"', html)
        self.assertNotIn('name="approved"', html)
        self.assertNotIn('placeholder="Notes" required', html)

    def test_ship_date_is_read_from_cli_source_not_browser(self):
        self.so.loc[0, "Ship Date"] = "2026-07-15"
        self.existing_review()
        fields = {"review_id": str(self.review["id"]), "production_start": "2026-07-10", "ship_date": "2027-01-01"}
        self.assertEqual(self.post("assign", **fields).status_code, 200)
        self.assertEqual(str(self.writes[-1][1]["ship"]), "2026-07-15")
        self.assertEqual(self.post("assign", **{**fields, "production_start": "2026-07-20"}).status_code, 400)

    def test_completed_plan_leaves_active_views_and_shows_handoff_history(self):
        self.existing_review()
        self.plan = {"id": uuid4(), "qb_num": "SO-1", "review_id": self.review["id"],
                     "planned_production_start": datetime(2026, 7, 10).date(), "assigned_ship_date": None,
                     "assigned_at": datetime.now(timezone.utc), "assigned_by": "Manual assignment",
                     "blocker_override": False, "notes": ""}
        response = self.client.get("/lt_planning")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Acme &lt;script&gt;", response.get_data(as_text=True))
        response = self.client.get("/lt_planning?so=SO-1&status=planning_complete")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Acme &lt;script&gt;", response.get_data(as_text=True))
        self.assertIn("MES handoff not recorded", response.get_data(as_text=True))
        self.assertIn('Approve source changes for MES', response.get_data(as_text=True))
        self.assertIn('source-review-form', response.get_data(as_text=True))
        self.assertIn("2099-12-31", response.get_data(as_text=True))
        self.assertNotIn("Not assigned in the source sales order", response.get_data(as_text=True))

    def test_completed_source_approval_preserves_plan_and_history(self):
        self.plan = {'id': uuid4(), 'planned_production_start': date(2026, 7, 10)}
        response = self.post('acknowledge_source', source_revision=1, revision=0)
        self.assertEqual(response.status_code, 200)
        self.publish.assert_called_once_with(self.conn, 'SO-1', None)
        self.content_review.assert_not_called()
        self.assertEqual(self.writes, [])
        self.source.assert_not_called()

    def test_source_approval_rejects_stale_versions_and_incomplete_plan(self):
        self.assertEqual(self.post('acknowledge_source', source_revision=1, revision=0).status_code, 400)
        self.plan = {'planned_production_start': date(2026, 7, 10)}
        self.assertEqual(self.post('acknowledge_source', source_revision=0, revision=0).status_code, 400)
        self.publish.assert_not_called()

    def test_source_approval_requires_csrf_and_propagates_allocation_errors(self):
        self.assertEqual(self.client.post('/lt_planning/acknowledge_source', json={}).status_code, 403)
        self.plan = {'planned_production_start': date(2026, 7, 10)}
        self.publish.side_effect = ValueError('SO quantities are below saved WO allocations.')
        response = self.post('acknowledge_source', source_revision=1, revision=0)
        self.assertEqual(response.status_code, 400)
        self.assertIn('saved WO allocations', response.json['error'])

    def test_separate_active_views_and_stale_review_routing(self):
        self.existing_review()
        material = self.client.get('/lt_planning?status=needs_material_review').get_data(as_text=True)
        planning = self.client.get('/lt_planning?status=needs_production_planning').get_data(as_text=True)
        self.assertNotIn('Acme &lt;script&gt;', material)
        self.assertIn('Acme &lt;script&gt;', planning)
        self.so.loc[0, 'Qty(-)'] = 12
        material = self.client.get('/lt_planning?status=needs_material_review').get_data(as_text=True)
        self.assertNotIn('Acme &lt;script&gt;', material)
        planning = self.client.get('/lt_planning?status=needs_production_planning').get_data(as_text=True)
        self.assertIn('Acme &lt;script&gt;', planning)
        self.assertIn('Material data changed', planning)

    def test_completion_publishes_without_prior_mes_approval(self):
        self.existing_review()
        self.mes_approved = False
        response = self.post("assign", review_id=str(self.review["id"]), production_start="2026-07-10")
        self.assertEqual(response.status_code, 200)
        self.publish.assert_called_once_with(self.conn, 'SO-1', '')

    def test_combined_review_failure_does_not_record_material_approval(self):
        self.content_review.side_effect = ValueError('SO version changed')
        response = self.post()
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.writes, [])

    def test_inline_assignment_can_approve_refreshed_material_status(self):
        self.existing_review()
        self.review['source_fingerprint'] = 'old'
        response = self.post('assign', review_id=str(self.review['id']), production_start='2026-07-10',
                             approved=False, notes='Confirmed shipping update')
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertIn('mrp_review_history', self.writes[0][0])
        self.assertEqual(self.writes[-1][1]['review'], self.writes[0][1]['id'])

    def test_failed_publication_does_not_write_schedule_or_history(self):
        self.existing_review()
        self.publish.side_effect = ValueError('Source is not staged')
        response = self.post("assign", review_id=str(self.review["id"]), production_start="2026-07-10")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.writes, [])

    def test_notes_carry_from_material_review_into_assignment_and_mes(self):
        self.existing_review()
        self.review['notes'] = 'Waiting on Part A\nVendor confirmed shipment'
        response = self.client.get('/planning?status=needs_production_planning')
        self.assertIn('Waiting on Part A\nVendor confirmed shipment</textarea>', response.get_data(as_text=True))
        self.assertNotIn('Review notes', response.get_data(as_text=True))
        response = self.post('assign', review_id=str(self.review['id']), production_start='2026-07-10')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.writes[-1][1]['notes'], self.review['notes'])
        self.publish.assert_called_once_with(self.conn, 'SO-1', self.review['notes'])

    def test_edited_assignment_notes_are_sent_to_mes(self):
        self.existing_review()
        self.review['notes'] = 'Original material note'
        response = self.post('assign', review_id=str(self.review['id']), production_start='2026-07-10', notes='Shipment confirmed')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.writes[-1][1]['notes'], 'Shipment confirmed')
        self.publish.assert_called_once_with(self.conn, 'SO-1', 'Shipment confirmed')
        self.assertEqual(self.review['notes'], 'Original material note')

    def test_completed_date_is_read_only_in_mrp(self):
        self.existing_review()
        self.plan = {'id': uuid4(), 'review_id': self.review['id'],
                     'planned_production_start': date(2026, 7, 10)}
        response = self.post('assign', review_id=str(self.review['id']), plan_id=str(self.plan['id']), production_start='2026-07-13')
        self.assertEqual(response.status_code, 400)
        self.assertIn('Adjust production dates in MES', response.json['error'])
        response = self.post(review_id=str(self.review['id']))
        self.assertEqual(response.status_code, 400)
        self.assertEqual(self.writes, [])

    def test_workspace_renders_escaped_data_and_history(self):
        response = self.client.get("/lt_planning?so=SO-1")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("Needs Material Review", html)
        self.assertNotIn('Blocking items · uncovered / required', html)
        self.assertIn("Acme &lt;script&gt;", html)
        self.assertIn("System-calculated material readiness", html)
        self.assertIn("Sales-order items", html)
        self.assertIn('<th>POD#</th>', html)
        self.assertNotIn('id="review-actor"', html)
        self.assertNotIn('id="review-approved"', html)
        self.assertNotIn('id="plan-actor"', html)
        self.assertNotIn('id="ship-date"', html)
        self.assertEqual(self.writes, [])
        self.existing_review()
        response = self.client.get("/lt_planning?so=SO-1&status=reviewed")
        self.assertEqual(response.status_code, 200, response.get_data(as_text=True))
        self.assertIn("No current material blockers", response.get_data(as_text=True))
        self.assertIn('class="inline-plan-form"', response.get_data(as_text=True))
        self.assertIn('Blocking items · uncovered / required', response.get_data(as_text=True))
        self.assertNotIn('Review / Plan Production', response.get_data(as_text=True))


if __name__ == "__main__":
    unittest.main()
