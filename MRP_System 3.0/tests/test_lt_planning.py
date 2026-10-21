"""Current SO ownership, revision handoff and append-only review history (no live writes)."""
from datetime import date, datetime, timezone
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4
import pandas as pd
from flask import Flask
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'Webpage'))
from lt_materials import material_context, validate_blockers
from lt_planning import create_lt_planning, requirements_frame, order_age


def frames(opening=2, inbound_date='2099-12-31', inbound_qty=20):
    so = pd.DataFrame([{'QB Num':'SO-1','Item':'Part-A','Qty(-)':10,'Ship Date':'2099-12-31','Name':'Acme <script>'}])
    ledger = pd.DataFrame([
        {'Item':'PART-A','Date':'2026-01-01','Opening':opening,'Delta':0,'Kind':'OPEN','QB Num':''},
        {'Item':'PART-A','Date':inbound_date,'Opening':opening,'Delta':inbound_qty,'Kind':'IN','QB Num':'PO-1'},
        {'Item':'PART-A','Date':'2099-12-31','Opening':opening,'Delta':-10,'Kind':'OUT','QB Num':'SO-1'}])
    pods = pd.DataFrame([{'Item':'PART-A','Ship Date':inbound_date,'Qty(+)':inbound_qty}])
    return so, ledger, pods


class MaterialSuggestionTests(unittest.TestCase):
    def test_unknown_date_supply_and_own_demand(self):
        so, ledger, pods = frames()
        fingerprint, rows = material_context(so,ledger,pods,'SO-1','2026-05-01')
        self.assertEqual(rows[0]['uncovered_qty'],8)
        self.assertEqual(rows[0]['reason'],'POD ship date unknown')
        self.assertEqual(fingerprint,material_context(so.iloc[::-1],ledger.iloc[::-1],pods,'SO-1','2026-05-01')[0])
        so.loc[0,'Qty(-)']=11
        self.assertNotEqual(fingerprint,material_context(so,ledger,pods,'SO-1','2026-05-01')[0])

    def test_tariff_charge_is_not_a_material_requirement(self):
        so,ledger,pods=frames()
        so=pd.concat([so,pd.DataFrame([{'QB Num':'SO-1','Item':' Tariff Charge ','Qty(-)':1}])],ignore_index=True)
        _,suggestions=material_context(so,ledger,pods,'SO-1','2026-05-01')
        self.assertEqual([r['item'] for r in suggestions],['PART-A'])

    def test_ship_date_and_own_demand_rescheduling_do_not_invalidate_material_review(self):
        so,ledger,pods=frames()
        original=material_context(so,ledger,pods,'SO-1','2026-05-01')[0]
        so.loc[0,'Ship Date']='2030-10-01'
        ledger.loc[ledger['QB Num'].eq('SO-1'),'Date']='2030-10-01'
        self.assertEqual(original,material_context(so,ledger,pods,'SO-1','2026-05-01')[0])

    def test_confirmed_supply_and_competing_demand(self):
        so,ledger,pods=frames(inbound_date='2026-05-10',inbound_qty=10)
        self.assertFalse(material_context(so,ledger,pods,'SO-1','2026-05-01')[1][0]['blocking'])
        ledger.loc[len(ledger)]={'Item':'PART-A','Date':'2026-06-01','Opening':2,'Delta':-5,'Kind':'OUT','QB Num':'OTHER'}
        self.assertEqual(material_context(so,ledger,pods,'SO-1','2026-05-01')[1][0]['uncovered_qty'],3)

    def test_repeated_component_totals(self):
        so,ledger,pods=frames(inbound_qty=0)
        row=material_context(pd.concat([so,so]),ledger,pods,'SO-1','2026-05-01')[1][0]
        self.assertEqual(row['required_qty'],20)
        self.assertEqual(row['reason'],'No open POD')

    def test_manual_validation(self):
        row={'item':'PART-A','required_qty':10,'uncovered_qty':8,'reason':'No open POD'}
        self.assertEqual(validate_blockers([row],False,{'PART-A':10}),[row])
        self.assertEqual(validate_blockers([],True,{'PART-A':10}),[])
        for invalid in ([{**row,'uncovered_qty':float('nan')}],[row,row],[{**row,'required_qty':11}],[{**row,'item':'UNKNOWN'}]):
            with self.assertRaises(ValueError): validate_blockers(invalid,False,{'PART-A':10})
        with self.assertRaises(ValueError): validate_blockers([],False,{'PART-A':10})
        with self.assertRaises(ValueError): validate_blockers([row],True,{'PART-A':10})


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        clock=patch('lt_planning.date',wraps=date)
        clock.start().today.return_value=date(2026,7,1)
        self.addCleanup(clock.stop)
        publisher=patch('lt_planning.publish_to_mes')
        self.publish=publisher.start(); self.addCleanup(publisher.stop)
        reviewer=patch('lt_planning.review_content')
        self.content_review=reviewer.start(); self.addCleanup(reviewer.stop)
        snapshot={'sales_order':'SO-1','customer':'Acme <script>','customer_po':'PO1','ship_date':'2099-12-31','terms':'',
                  'items':[{'id':'item-a','item':'PART-A','quantity':10,'configuration':'','inventory_site':''}]}
        self.staged={'sales_order':'SO-1','customer':'Acme','latest_snapshot':snapshot,'previous_snapshot':None,
                     'approved_snapshot':None,'items':[],'source_revision':1,'revision':0,'metadata_fields':{},
                     'workflow_status':'needs_material_review','returned_from_mes':False,'google_present':True,
                     'review_incident_id':uuid4(),'material_review_id':None,'approved_order_signature':None}
        self.records=[self.staged]
        self.source_dates=[]
        self.review=None; self.writes=[]; self.duplicate=None
        self.engine=MagicMock()
        self.conn=self.engine.begin.return_value.__enter__.return_value
        self.read_conn=self.engine.connect.return_value.__enter__.return_value
        self.conn.execute.side_effect=self.execute; self.read_conn.execute.side_effect=self.execute
        self.so,self.ledger,self.pods=frames()
        self.source=MagicMock(side_effect=lambda force:(self.so,self.ledger,self.pods,'test',None))
        self.app=Flask(__name__); self.app.secret_key='test'
        self.app.add_url_rule('/',endpoint='index',view_func=lambda:'')
        self.app.register_blueprint(create_lt_planning(self.engine,self.source))
        self.client=self.app.test_client()
        with self.client.session_transaction() as session: session['lt_csrf']='token'

    def execute(self, statement, params=None):
        sql=str(statement); result=MagicMock()
        result.scalar_one_or_none.return_value=None
        result.mappings.return_value.first.return_value=None
        result.mappings.return_value.all.return_value=[]
        if 'MIN("Order Date") AS created_date' in sql:
            result.mappings.return_value.all.return_value=self.source_dates
        if 'SELECT qb_num FROM public.' in sql: result.scalar_one_or_none.return_value=self.duplicate
        if 'SELECT * FROM public.approved_work_orders WHERE sales_order' in sql:
            result.mappings.return_value.first.return_value=self.staged
        elif 'SELECT * FROM public.approved_work_orders' in sql:
            result.mappings.return_value.all.return_value=[r for r in self.records if r['workflow_status'] in ('needs_material_review','needs_production_planning')]
        if 'SELECT * FROM public.mrp_review_history' in sql:
            result.mappings.return_value.first.return_value=self.review
        if 'SELECT DISTINCT ON' in sql:
            result.mappings.return_value.all.return_value=[self.review] if self.review else []
        if sql.lstrip().startswith(('INSERT','UPDATE')): self.writes.append((sql,params))
        return result

    def existing_review(self, blockers=None):
        fingerprint,_=material_context(self.so,self.ledger,self.pods,'SO-1')
        self.review={'id':uuid4(),'qb_num':'SO-1','source_fingerprint':fingerprint,'blockers':blockers or [],
                     'assessed_at':datetime.now(timezone.utc),'notes':'','no_blockers':not blockers}
        self.staged.update(workflow_status='needs_production_planning',material_review_id=self.review['id'],
                           approved_order_signature={'part-a':10})

    def payload(self, **extra):
        fingerprint,_=material_context(self.so,self.ledger,self.pods,'SO-1')
        return {'submission_id':str(uuid4()),'so':'SO-1','fingerprint':fingerprint,
                'review_id':str(self.review['id']) if self.review else '', 'no_blockers':True,'blockers':[],
                'order_content':{'source_revision':1,'revision':0,'items':self.staged['latest_snapshot']['items']},**extra}

    def post(self, action='review', **extra):
        return self.client.post('/lt_planning/'+action,json=self.payload(**extra),headers={'X-CSRF-Token':'token'})

    def history(self, table):
        return [(sql,args) for sql,args in self.writes if 'INSERT INTO public.'+table in sql]

    def test_so_created_date_age_thresholds(self):
        today=date(2026,7,1)
        for created,expected in [('2026-06-24',''),('2026-06-23','so-age-warning'),
                                 ('2026-06-17','so-age-warning'),('2026-06-16','so-age-danger')]:
            with self.subTest(created=created):
                self.assertEqual(order_age(created,today)[2],expected)
        for missing in (None,'','invalid','2099-12-31'):
            self.assertEqual(order_age(missing,today),('',None,''))

    def test_planning_rows_display_source_creation_date_and_age_color(self):
        self.existing_review()
        for created,expected in [('2026-06-24',''),('2026-06-23','so-age-warning'),
                                 ('2026-06-17','so-age-warning'),('2026-06-16','so-age-danger')]:
            with self.subTest(created=created):
                self.source_dates=[{'sales_order':'SO-1','created_date':created}]
                html=self.client.get('/planning?status=needs_production_planning').text
                self.assertIn('<th>SO Created Date</th>',html)
                self.assertIn(created,html)
                if expected:
                    self.assertIn('class=" '+expected+'"',html)
                else:
                    self.assertNotIn('so-age-warning',html)
                    self.assertNotIn('so-age-danger',html)
        self.source_dates=[]
        html=self.client.get('/planning?status=needs_production_planning').text
        self.assertNotIn('so-age-danger',html)
        self.assertIn('—',html)

    def test_material_requirements_only_include_work_not_already_covered(self):
        self.so.loc[0,'Qty(-)']=12
        self.staged['latest_snapshot']['items'][0].update(quantity=12,remaining_quantity=12,shipped_quantity=0)
        allocated=[{'items':[{'item':'PART-A','quantity':10}],'is_shipped':False}]
        self.assertEqual(requirements_frame(self.so,self.staged,allocated).iloc[0]['Qty(-)'],2)
        self.staged['approved_order_signature']={'part-a':10}
        self.assertEqual(requirements_frame(self.so,self.staged,(),True).iloc[0]['Qty(-)'],2)

    def test_shipped_wo_does_not_cover_new_outstanding_work(self):
        self.so.loc[0,'Qty(-)']=2
        self.staged['latest_snapshot']['items'][0].update(quantity=8,remaining_quantity=2,shipped_quantity=6)
        allocated=[{'items':[{'item':'PART-A','quantity':6}],'is_shipped':True},
                   {'items':[{'item':'PART-A','quantity':1}],'is_shipped':False}]
        self.assertEqual(requirements_frame(self.so,self.staged,allocated).iloc[0]['Qty(-)'],1)

    def test_list_skips_hidden_material_calculations_but_open_review_calculates(self):
        with patch('lt_planning.material_context', wraps=material_context) as calculate:
            self.assertEqual(self.client.get('/planning?status=needs_material_review').status_code,200)
            calculate.assert_not_called()
            self.assertEqual(self.client.get('/planning?so=SO-1').status_code,200)
            self.assertEqual(calculate.call_count,1)
        self.existing_review()
        with patch('lt_planning.material_context', wraps=material_context) as calculate:
            self.client.get('/planning?status=needs_material_review')
            calculate.assert_not_called()
            self.assertEqual(self.client.get('/planning?status=needs_production_planning').status_code,200)
            calculate.assert_not_called()

    def test_search_shows_saved_records_without_calculating(self):
        with patch('lt_planning.material_context', wraps=material_context) as calculate:
            self.client.get('/planning?q=does-not-match')
            calculate.assert_not_called()
            self.assertEqual(self.client.get('/planning?q=SO-1').status_code,200)
            calculate.assert_not_called()

    def test_only_two_active_stages_and_no_history(self):
        html=self.client.get('/planning').text
        self.assertIn('Needs Material Review',html)
        self.assertIn('Needs Production Planning',html)
        self.assertNotIn('Planning Complete',html)
        self.assertNotIn('Blocking items · uncovered / required',html)
        html=self.client.get('/planning?so=SO-1').text
        self.assertIn('Sales-order items',html)
        self.assertIn('<th>POD#</th>',html)
        self.assertNotIn('id="review-approved"',html)
        self.assertNotIn('Review history',html)

    def test_released_so_absent_even_when_searched(self):
        self.staged['workflow_status']='released_to_mes'
        html=self.client.get('/planning?q=SO-1&so=SO-1').text
        self.assertNotIn('<strong>SO-1</strong>',html)
        self.assertNotIn('id="review-form"',html)

    def test_old_complete_bookmark_redirects(self):
        response=self.client.get('/planning?status=planning_complete')
        self.assertEqual(response.status_code,302)
        self.assertIn('needs_material_review',response.location)
        self.assertEqual(self.client.post('/lt_planning/acknowledge_source').status_code,410)

    def test_returned_badge_is_not_an_additional_stage(self):
        self.staged['returned_from_mes']=True
        html=self.client.get('/planning?so=SO-1').text
        self.assertIn('Returned from MES',html)
        self.assertIn('Approve Revision &amp; Return to MES',html)
        self.assertNotIn('status=returned_from_mes',html)

    def test_csrf_required_and_checkbox_not_required(self):
        self.assertEqual(self.client.post('/lt_planning/review',json=self.payload()).status_code,403)
        self.assertEqual(self.writes,[])
        self.assertEqual(self.post(approved=False).status_code,200)
        args=self.history('mrp_review_history')[0][1]
        self.assertEqual(args['kind'],'initial')
        self.assertEqual(args['incident'],str(self.staged['review_incident_id']))
        self.assertEqual(args['version'],1)
        self.assertFalse(self.publish.called)
        self.assertTrue(any("workflow_status='needs_production_planning'" in sql for sql,_ in self.writes))

    def test_returned_review_publishes_without_production_date_or_new_plan(self):
        self.staged['returned_from_mes']=True
        response=self.post(notes='Replacement approved')
        self.assertEqual(response.status_code,200,response.text)
        self.assertTrue(response.json['handed_off'])
        self.publish.assert_called_once_with(self.conn,'SO-1','Replacement approved',returned=True)
        self.assertEqual(self.history('mrp_review_history')[0][1]['kind'],'returned_from_mes')
        self.assertEqual(self.history('lt_planning_history'),[])
        self.assertFalse(any('production_overrides' in sql for sql,_ in self.writes))

    def test_assignment_records_link_and_notes_then_leaves_mrp(self):
        self.existing_review(); self.review['notes']='Vendor confirmed\nPOD-123'
        html=self.client.get('/planning?status=needs_production_planning').text
        self.assertIn('Vendor confirmed\nPOD-123</textarea>',html)
        response=self.post('assign',production_start='2026-07-10')
        self.assertEqual(response.status_code,200,response.text)
        self.publish.assert_called_once_with(self.conn,'SO-1',self.review['notes'])
        args=self.history('lt_planning_history')[0][1]
        self.assertEqual(args['review'],str(self.review['id']))
        self.assertEqual(args['notes'],self.review['notes'])
        self.assertTrue(response.json['handed_off'])

    def test_blockers_need_no_checkbox_or_notes_but_are_audited(self):
        self.existing_review([{'item':'PART-A','required_qty':10,'uncovered_qty':8,'reason':'No open POD'}])
        html=self.client.get('/planning?status=needs_production_planning').text
        self.assertNotIn('name="approved"',html); self.assertNotIn('name="blocker_override"',html)
        self.assertEqual(self.post('assign',production_start='2026-07-10',approved=False,notes='').status_code,200)
        self.assertTrue(self.history('lt_planning_history')[0][1]['override'])

    def test_invalid_dates_and_browser_ship_date_are_not_accepted(self):
        self.so.loc[0,'Ship Date']='2026-07-15'; self.existing_review()
        for start in ('2026-06-30','2026-07-11','2026-07-20',''):
            self.assertEqual(self.post('assign',production_start=start,ship_date='2099-12-31').status_code,400)
        self.assertEqual(self.post('assign',production_start='2026-07-10',ship_date='2099-12-31').status_code,200)
        self.assertEqual(str(self.history('lt_planning_history')[0][1]['ship']),'2026-07-15')

    def test_stale_form_and_wrong_owner_are_rejected(self):
        self.assertEqual(self.post(fingerprint='old').status_code,400)
        self.staged['workflow_status']='released_to_mes'
        self.assertEqual(self.post().status_code,400)
        self.assertEqual(self.post('assign',production_start='2026-07-10').status_code,400)
        self.assertEqual(self.writes,[])

    def test_item_change_during_planning_cannot_bypass_review(self):
        self.existing_review(); self.staged['approved_order_signature']={'part-a':9}
        self.assertEqual(self.post('assign',production_start='2026-07-10').status_code,400)
        self.publish.assert_not_called()

    def test_assignment_uses_saved_review_without_recalculating_or_appending_assessment(self):
        self.existing_review([{'item':'PART-A','required_qty':10,'uncovered_qty':8,'reason':'No open POD','pod_num':'POD-123'}])
        self.review['source_fingerprint']='old'
        payload=self.payload(production_start='2026-07-10',notes='Shipment checked')
        with patch('lt_planning.material_context', side_effect=AssertionError('No assignment recalculation')):
            response=self.client.post('/lt_planning/assign',json=payload,headers={'X-CSRF-Token':'token'})
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(self.history('mrp_review_history'),[])
        self.assertEqual(self.history('lt_planning_history')[0][1]['review'],str(self.review['id']))
        self.assertTrue(self.history('lt_planning_history')[0][1]['override'])

    def test_list_displays_exact_saved_blockers_even_when_calculation_would_differ(self):
        self.existing_review([{'item':'SAVED-ONLY','required_qty':1,'uncovered_qty':1,'reason':'No open POD','pod_num':'POD-SAVED'}])
        self.review['source_fingerprint']='old'
        with patch('lt_planning.material_context', side_effect=AssertionError('No list recalculation')):
            html=self.client.get('/planning?status=needs_production_planning').text
        self.assertIn('SAVED-ONLY',html)
        self.assertIn('POD-SAVED',html)
        self.assertNotIn('Current calculated material status',html)
        self.assertEqual(self.writes,[])

    def test_blocker_saves_without_reason_input(self):
        html=self.client.get('/planning?so=SO-1').text
        self.assertNotIn('<th>Reason</th>',html)
        blocker={'item':'PART-A','required_qty':10,'uncovered_qty':8,'pod_num':'POD-123'}
        response=self.post(no_blockers=False,blockers=[blocker])
        self.assertEqual(response.status_code,200,response.text)
        saved=json.loads(self.history('mrp_review_history')[0][1]['blockers'])[0]
        self.assertEqual(saved['reason'],'')
        self.assertEqual(saved['item'],'PART-A')
        self.assertEqual(saved['pod_num'],'POD-123')

    def test_pod_saved_and_invalid_length_rejected(self):
        blocker={'item':'PART-A','required_qty':10,'uncovered_qty':8,'reason':'No open POD','pod_num':' POD-123 '}
        self.assertEqual(self.post(no_blockers=False,blockers=[blocker]).status_code,200)
        self.assertEqual(json.loads(self.history('mrp_review_history')[0][1]['blockers'])[0]['pod_num'],'POD-123')
        self.assertEqual(self.post(no_blockers=False,blockers=[{**blocker,'pod_num':'X'*201}]).status_code,400)

    def test_source_missing_and_drop_ship_cannot_release(self):
        self.staged['google_present']=False
        self.assertNotIn('<strong>SO-1</strong>',self.client.get('/planning').text)
        self.assertNotIn('<strong>SO-1</strong>',self.client.get('/planning?q=SO-1&so=SO-1').text)
        self.assertEqual(self.post().status_code,400)
        self.staged['google_present']=True
        self.staged['latest_snapshot']['inventory_site']=' Drop Ship '
        self.assertNotIn('<strong>SO-1</strong>',self.client.get('/planning').text)
        self.assertEqual(self.post().status_code,400)

    def test_missing_source_material_review_not_counted_or_shown(self):
        self.staged.update(google_present=False,previous_snapshot=self.staged['latest_snapshot'],latest_snapshot=None)
        with patch('lt_planning.material_context', side_effect=AssertionError('Missing order must not be assessed')):
            html=self.client.get('/planning?q=SO-1&so=SO-1').text
        self.assertNotIn('<strong>SO-1</strong>',html)
        self.assertNotIn('id="review-form"',html)
        self.assertIn('<strong>0</strong><span>Needs Material Review</span>',html)
        self.assertEqual(self.writes,[])

    def test_duplicate_is_idempotent(self):
        self.duplicate='SO-1'
        self.assertEqual(self.post().status_code,200)
        self.assertEqual(self.writes,[]); self.publish.assert_not_called()

    def test_failed_publication_does_not_write_schedule(self):
        self.existing_review(); self.publish.side_effect=ValueError('Saved allocation conflict')
        self.assertEqual(self.post('assign',production_start='2026-07-10').status_code,400)
        self.assertFalse(any('production_overrides' in sql for sql,_ in self.writes))

    def test_sort_ship_date_then_so_missing_last(self):
        self.records=[]; base=self.so.iloc[0].to_dict()
        for number,day in [('SO-A','2099-12-31'),('SO-Z','2026-07-04'),('SO-B','2026-07-04'),('SO-0',None)]:
            record=dict(self.staged,sales_order=number,latest_snapshot=dict(self.staged['latest_snapshot'],ship_date=day or ''))
            self.records.append(record)
        self.so=pd.DataFrame([{**base,'QB Num':r['sales_order'],'Ship Date':r['latest_snapshot']['ship_date']} for r in self.records])
        html=self.client.get('/planning').text
        positions=[html.index('<strong>'+number+'</strong>') for number in ['SO-B','SO-Z','SO-A','SO-0']]
        self.assertEqual(positions,sorted(positions))


if __name__=='__main__': unittest.main()
