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

    def test_education_discount_is_not_a_material_requirement(self):
        so,ledger,pods=frames()
        so=pd.concat([so,pd.DataFrame([{'QB Num':'SO-1','Item':' EdUcAtIoN Discount ','Qty(-)':1}])],ignore_index=True)
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
                     'workflow_status':'needs_material_review','returned_from_mes':False,'source_present':True,
                     'review_incident_id':uuid4(),'material_review_id':None,'approved_order_signature':None}
        self.records=[self.staged]
        self.source_dates=[]
        self.inventory_names=[]
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
        if 'SELECT "Part_Number" FROM public.inventory_status' in sql:
            result.mappings.return_value.all.return_value=[{'Part_Number': name} for name in self.inventory_names]
        if 'MIN("Order Date") AS created_date' in sql:
            result.mappings.return_value.all.return_value=self.source_dates
        if 'SELECT qb_num FROM public.' in sql: result.scalar_one_or_none.return_value=self.duplicate
        if 'SELECT * FROM public.sales_order_workflow WHERE sales_order' in sql:
            result.mappings.return_value.first.return_value=self.staged
        elif 'SELECT * FROM public.sales_order_workflow' in sql:
            result.mappings.return_value.all.return_value=[r for r in self.records if r['workflow_status'] in ('needs_material_review','needs_production_planning')]
        if 'SELECT * FROM public.mrp_review_history' in sql:
            result.mappings.return_value.first.return_value=self.review
        if 'SELECT DISTINCT ON' in sql:
            result.mappings.return_value.all.return_value=[self.review] if self.review else []
        if sql.lstrip().startswith(('INSERT','UPDATE')): self.writes.append((sql,params))
        return result

    def save_notes(self, **extra):
        return self.post('notes', source_revision=1, revision=0, notes_version='', notes='Follow up', **extra)

    def apply_note_write(self):
        metadata = next(params['metadata'] for sql, params in reversed(self.writes) if 'SET metadata_fields=' in sql)
        self.staged['metadata_fields'] = json.loads(metadata)

    def test_notes_only_does_not_assign_publish_or_recalculate(self):
        self.existing_review()
        response = self.save_notes()
        self.assertEqual(response.status_code,200,response.json)
        self.assertFalse(response.json['handed_off'])
        self.assertEqual(response.json['next_status'],'needs_production_planning')
        self.assertEqual(len(self.writes),1)
        self.assertIn('SET metadata_fields=',self.writes[0][0])
        self.assertEqual(self.staged['workflow_status'],'needs_production_planning')
        self.source.assert_not_called()
        self.publish.assert_not_called()
        self.content_review.assert_not_called()
        self.assertEqual(self.history('mrp_review_history'),[])
        self.assertEqual(self.history('lt_planning_history'),[])
        self.apply_note_write()
        self.assertEqual(self.staged['metadata_fields']['planning_note']['notes'],'Follow up')
        self.assertEqual(len(self.staged['metadata_fields']['planning_note_history']),1)
        self.assertEqual(self.review['notes'],'')
        self.assertIn('>Follow up</textarea>',self.client.get('/planning?status=needs_production_planning').text)

    def test_note_edits_append_and_retries_do_not_append_twice(self):
        self.existing_review()
        first = self.save_notes().json
        self.apply_note_write()
        payload = self.payload(source_revision=1,revision=0,notes_version=first['notes_version'],notes='Second note')
        headers={'X-CSRF-Token':'token'}
        second=self.client.post('/lt_planning/notes',json=payload,headers=headers)
        self.assertEqual(second.status_code,200,second.json)
        self.apply_note_write()
        self.assertEqual([event['notes'] for event in self.staged['metadata_fields']['planning_note_history']],['Follow up','Second note'])
        writes=len(self.writes)
        retry=self.client.post('/lt_planning/notes',json=payload,headers=headers)
        self.assertEqual(retry.status_code,200,retry.json)
        self.assertEqual(len(self.writes),writes)
        self.assertEqual(retry.json['notes_version'],second.json['notes_version'])
        payload['notes']='Different payload with reused UUID'
        self.assertEqual(self.client.post('/lt_planning/notes',json=payload,headers=headers).status_code,400)

    def test_notes_reject_stale_tabs_and_stale_assignment(self):
        self.existing_review()
        first=self.save_notes().json
        self.apply_note_write()
        writes=len(self.writes)
        self.assertEqual(self.save_notes().status_code,400)
        self.assertEqual(self.post('assign',production_start='2026-07-06',notes='stale').status_code,400)
        self.assertEqual(len(self.writes),writes)
        self.publish.assert_not_called()
        response=self.post('assign',production_start='2026-07-06',notes_version=first['notes_version'])
        self.assertEqual(response.status_code,200,response.json)
        self.assertEqual(self.publish.call_args.args[2],'Follow up')

    def test_notes_reject_owner_source_review_and_version_changes(self):
        self.existing_review()
        original=dict(self.staged)
        for update in ({'workflow_status':'released_to_mes'}, {'source_present':False},
                       {'material_review_id':uuid4()}, {'source_revision':2}, {'revision':2},
                       {'approved_order_signature':{'part-a':99}}):
            self.staged.update(original)
            self.staged.update(update)
            response=self.save_notes()
            self.assertEqual(response.status_code,400,(update,response.json))
            self.assertEqual(self.writes,[])
        self.publish.assert_not_called()

    def test_notes_csrf_limits_empty_clear_and_date_guard(self):
        self.existing_review()
        self.assertEqual(self.client.post('/lt_planning/notes',json=self.payload(notes='test')).status_code,403)
        self.assertEqual(self.post('notes',notes='x'*4001).status_code,400)
        self.assertEqual(self.save_notes(production_start='2026-07-06').status_code,400)
        response=self.post('notes',source_revision=1,revision=0,notes_version='',notes='')
        self.assertEqual(response.status_code,200,response.json)
        self.assertEqual(response.json['notes'],'')

    def test_assignment_retains_original_creation_date_in_same_transaction(self):
        self.existing_review()
        self.so['Order Date']='2020-10-01'
        self.staged['metadata_fields']={'notes':'Preserve unrelated metadata'}
        response=self.post('assign',production_start='2026-07-06')
        self.assertEqual(response.status_code,200,response.json)
        capture=next((sql,params) for sql,params in self.writes if 'jsonb_build_object(\'planning_kpi\'' in sql)
        self.assertEqual(json.loads(capture[1]['kpi'])['created_date'],'2020-10-01')
        self.assertIn('COALESCE(metadata_fields::jsonb',capture[0])
        self.assertEqual(self.staged['metadata_fields']['notes'],'Preserve unrelated metadata')
        self.assertEqual(len(self.history('lt_planning_history')),1)

    def test_canonical_blocker_display_preserves_saved_history(self):
        self.existing_review([{'item':'I7-9700E','required_qty':10,'uncovered_qty':8,'reason':'No open POD'}])
        self.inventory_names=['i7-9700E']
        with patch('lt_planning.material_context',wraps=material_context) as calculate:
            html=self.client.get('/planning?status=needs_production_planning').text
            calculate.assert_not_called()
        self.assertIn('<strong>i7-9700E</strong>',html)
        self.assertNotIn('<strong>I7-9700E</strong>',html)
        self.assertEqual(self.review['blockers'][0]['item'],'I7-9700E')
        self.assertEqual(self.writes,[])
        self.client.get('/planning?status=needs_production_planning')
        queries=[str(call.args[0]) for call in self.read_conn.execute.call_args_list]
        self.assertEqual(sum('SELECT "Part_Number" FROM public.inventory_status' in sql for sql in queries),1)
        self.inventory_names=['i7-9700e']
        html=self.client.get('/planning?status=needs_production_planning&reload=1').text
        self.assertIn('<strong>i7-9700e</strong>',html)
        self.assertEqual(self.review['blockers'][0]['item'],'I7-9700E')

    def test_canonical_display_aliases_and_unknown_names(self):
        self.inventory_names=['Part-Canonical']
        display=self.app.jinja_env.filters['canonical_item']
        with patch('mrp_system.normalize.mrp_normalize.normalize_item',side_effect=lambda value:'PART-CANONICAL' if value=='old-alias' else value):
            self.assertEqual(display('old-alias'),'Part-Canonical')
            self.assertEqual(display('UNKNOWN-Manually-entered'),'UNKNOWN-Manually-entered')

    def test_loaded_timestamp_displays_whole_seconds(self):
        from datetime import datetime
        self.source.side_effect=lambda force:(self.so,self.ledger,self.pods,datetime(2026,10,29,14,45,13,395892),None)
        html=self.client.get('/planning').text
        self.assertIn('Loaded 2026-10-29 14:45:13.',html)
        self.assertNotIn('395892',html)

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

    def test_production_table_has_full_page_toggle_only_on_production_stage(self):
        html=self.client.get('/planning?status=needs_production_planning').text
        self.assertIn('id="planning-expand-table"',html)
        self.assertIn('aria-controls="planning-orders-card" aria-expanded="false"',html)
        self.assertIn('icon-tabler-arrows-maximize',html)
        self.assertIn('stroke="currentColor"',html)
        self.assertNotIn('planning-expand.jpg',html)
        self.assertIn('aria-label="Expand table" title="Expand table"',html)
        self.assertIn('data-expand-label>Expand table</span>',html)
        self.assertNotIn('id="planning-expand-table"',self.client.get('/planning?status=needs_material_review').text)

    def test_sidebar_has_no_instructional_footer(self):
        html=self.client.get('/planning').text
        self.assertNotIn('class="sidebar-footer"',html)
        self.assertNotIn('Review the SO and materials together.',html)
        self.assertNotIn('MES receives completed plans.',html)
        self.assertIn('Loaded test.',html)

    def test_search_form_is_in_sidebar_and_keeps_stage_and_query(self):
        for stage in ('needs_material_review','needs_production_planning'):
            with self.subTest(stage=stage):
                html=self.client.get('/planning',query_string={'status':stage,'q':'SO-1'}).text
                sidebar=html.split('<aside class="app-sidebar">',1)[1].split('</aside>',1)[0]
                main=html.split('<main id="workspace">',1)[1]
                self.assertIn('class="sidebar-search"',sidebar)
                self.assertIn('role="search"',sidebar)
                self.assertIn('name="status" value="'+stage+'"',sidebar)
                self.assertIn('id="q" name="q" type="search" value="SO-1"',sidebar)
                self.assertNotIn('<button',sidebar)
                self.assertNotIn('>Clear</a>',sidebar)
                self.assertIn('placeholder="SO / customer · Enter"',sidebar)
                self.assertNotIn('id="q"',main)
                self.assertNotIn('class="filters"',html)
                self.assertEqual(html.count('id="q"'),1)
                self.assertIn('/static/lt-planning.css?v=14',html)

    def test_search_shows_saved_records_without_calculating(self):
        with patch('lt_planning.material_context', wraps=material_context) as calculate:
            self.client.get('/planning?q=does-not-match')
            calculate.assert_not_called()
            self.assertEqual(self.client.get('/planning?q=SO-1').status_code,200)
            calculate.assert_not_called()

    def test_both_stages_use_the_same_table_layout(self):
        self.existing_review()
        for stage in ('needs_material_review','needs_production_planning'):
            with self.subTest(stage=stage):
                html=self.client.get('/planning',query_string={'status':stage}).text
                self.assertIn('class="table-wrap"',html)
                self.assertNotIn('production-planning-table',html)
                self.assertIn('/static/lt-planning.css?v=14',html)
                self.assertIn('class="result-card planning-orders-card"',html)
                if stage == 'needs_production_planning':
                    self.assertIn('Blocking items · uncovered / required',html)
                    self.assertIn('class="inline-plan-form"',html)
                    self.assertIn('>Save</button>',html)
                    self.assertNotIn('Date optional',html)
                    self.assertNotIn('name="production_start" type="date" required',html)
        css=(Path(__file__).resolve().parents[2]/'Webpage'/'static'/'lt-planning.css').read_text()
        self.assertIn('.table-wrap{width:100%;height:var(--planning-table-height,60vh);min-height:240px;overflow:auto}',css)
        self.assertIn('.table{width:100%;',css)
        self.assertNotIn('calc(100dvh - 300px)',css)
        self.assertIn('.planning-orders-card{margin-bottom:0}',css)
        self.assertNotIn('production-planning-table',css)

    def test_production_assignment_controls_are_compact_and_keep_notes(self):
        self.existing_review()
        self.review['notes']='Keep this note\nAnd this line'
        html=self.client.get('/planning?status=needs_production_planning').text
        self.assertIn('name="notes" rows="1"',html)
        self.assertIn('Keep this note\nAnd this line</textarea>',html)
        self.assertIn('class="visually-hidden" for="notes-1"',html)
        self.assertNotIn('btn-primary mt-2',html)
        css=(Path(__file__).resolve().parents[2]/'Webpage'/'static'/'lt-planning.css').read_text()
        self.assertIn('.inline-plan-form{position:relative;display:flex;align-items:center;',css)
        self.assertIn('resize:vertical',css)

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

    def test_initial_review_has_hidden_immediate_assignment_controls(self):
        html=self.client.get('/planning?so=SO-1').text
        self.assertIn('id="review-production-panel" class="mb-3" hidden',html)
        self.assertIn('id="review-production-start" name="production_start" type="date" disabled',html)
        self.assertIn('id="review-submit"',html)
        self.assertIn('/static/lt-planning.js?v=11',html)
        self.staged['returned_from_mes']=True
        html=self.client.get('/planning?so=SO-1').text
        self.assertNotIn('id="review-production-panel"',html)
        self.assertNotIn('id="review-production-start"',html)

    def test_no_blocker_review_can_assign_and_publish_atomically(self):
        response=self.post(production_start='2026-07-10',notes='Ready for assembly')
        self.assertEqual(response.status_code,200,response.text)
        self.assertTrue(response.json['handed_off'])
        self.assertEqual(response.json['next_status'],'needs_material_review')
        self.content_review.assert_called_once()
        self.publish.assert_called_once_with(self.conn,'SO-1','Ready for assembly')
        material=self.history('mrp_review_history')[0][1]
        planning=self.history('lt_planning_history')[0][1]
        self.assertTrue(material['none'])
        self.assertTrue(material['handoff'])
        self.assertEqual(json.loads(material['blockers']),[])
        self.assertEqual(planning['review'],material['id'])
        self.assertEqual(planning['id'],material['id'])
        self.assertEqual(planning['start'],date(2026,7,10))
        self.assertEqual(planning['notes'],material['notes'])
        self.assertFalse(planning['override'])
        self.engine.begin.assert_called_once()
        self.assertEqual(len(self.history('mrp_review_history')),1)
        self.assertEqual(len(self.history('lt_planning_history')),1)
        self.assertTrue(any('INSERT INTO public.production_overrides' in sql for sql,_ in self.writes))

    def test_combined_assignment_requires_explicit_no_blockers(self):
        blocker={'item':'PART-A','required_qty':10,'uncovered_qty':8}
        for extra in ({'no_blockers':False,'blockers':[blocker]},
                      {'no_blockers':False,'blockers':[]},
                      {'no_blockers':True,'blockers':[blocker]}):
            with self.subTest(extra=extra):
                response=self.post(production_start='2026-07-10',**extra)
                self.assertEqual(response.status_code,400,response.text)
        self.assertEqual(self.writes,[])
        self.content_review.assert_not_called()
        self.publish.assert_not_called()

    def test_combined_assignment_validates_date_before_saving_review(self):
        self.so.loc[0,'Ship Date']='2026-07-15'
        for start in ('2026-06-30','2026-07-11','2026-07-20',''):
            with self.subTest(start=start):
                response=self.post(production_start=start,ship_date='2099-12-31')
                self.assertEqual(response.status_code,400,response.text)
        self.assertEqual(self.writes,[])
        self.content_review.assert_not_called()
        self.publish.assert_not_called()
        self.assertEqual(self.post(production_start='2026-07-10').status_code,200)
        self.assertEqual(self.history('lt_planning_history')[0][1]['ship'],date(2026,7,15))

    def test_combined_assignment_cannot_reschedule_returned_revision(self):
        self.staged['returned_from_mes']=True
        response=self.post(production_start='2026-07-10')
        self.assertEqual(response.status_code,400,response.text)
        self.assertEqual(self.writes,[])
        self.publish.assert_not_called()

    def test_combined_assignment_rejects_stale_material_review(self):
        self.assertEqual(self.post(production_start='2026-07-10',fingerprint='old').status_code,400)
        self.assertEqual(self.writes,[])
        self.content_review.assert_not_called()
        self.publish.assert_not_called()

    def test_combined_assignment_retry_does_not_duplicate_histories(self):
        payload=self.payload(production_start='2026-07-10')
        response=self.client.post('/lt_planning/review',json=payload,headers={'X-CSRF-Token':'token'})
        self.assertEqual(response.status_code,200,response.text)
        self.duplicate='SO-1'
        self.staged['workflow_status']='released_to_mes'
        response=self.client.post('/lt_planning/review',json=payload,headers={'X-CSRF-Token':'token'})
        self.assertEqual(response.status_code,200,response.text)
        self.assertEqual(len(self.history('mrp_review_history')),1)
        self.assertEqual(len(self.history('lt_planning_history')),1)
        self.assertEqual(self.publish.call_count,1)

    def test_combined_publication_failure_rolls_back_and_never_writes_schedule(self):
        self.publish.side_effect=ValueError('Saved allocation conflict')
        response=self.post(production_start='2026-07-10')
        self.assertEqual(response.status_code,400,response.text)
        self.assertFalse(any('production_overrides' in sql for sql,_ in self.writes))
        self.assertEqual(self.history('lt_planning_history'),[])
        # SQLAlchemy engine.begin() receives the exception and rolls back the
        # earlier SO/material-review writes in this same transaction.
        self.assertIs(self.engine.begin.return_value.__exit__.call_args.args[0],ValueError)

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
        self.staged['source_present']=False
        self.assertNotIn('<strong>SO-1</strong>',self.client.get('/planning').text)
        self.assertNotIn('<strong>SO-1</strong>',self.client.get('/planning?q=SO-1&so=SO-1').text)
        self.assertEqual(self.post().status_code,400)
        self.staged['source_present']=True
        self.staged['latest_snapshot']['inventory_site']=' Drop Ship '
        self.assertNotIn('<strong>SO-1</strong>',self.client.get('/planning').text)
        self.assertEqual(self.post().status_code,400)

    def test_missing_source_material_review_not_counted_or_shown(self):
        self.staged.update(source_present=False,previous_snapshot=self.staged['latest_snapshot'],latest_snapshot=None)
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
