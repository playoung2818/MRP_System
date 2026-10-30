"""KPI calendar arithmetic, first-event semantics and read-only dashboard."""
from copy import deepcopy
from datetime import date
from pathlib import Path
import json
import sys
import unittest
from unittest.mock import MagicMock

import pandas as pd
from flask import Flask
from sqlalchemy.exc import SQLAlchemyError

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'Webpage'))
from planning_kpi import create_planning_kpi
from mrp_system.planning_kpi import (
    build_planning_kpi, created_date, retain_creation_date, source_created_dates,
)
from mrp_system.order_staging import stage_cli_orders


class PlanningKpiTests(unittest.TestCase):
    def record(self, number='SO-20260001', **extra):
        result = dict(sales_order=number, customer='Customer', source_present=True,
                      workflow_status='needs_production_planning', metadata_fields={})
        result.update(extra)
        return result

    def test_mean_median_p90_and_waiting_are_separate(self):
        records=[self.record(f'SO-2026000{i}') for i in range(1,6)]
        histories=[dict(qb_num=r['sales_order'],assigned_at=f'2026-10-{1+days:02d}T12:00:00Z')
                   for r,days in zip(records,[0,1,3,10])]
        dates={r['sales_order']:date(2026,10,1) for r in records}
        report=build_planning_kpi(records,histories,dates,now='2026-10-20T12:00:00Z')
        self.assertEqual(report['average'],3.5)
        self.assertEqual(report['median'],2)
        self.assertAlmostEqual(report['p90'],7.9)
        self.assertEqual(report['awaiting_count'],1)
        self.assertEqual(report['assigned_count'],4)
        self.assertEqual(report['rows'][0]['days'],19)
        self.assertTrue(report['rows'][0]['awaiting'])

    def test_first_save_not_scheduled_start_or_later_reschedule(self):
        report=build_planning_kpi([self.record()], [
            dict(qb_num='SO-20260001',assigned_at='2026-10-15T12:00:00Z',planned_production_start='2027-01-01'),
            dict(qb_num='SO-20260001',assigned_at='2026-10-03T12:00:00Z',planned_production_start='2026-12-01')],
            {'SO-20260001':date(2026,10,1)},now='2026-10-20')
        self.assertEqual(report['rows'][0]['days'],2)
        self.assertIn('2026-10-03',report['rows'][0]['first_assigned_at'])
        self.assertFalse(report['rows'][0]['awaiting'])

    def test_calendar_days_include_weekends_and_same_day_is_zero(self):
        r=self.record()
        for assigned,expected in [('2026-10-05T00:01:00Z',3),('2026-10-02T23:59:00Z',0)]:
            report=build_planning_kpi([r],[dict(qb_num=r['sales_order'],assigned_at=assigned)],
                                      {r['sales_order']:date(2026,10,2)},now='2026-10-20')
            self.assertEqual(report['average'],expected)

    def test_utc_day_boundary_is_explicit(self):
        report=build_planning_kpi([self.record()],[dict(qb_num='SO-20260001',assigned_at='2026-10-01T23:00:00-05:00')],
            {'SO-20260001':date(2026,10,1)},now='2026-10-20')
        self.assertEqual(report['rows'][0]['days'],1)
        self.assertIn('2026-10-02 04:00:00 UTC',report['rows'][0]['first_assigned_at'])

    def test_retained_creation_survives_missing_source_and_revision_return(self):
        r=self.record()
        r.update(metadata_fields={'planning_kpi':{'created_date':'2026-10-01'}},
                 source_present=False,returned_from_mes=True,workflow_status='needs_material_review')
        report=build_planning_kpi([r],[dict(qb_num=r['sales_order'],assigned_at='2026-10-04T12:00:00Z')],{},now='2026-10-20')
        self.assertEqual(report['average'],3)
        self.assertEqual(report['awaiting_count'],0)
        self.assertIn('Source missing',report['rows'][0]['stage'])

    def test_missing_invalid_future_dates_are_not_zero_or_averaged(self):
        histories=[dict(qb_num='SO-20260001',assigned_at='2026-09-30T12:00:00Z')]
        report=build_planning_kpi([self.record()],histories,{'SO-20260001':date(2026,10,1)},now='2026-10-20')
        self.assertIsNone(report['average'])
        self.assertIsNone(report['rows'][0]['days'])
        for value in ('2099-12-31','1900-01-01','',None,0,{},'not-a-date'):
            self.assertIsNone(created_date(value))
        report=build_planning_kpi([self.record()],[],{'SO-20260001':date(2026,10,21)},now='2026-10-20')
        self.assertEqual(report['awaiting_count'],1)
        self.assertIsNone(report['rows'][0]['days'])

    def test_legacy_released_and_missing_source_are_not_new_waiters(self):
        for update in ({'workflow_status':'released_to_mes'}, {'source_present':False},
                       {'returned_from_mes':True}, {'mes_released_at':'2026-10-01'}, {'workflow_status':'closed'}):
            r=self.record();r.update(update)
            report=build_planning_kpi([r],[],{'SO-20260001':date(2026,10,1)},now='2026-10-20')
            self.assertEqual(report['awaiting_count'],0,update)
            self.assertIsNone(report['average'])
            self.assertIsNone(report['rows'][0]['days'])

    def test_material_content_approval_is_not_a_production_assignment(self):
        r=self.record(has_published_snapshot=True)
        report=build_planning_kpi([r],[],{'SO-20260001':date(2026,10,1)},now='2026-10-20')
        self.assertEqual(report['awaiting_count'],1)
        self.assertEqual(report['rows'][0]['days'],19)

    def test_canonical_aliases_count_once_and_first_history_wins(self):
        canonical=self.record()
        alias=dict(canonical,sales_order='EO-20260001',customer='Retired alias')
        histories=[dict(qb_num='EO-20260001',assigned_at='2026-10-02'),dict(qb_num='SO-20260001',assigned_at='2026-10-04')]
        report=build_planning_kpi([canonical,alias],histories,{'SO-20260001':date(2026,10,1)},now='2026-10-20')
        self.assertEqual(len(report['rows']),1)
        self.assertEqual(report['rows'][0]['customer'],'Customer')
        self.assertEqual(report['average'],1)

    def test_empty_report_has_no_fake_zero_metrics(self):
        report=build_planning_kpi([],[],{},now='2026-10-20')
        self.assertEqual(report['rows'],[])
        for key in ('average','median','p90'):self.assertIsNone(report[key])

    def test_source_original_date_capture_and_preserved_metadata(self):
        frame=pd.DataFrame([{'QB Num':'EO-20260001','SO Entry Date':'2020-10-02'},
                            {'QB Num':'SO-20260001','SO Entry Date':'2020-10-01'}])
        dates=source_created_dates(frame)
        self.assertEqual(dates,{'SO-20260001':date(2020,10,1)})
        old={'notes':'Keep notes','planning_note_history':[{'notes':'Keep history'}]}
        original=deepcopy(old)
        stored=retain_creation_date(old,dates['SO-20260001'],today=date(2026,10,20))
        self.assertEqual(old,original)
        self.assertEqual(stored['notes'],'Keep notes')
        self.assertEqual(stored['planning_note_history'],old['planning_note_history'])
        self.assertEqual(stored['planning_kpi']['created_date'],'2020-10-01')
        self.assertEqual(retain_creation_date(stored,'2021-01-01',today=date(2026,10,20)),stored)
        self.assertEqual(retain_creation_date({},'2099-12-31'),{})
        self.assertEqual(source_created_dates(pd.DataFrame()),{})

    def test_staging_retains_date_without_adding_it_to_amendment_snapshot(self):
        engine=MagicMock();conn=engine.begin.return_value.__enter__.return_value
        conn.execute.return_value.mappings.return_value.__iter__.return_value=iter([])
        frame=pd.DataFrame([{'QB Num':'SO-20260001','Item':'ABC','Qty':1,
                             'Inventory Site':'WH01S-NTA','SO Entry Date':'2020-10-01'}])
        stage_cli_orders(engine,frame)
        sql,params=conn.execute.call_args.args
        self.assertIn('CAST(:metadata AS json)',str(sql))
        self.assertEqual(json.loads(params['metadata'])['planning_kpi']['created_date'],'2020-10-01')
        self.assertNotIn('created_date',json.loads(params['snapshot']))
        self.assertNotIn('production_overrides',' '.join(str(c.args[0]) for c in conn.execute.call_args_list))


class PlanningKpiRouteTests(unittest.TestCase):
    def setUp(self):
        self.engine=MagicMock();self.conn=self.engine.connect.return_value.__enter__.return_value
        self.conn.execution_options.return_value=self.conn
        self.calls=[]
        self.blockers=None
        def execute(statement,params=None):
            sql=str(statement);self.calls.append(sql)
            result=MagicMock()
            if 'FROM public.sales_order_workflow' in sql:
                rows=[dict(sales_order='SO-1',customer='<script>Customer</script>',workflow_status='needs_production_planning',
                           source_present=True,metadata_fields={},blockers=self.blockers)]
            elif 'FROM public.open_sales_orders' in sql:
                rows=[{'QB Num':'SO-1','Order Date':'2020-10-01'}]
            else:rows=[]
            result.mappings.return_value=rows
            return result
        self.conn.execute.side_effect=execute
        self.app=Flask(__name__)
        self.app.jinja_env.filters['canonical_item']=lambda value: {'I7-9700E':'i7-9700E'}.get(value,value)
        self.app.register_blueprint(create_planning_kpi(self.engine))
        self.client=self.app.test_client()

    def test_dashboard_has_requested_metrics_and_exact_six_columns(self):
        response=self.client.get('/planning/kpi')
        self.assertEqual(response.status_code,200)
        html=response.text
        for label in ('Average days to assignment','Median days to assignment','90th-percentile days to assignment',
                      'Awaiting first assignment','Created date','First assigned at','Days to assign / waiting',
                      'Blocking items · uncovered / required'):
            self.assertIn(label,html)
        self.assertEqual(html.count('<th>'),6)
        self.assertIn('&lt;script&gt;Customer&lt;/script&gt;',html)
        self.assertNotIn('<script>Customer</script>',html)
        self.assertTrue(all(sql.lstrip().startswith('SELECT ') for sql in self.calls))
        self.engine.begin.assert_not_called()
        self.assertEqual(self.client.post('/planning/kpi').status_code,405)

    def test_blockers_are_saved_quantities_with_canonical_display_and_pod(self):
        self.blockers=[dict(item='I7-9700E',uncovered_qty=3,required_qty=7,
                            pod_num='POD-TEST',reason='Saved blocker reason')]
        html=self.client.get('/planning/kpi').text
        self.assertIn('<strong>i7-9700E</strong>',html)
        self.assertIn('3 / 7',html)
        self.assertIn('POD# POD-TEST',html)
        self.assertIn('Saved blocker reason',html)
        self.assertEqual(self.blockers[0]['item'],'I7-9700E')
        self.assertNotIn('<th>Stage</th>',html)
        self.assertTrue(any('review.id = workflow.material_review_id' in sql for sql in self.calls))
        self.assertTrue(all(sql.lstrip().startswith('SELECT ') for sql in self.calls))

    def test_no_review_is_distinct_from_saved_review_without_blockers(self):
        self.assertIn('SO and material review required',self.client.get('/planning/kpi').text)
        self.blockers=[]
        html=self.client.get('/planning/kpi').text
        self.assertIn('No recorded material blockers',html)
        self.assertNotIn('SO and material review required',html)

    def test_kpi_sidebar_is_part_of_planning_not_the_home_workspace(self):
        html=self.client.get('/planning/kpi').text
        sidebar=html.split('<aside class="app-sidebar">',1)[1].split('</aside>',1)[0]
        self.assertIn('aria-label="Planning stages"',sidebar)
        self.assertIn('/planning?status=needs_material_review',sidebar)
        self.assertIn('/planning?status=needs_production_planning',sidebar)
        self.assertIn('aria-current="page" href="/planning/kpi"',sidebar)
        for href in ('/quotation_lookup','/item_info','/purchase_orders'):
            self.assertNotIn(href,sidebar)

    def test_database_failure_does_not_report_zero_metrics(self):
        self.conn.execute.side_effect=SQLAlchemyError('test unavailable')
        response=self.client.get('/planning/kpi')
        self.assertEqual(response.status_code,503)
        self.assertIn('Planning KPI is unavailable',response.text)
        self.assertNotIn('test unavailable',response.text)
