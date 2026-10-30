"""Read-only SO planning KPI dashboard over existing workflow/history tables."""
from pathlib import Path
import pandas as pd
from flask import Blueprint, current_app, render_template
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from mrp_system.planning_kpi import build_planning_kpi, source_created_dates


def create_planning_kpi(engine):
    bp = Blueprint('planning_kpi', __name__, template_folder=str(Path(__file__).parent / 'templates'))

    @bp.get('/planning/kpi')
    def dashboard():
        try:
            with engine.connect() as conn:
                conn = conn.execution_options(isolation_level='REPEATABLE READ')
                records = list(conn.execute(text('''SELECT sales_order,
                    COALESCE(NULLIF(customer, ''), NULLIF(latest_snapshot->>'customer', ''),
                             NULLIF(previous_snapshot->>'customer', ''), approved_snapshot->>'customer', '') AS customer,
                    workflow_status, source_present, metadata_fields, mes_released_at, returned_from_mes,
                    review.blockers
                    FROM public.sales_order_workflow workflow
                    LEFT JOIN public.mrp_review_history review ON review.id = workflow.material_review_id''')).mappings())
                assignments = list(conn.execute(text('''SELECT qb_num, MIN(assigned_at) AS assigned_at
                    FROM public.lt_planning_history GROUP BY qb_num''')).mappings())
                source = pd.DataFrame(list(conn.execute(text('''SELECT DISTINCT "QB Num", "Order Date"
                    FROM public.open_sales_orders''')).mappings()), columns=['QB Num', 'Order Date'])
            report = build_planning_kpi(records, assignments, source_created_dates(source))
            return render_template('planning_kpi.html', report=report, error=None)
        except SQLAlchemyError:
            current_app.logger.exception('Planning KPI read failed')
            return render_template('planning_kpi.html', report=None,
                                   error='Planning KPI is unavailable. Check database access and try again.'), 503

    return bp
