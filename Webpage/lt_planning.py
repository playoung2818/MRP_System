"""Current MRP work only. History remains append-only in the database."""
import json
import secrets
from datetime import date, timezone
from pathlib import Path
from uuid import UUID, uuid4
import pandas as pd
from flask import Blueprint, abort, current_app, jsonify, redirect, render_template, request, session, url_for
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from lt_materials import CUTOFF, material_context, validate_blockers
from so_review.combined import content_context, review_content
from so_review.service import is_drop_ship_order

REVIEW, PLANNING = 'needs_material_review', 'needs_production_planning'
LABELS = {REVIEW: 'Needs Material Review', PLANNING: 'Needs Production Planning'}


def order_age(created, today=None):
    """Use the source order date, never the import/review/assignment timestamp."""
    created = pd.to_datetime(created, errors='coerce')
    if pd.isna(created) or created >= CUTOFF:
        return '', None, ''
    created = created.date()
    days = ((today or date.today()) - created).days
    color = 'so-age-danger' if days > 14 else 'so-age-warning' if days > 7 else ''
    return created.isoformat(), days, color


def source_ship_date(group):
    if group.empty or 'Ship Date' not in group:
        return None
    dates = pd.to_datetime(group['Ship Date'], errors='coerce')
    placeholders = ((dates.dt.month.eq(7) & dates.dt.day.eq(4)) | (dates.dt.month.eq(12) & dates.dt.day.eq(31)))
    if dates.isna().any() or placeholders.any() or dates.ge(CUTOFF).any():
        return None
    return dates.max().date()


def displayed_ship_date(group):
    if group.empty or 'Ship Date' not in group:
        return ''
    return ', '.join(sorted(set(pd.to_datetime(group['Ship Date'], errors='coerce').dropna().dt.strftime('%Y-%m-%d'))))


def publish_to_mes(conn, qb_num, notes, returned=False):
    from mrp_system.order_staging import publish_completed_order
    publish_completed_order(conn, qb_num, notes=notes, allow_reconciliation=returned)


def latest(conn, table, qb_num):
    column = 'assessed_at' if table == 'mrp_review_history' else 'assigned_at'
    return conn.execute(text(f'SELECT * FROM public.{table} WHERE qb_num=:so ORDER BY {column} DESC, id DESC LIMIT 1'), {'so': qb_num}).mappings().first()


def requirements_frame(so, record, wo_records=(), finished_goods=False):
    group = so.loc[so['QB Num'].fillna('').astype(str).str.strip().eq(record['sales_order'])].copy()
    source = record['latest_snapshot'] or {}
    if group.empty:
        group = pd.DataFrame([{'QB Num': record['sales_order'], 'Item': i['item'],
                              'Qty(-)': i.get('remaining_quantity', i['quantity']),
                              'Ship Date': source.get('ship_date'), 'Name': source.get('customer', '')}
                             for i in source.get('items', [])],
                            columns=['QB Num', 'Item', 'Qty(-)', 'Ship Date', 'Name'])
    if not wo_records and not finished_goods:
        return group
    from collections import defaultdict
    from mrp_system.so_workflow import shipment_conflicts
    uncertain = {r['item'] for r in shipment_conflicts(source.get('items', []), wo_records, source)}
    covered, shipped = defaultdict(int), defaultdict(int)
    for wo in wo_records:
        if not wo.get('is_shipped'):
            for item in wo['items'] or []:
                covered[item['item'].strip().casefold()] += int(item['quantity'])
    for item in source.get('items', []):
        shipped[item['item'].strip().casefold()] += int(item.get('shipped_quantity', 0))
    if finished_goods:
        for name, qty in (record.get('approved_order_signature') or {}).items():
            covered[name] = max(covered[name], max(0, qty-shipped[name]))
    # Unknown shipment attribution stays conservative, rather than silently
    # treating already allocated WOs as either new work or completed shipment.
    for index, row in group.iterrows():
        name = str(row['Item']).strip().casefold()
        qty = float(row['Qty(-)'])
        deduction = 0 if name in uncertain else min(qty, covered[name])
        group.at[index, 'Qty(-)'] = qty-deduction
        covered[name] -= deduction
    return group


def create_lt_planning(engine, source):
    bp = Blueprint('lt_planning', __name__, template_folder=str(Path(__file__).parent / 'templates'))

    @bp.app_template_filter('lt_utc')
    def utc_timestamp(value):
        return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

    def data(force=False):
        so, ledger, pods, loaded, error = source(force)
        if error or so is None or ledger is None or pods is None:
            abort(503, description='MRP data could not be loaded. Reload before planning.')
        return so, ledger, pods, loaded

    def csrf():
        if 'lt_csrf' not in session:
            session['lt_csrf'] = secrets.token_urlsafe(32)
        return session['lt_csrf']

    @bp.errorhandler(SQLAlchemyError)
    def database_error(exc):
        current_app.logger.exception('SO workflow database operation failed')
        message = 'SO workflow unavailable. Check database access and the shared SO workflow schema.'
        if request.method == 'POST':
            return jsonify(error=message), 503
        return render_template('lt_planning.html', db_error=message), 503

    @bp.get('/lt_planning')
    @bp.get('/planning')
    def workspace():
        status = request.args.get('status', REVIEW)
        status = {'needs_review': REVIEW, 'reviewed': PLANNING, 'all': REVIEW}.get(status, status)
        if status == 'planning_complete' or request.args.get('assigned') == '1':
            return redirect(url_for('lt_planning.workspace', status=REVIEW))
        if status not in LABELS:
            status = REVIEW
        so, ledger, pods, loaded = data(request.args.get('reload') == '1')
        query, selected = request.args.get('q', '').strip(), request.args.get('so', '').strip()
        with engine.connect() as conn:
            records = conn.execute(text("SELECT * FROM public.approved_work_orders WHERE workflow_status IN ('needs_material_review','needs_production_planning') ORDER BY sales_order")).mappings().all()
            reviews = {r['qb_num']: dict(r) for r in conn.execute(text('SELECT DISTINCT ON (qb_num) * FROM public.mrp_review_history ORDER BY qb_num, assessed_at DESC, id DESC')).mappings().all()}
            from collections import defaultdict
            wo_by_so = defaultdict(list)
            for wo in conn.execute(text('SELECT sales_order,items,is_shipped FROM public."WO Details" WHERE record_source=\'mes\'')).mappings().all():
                wo_by_so[wo['sales_order']].append(wo)
            finished = {r['wo_number']: r['is_finished_goods'] for r in conn.execute(text('SELECT wo_number,is_finished_goods FROM public.production_overrides')).mappings().all()}
            from mrp_system.transform.sales_order import normalize_wo_number
            created_dates = {}
            for entry in conn.execute(text('SELECT "QB Num" AS sales_order, MIN("Order Date") AS created_date FROM public.open_sales_orders GROUP BY "QB Num"')).mappings().all():
                number = normalize_wo_number(str(entry['sales_order'] or '').strip())
                created = pd.to_datetime(entry['created_date'], errors='coerce')
                if pd.notna(created) and (number not in created_dates or created < created_dates[number]):
                    created_dates[number] = created
            all_rows, detail = [], None
            for record in records:
                number = record['sales_order']
                snapshot = record['latest_snapshot'] or record['previous_snapshot'] or record['approved_snapshot'] or {}
                # The material-review queue contains current imported orders only.
                # Keep missing-source records/WOs in the database, not this queue.
                if is_drop_ship_order(snapshot) or (record['workflow_status'] == REVIEW and (not record['source_present'] or not record['latest_snapshot'])):
                    continue
                group = requirements_frame(so, record, wo_by_so[number], finished.get(number, False))
                review = reviews.get(number)
                customer = snapshot.get('customer') or record['customer'] or ''
                # Saved assessments are authoritative on lists and during date
                # assignment. Only opening a new material review calculates suggestions.
                needs_material = number == selected and record['workflow_status'] == REVIEW and record['source_present']
                fingerprint, suggestions = (material_context(group, ledger, pods, number)
                                            if needs_material and not group.empty
                                            else ((review or {}).get('source_fingerprint', ''), []))
                created_date, age_days, age_class = order_age(created_dates.get(number))
                row = {'so': number, 'customer': customer,
                       'created_date': created_date, 'age_days': age_days, 'age_class': age_class,
                       'status': record['workflow_status'], 'returned': record['returned_from_mes'],
                       'source_missing': not record['source_present'], 'review': review,
                       'ship_date': displayed_ship_date(group) or snapshot.get('ship_date', ''),
                       'assignment': {'so': number, 'fingerprint': fingerprint,
                                      'review_id': str(review['id']) if review else '',
                                      'submission_id': str(uuid4())}}
                all_rows.append(row)
                if number == selected and row['status'] == REVIEW and not row['source_missing']:
                    content = content_context(record)
                    allocations = wo_by_so[number]
                    detail = {'so': number, 'fingerprint': fingerprint, 'suggestions': suggestions,
                              'review': review, 'ship_date': row['ship_date'], 'content': content,
                              'returned': row['returned'], 'eligible': bool(content),
                              'notes': (record['metadata_fields'] or {}).get('notes', (review or {}).get('notes', '')) if row['returned'] else (review or {}).get('notes', ''),
                              'allocations': [i for wo in allocations for i in wo['items'] or []]}
        counts = {key: sum(row['status'] == key for row in all_rows) for key in LABELS}
        rows = [row for row in all_rows if (query.casefold() in (row['so']+' '+row['customer']).casefold() if query else row['status'] == status)]
        rows.sort(key=lambda row: (row['ship_date'].split(', ')[0] or '9999-12-31', row['so']))
        return render_template('lt_planning.html', rows=rows, counts=counts, status=status, query=query,
                               detail=detail, loaded=loaded, csrf_token=csrf(),
                               review_submission=str(uuid4()), stage_labels=LABELS)

    @bp.post('/lt_planning/acknowledge_source')
    def obsolete_source_approval():
        return jsonify(error='SO revisions must be reviewed through Needs Material Review.'), 410

    @bp.post('/lt_planning/<action>')
    def save(action):
        if action not in ('review', 'assign'):
            abort(404)
        if not secrets.compare_digest(request.headers.get('X-CSRF-Token', ''), csrf()):
            return jsonify(error='Session expired. Reload before saving.'), 403
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(error='A JSON form is required.'), 400
        try:
            submission = str(UUID(str(payload.get('submission_id', ''))))
            number = str(payload.get('so') or '').strip()
            notes = str(payload.get('notes') or '').strip()
            if not number or len(notes) > 4000:
                raise ValueError('An SO is required. Notes must be under 4,000 characters.')
            so, ledger, pods, loaded = data(True)
            with engine.begin() as conn:
                conn.execute(text('SELECT pg_advisory_xact_lock(26091003)'))
                conn.execute(text('SELECT pg_advisory_xact_lock(hashtextextended(:so, 0))'), {'so': number})
                table = 'mrp_review_history' if action == 'review' else 'lt_planning_history'
                duplicate = conn.execute(text(f'SELECT qb_num FROM public.{table} WHERE id=CAST(:id AS uuid)'), {'id': submission}).scalar_one_or_none()
                if duplicate is not None:
                    if duplicate != number:
                        raise ValueError('Invalid submission ID.')
                    owner = conn.execute(text('SELECT workflow_status FROM public.approved_work_orders WHERE sales_order=:so'), {'so': number}).scalar_one_or_none()
                    return jsonify(ok=True, so=number, next_status=PLANNING if owner == PLANNING else REVIEW,
                                   handed_off=owner != PLANNING)
                record = conn.execute(text('SELECT * FROM public.approved_work_orders WHERE sales_order=:so FOR UPDATE'), {'so': number}).mappings().first()
                expected_status = REVIEW if action == 'review' else PLANNING
                if not record or record['workflow_status'] != expected_status:
                    raise ValueError('This SO is not owned by this MRP step. Reload the workspace.')
                if not record['source_present'] or not record['latest_snapshot']:
                    raise ValueError('SO missing from current source. Resolve its disposition before release.')
                if is_drop_ship_order(record['latest_snapshot']):
                    raise ValueError('Drop Ship SOs are excluded from production planning.')
                wo_records = conn.execute(text('SELECT items,is_shipped FROM public."WO Details" WHERE sales_order=:so AND record_source=\'mes\''), {'so': number}).mappings().all()
                finished_goods = conn.execute(text('SELECT is_finished_goods FROM public.production_overrides WHERE wo_number=:so'), {'so': number}).scalar_one_or_none() is True
                group = requirements_frame(so, record, wo_records, finished_goods)
                review = latest(conn, 'mrp_review_history', number)
                if action == 'review':
                    fingerprint, suggestions = material_context(group, ledger, pods, number)
                    if payload.get('fingerprint') != fingerprint:
                        raise ValueError('MRP data changed since opening this form. Reload and review again.')
                if str(payload.get('review_id') or '') != (str(review['id']) if review else ''):
                    raise ValueError('Another review was saved. Reload before continuing.')
                returned = bool(record['returned_from_mes'])
                if 'notes' not in payload:
                    notes = str(((record['metadata_fields'] or {}).get('notes', '') if returned else (review or {}).get('notes', '')) or '')
                if len(notes) > 4000:
                    raise ValueError('Notes must be under 4,000 characters.')
                if action == 'review':
                    requirements = {r['item']: r['required_qty'] for r in suggestions}
                    blockers = validate_blockers(payload.get('blockers', []), payload.get('no_blockers') is True, requirements)
                    review_content(conn, number, payload.get('order_content') or {})
                    conn.execute(text('''INSERT INTO public.mrp_review_history
                        (id,qb_num,run_id,source_fingerprint,reviewed_by,no_blockers,blockers,system_suggestions,notes,
                         incident_id,source_revision,review_kind,mes_sent_at)
                        VALUES (CAST(:id AS uuid),:so,:run,:fingerprint,'Manual review',:none,
                         CAST(:blockers AS jsonb),CAST(:suggestions AS jsonb),:notes,CAST(:incident AS uuid),
                         :version,:kind,CASE WHEN :returned THEN CURRENT_TIMESTAMP ELSE NULL END)'''),
                        {'id': submission, 'so': number, 'run': 'manual-'+submission, 'fingerprint': fingerprint,
                         'none': not blockers, 'blockers': json.dumps(blockers), 'suggestions': json.dumps(suggestions),
                         'notes': notes, 'incident': str(record['review_incident_id']), 'version': record['source_revision'],
                         'kind': 'returned_from_mes' if returned else 'initial', 'returned': returned})
                    if returned:
                        # Existing production date/finished-goods state and WOs are untouched.
                        publish_to_mes(conn, number, notes, returned=True)
                    else:
                        from mrp_system.so_workflow import order_signature
                        conn.execute(text("UPDATE public.approved_work_orders SET workflow_status='needs_production_planning', approved_order_signature=CAST(:signature AS jsonb) WHERE sales_order=:so"),
                                     {'so': number, 'signature': json.dumps(order_signature(record['latest_snapshot']))})
                    conn.execute(text('UPDATE public.approved_work_orders SET material_review_id=CAST(:id AS uuid) WHERE sales_order=:so'), {'id': submission, 'so': number})
                else:
                    if not review or str(record['material_review_id']) != str(review['id']):
                        raise ValueError('Approve the current material review before assigning production.')
                    from mrp_system.so_workflow import order_signature
                    if record['approved_order_signature'] != order_signature(record['latest_snapshot']):
                        raise ValueError('Items or ordered quantities changed. Return to material review.')
                    start = date.fromisoformat(str(payload.get('production_start', '')))
                    if start < date.today() or start.weekday() >= 5:
                        raise ValueError('Production date must be today or a future weekday.')
                    ship = source_ship_date(group)
                    if ship and start > ship:
                        raise ValueError('Production start must not follow the imported ship date.')
                    publish_to_mes(conn, number, notes)
                    conn.execute(text('''INSERT INTO public.production_overrides
                        (wo_number,production_date,is_finished_goods,schedule_updated_at,schedule_updated_by,finished_goods_updated_at,finished_goods_updated_by)
                        VALUES (:so,:start,FALSE,CURRENT_TIMESTAMP AT TIME ZONE 'UTC','MRP Planning',NULL,NULL)
                        ON CONFLICT (wo_number) DO UPDATE SET production_date=EXCLUDED.production_date,is_finished_goods=FALSE,
                        schedule_updated_at=EXCLUDED.schedule_updated_at,schedule_updated_by=EXCLUDED.schedule_updated_by,
                        finished_goods_updated_at=NULL,finished_goods_updated_by=NULL'''), {'so': number,'start': start})
                    conn.execute(text('''INSERT INTO public.lt_planning_history
                        (id,qb_num,review_id,planned_production_start,assigned_ship_date,assigned_by,blocker_override,notes,mes_sent_at)
                        VALUES (CAST(:id AS uuid),:so,CAST(:review AS uuid),:start,:ship,'Manual assignment',:override,:notes,CURRENT_TIMESTAMP)'''),
                        {'id': submission,'so': number,'review': str(review['id']),'start': start,'ship': ship,
                         'override': bool(review['blockers']),'notes': notes})
                    conn.execute(text('UPDATE public.approved_work_orders SET material_review_id=CAST(:id AS uuid) WHERE sales_order=:so'), {'id': str(review['id']),'so': number})
            handed_off = action == 'assign' or returned
            return jsonify(ok=True, so=number, handed_off=handed_off, next_status=REVIEW if handed_off else PLANNING)
        except (ValueError, TypeError, KeyError) as exc:
            return jsonify(error=str(exc)), 400

    return bp
