"""Manual LT workflow. No ETL writes, source SO updates, or implicit schema DDL."""
import json
import secrets
from datetime import date, timezone
from pathlib import Path
from uuid import UUID, uuid4, uuid5, NAMESPACE_URL

import pandas as pd
from flask import Blueprint, abort, current_app, jsonify, render_template, request, session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from lt_materials import CUTOFF, REASONS, material_context, validate_blockers
from unassigned_so import unassigned_orders
from so_review.combined import content_context, review_content
from so_review.service import is_drop_ship_order


def source_ship_date(group):
    """Use the current CLI-imported SO dates, never a browser-supplied date.

    A partially assigned order is still unassigned; the latest real line date is
    the order-level ship date once every line has a usable date.
    """
    if group.empty or "Ship Date" not in group:
        return None
    dates = pd.to_datetime(group["Ship Date"], errors="coerce")
    placeholders = ((dates.dt.month.eq(7) & dates.dt.day.eq(4))
                    | (dates.dt.month.eq(12) & dates.dt.day.eq(31)))
    if dates.isna().any() or placeholders.any() or dates.ge(CUTOFF).any():
        return None
    return dates.max().date()


def displayed_ship_date(group):
    """Show imported dates, including placeholders, without status substitution."""
    if group.empty or "Ship Date" not in group:
        return ""
    dates = pd.to_datetime(group["Ship Date"], errors="coerce").dropna()
    return ", ".join(sorted(set(dates.dt.strftime("%Y-%m-%d"))))


def current_blockers(suggestions, prior_review):
    """Refresh quantities/reasons without discarding manually recorded POD refs."""
    prior = {b['item']: b for b in (prior_review or {}).get('blockers', [])}
    rows = []
    for suggestion in suggestions:
        if not suggestion['blocking']:
            continue
        blocker = {k: suggestion[k] for k in ('item', 'required_qty', 'uncovered_qty', 'reason')}
        if prior.get(suggestion['item'], {}).get('pod_num'):
            blocker['pod_num'] = prior[suggestion['item']]['pod_num']
        if suggestion.get('warning'):
            blocker['warning'] = suggestion['warning']
        rows.append(blocker)
    return rows


def publish_to_mes(conn, qb_num, notes):
    # Lazy import: server configures the MRP module path before handling requests.
    from mrp_system.order_staging import publish_completed_order
    publish_completed_order(conn, qb_num, notes=notes)


def latest(conn, table, qb_num):
    # Table is chosen internally, never from a request.
    column = "assessed_at" if table == "mrp_review_history" else "assigned_at"
    return conn.execute(text(f"SELECT * FROM public.{table} WHERE qb_num=:so ORDER BY {column} DESC, id DESC LIMIT 1"), {"so": qb_num}).mappings().first()


def create_lt_planning(engine, source):
    bp = Blueprint("lt_planning", __name__, template_folder=str(Path(__file__).parent / "templates"))

    @bp.app_template_filter("lt_utc")
    def utc_timestamp(value):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def data(force=False):
        so, ledger, pods, loaded, error = source(force)
        if error or so is None or ledger is None or pods is None:
            abort(503, description="MRP data could not be loaded. Reload the data before planning.")
        return so, ledger, pods, loaded

    def csrf():
        if "lt_csrf" not in session:
            session["lt_csrf"] = secrets.token_urlsafe(32)
        return session["lt_csrf"]

    @bp.errorhandler(SQLAlchemyError)
    def database_error(exc):
        current_app.logger.exception("LT planning database operation failed")
        message = "Planning history is unavailable. Check the database connection and run the LT planning setup migration."
        if request.method == "POST":
            return jsonify(error=message), 503
        return render_template("lt_planning.html", db_error=message), 503

    @bp.get("/lt_planning")
    @bp.get("/planning")
    def workspace():
        so, ledger, pods, loaded = data(request.args.get("reload") == "1")
        status = request.args.get("status", "needs_material_review")
        status = {"needs_review": "needs_material_review", "reviewed": "needs_production_planning", "all": "needs_material_review"}.get(status, status)
        if request.args.get("assigned") == "1":
            status = "planning_complete"
        if status not in {"needs_material_review", "needs_production_planning", "planning_complete"}:
            status = "needs_material_review"
        query = request.args.get("q", "").strip()
        archived = status == "planning_complete"
        selected = request.args.get("so", "").strip()
        with engine.connect() as conn:
            reviews = conn.execute(text("SELECT DISTINCT ON (qb_num) * FROM public.mrp_review_history ORDER BY qb_num, assessed_at DESC, id DESC")).mappings().all()
            plans = conn.execute(text("SELECT DISTINCT ON (qb_num) * FROM public.lt_planning_history ORDER BY qb_num, assigned_at DESC, id DESC")).mappings().all()
            review_by_so = {r["qb_num"]: dict(r) for r in reviews}
            plan_by_so = {r["qb_num"]: dict(r) for r in plans}
            excluded = set()
            for record in conn.execute(text('''SELECT sales_order, latest_snapshot, previous_snapshot, approved_snapshot,
                    metadata_fields, items FROM public.approved_work_orders''')).mappings().all():
                snapshot = record['latest_snapshot'] or record['previous_snapshot'] or record['approved_snapshot'] or dict(
                    record['metadata_fields'] or {}, items=record['items'])
                if is_drop_ship_order(snapshot):
                    excluded.add(record['sales_order'])
            if 'Inventory Site' in so:
                sites = so['Inventory Site'].fillna('').astype(str).str.strip().str.casefold()
                excluded.update(so.loc[sites.eq('drop ship'), 'QB Num'].astype(str).str.strip())
            if selected in excluded:
                selected = ''
            staged_keys = set(conn.execute(text("""SELECT sales_order FROM public.approved_work_orders
                WHERE google_present AND (NOT is_active OR approved_at IS NULL
                    OR CAST(approved_snapshot AS jsonb) IS DISTINCT FROM CAST(latest_snapshot AS jsonb))""")).scalars())
            history = []
            planning_history = []
            selected_content = None
            if selected:
                selected_content = content_context(conn.execute(text('SELECT * FROM public.approved_work_orders WHERE sales_order=:so'), {'so': selected}).mappings().first())
                history = conn.execute(text("SELECT * FROM public.mrp_review_history WHERE qb_num=:so ORDER BY assessed_at DESC, id DESC"), {"so": selected}).mappings().all()
                planning_history = conn.execute(text("SELECT * FROM public.lt_planning_history WHERE qb_num=:so ORDER BY assigned_at DESC, id DESC"), {"so": selected}).mappings().all()
        unassigned = unassigned_orders(so)
        unassigned_keys = set(unassigned["QB Num"])
        current_keys = set(so["QB Num"].fillna("").astype(str).str.strip())
        keys = (unassigned_keys | staged_keys | set(review_by_so) | set(plan_by_so)) - excluded
        all_rows = []
        for key in sorted(keys):
            group = so.loc[so["QB Num"].fillna("").astype(str).str.strip().eq(key)]
            names = group.get("Name", pd.Series(dtype=str)).dropna().astype(str)
            review = review_by_so.get(key)
            plan = plan_by_so.get(key)
            complete = bool(plan and plan['planned_production_start'])
            # A confirmed production date completes MRP planning. Later changes
            # to that production date belong to MES, not a separate MRP page.
            stale = False
            fingerprint, suggestions = '', []
            if review and not complete and key in current_keys:
                fingerprint, suggestions = material_context(so, ledger, pods, key)
                stale = review["source_fingerprint"] != fingerprint
            stage = "planning_complete" if complete else ("needs_material_review" if not review else "needs_production_planning")
            if key not in current_keys and not complete:
                continue
            row = {"so": key, "customer": names.iloc[0] if len(names) else "", "review": review,
                   "plan": plan, "ship_date": displayed_ship_date(group), "stale": stale, "status": stage,
                   "current_blockers": current_blockers(suggestions, review),
                   "assignment": {"so": key, "fingerprint": fingerprint,
                       "review_id": str(review['id']) if review else '',
                       "plan_id": str(plan['id']) if plan else '', "submission_id": str(uuid4())}}
            all_rows.append(row)
        counts = {stage: sum(r["status"] == stage for r in all_rows)
                  for stage in ("needs_material_review", "needs_production_planning", "planning_complete")}
        rows = [r for r in all_rows if r["status"] == status
                and (not query or query.casefold() in (r["so"] + " " + r["customer"]).casefold())]
        detail = None
        if selected and not plan_by_so.get(selected):
            group = so.loc[so["QB Num"].fillna("").astype(str).str.strip().eq(selected)]
            if not group.empty or selected in review_by_so or selected in plan_by_so:
                fingerprint, suggestions = material_context(so, ledger, pods, selected) if not group.empty else ("", [])
                review = review_by_so.get(selected)
                detail = {"so": selected, "fingerprint": fingerprint, "suggestions": suggestions,
                          "review": review, "plan": plan_by_so.get(selected),
                          "stale": bool(review and review["source_fingerprint"] != fingerprint),
                          "eligible": not group.empty, "ship_date": displayed_ship_date(group),
                          "content": selected_content}
        return render_template("lt_planning.html", rows=rows, counts=counts, status=status,
                               query=query, archived=archived, detail=detail, history=history,
                               planning_history=planning_history, loaded=loaded, reasons=REASONS,
                               csrf_token=csrf(), review_submission=str(uuid4()), plan_submission=str(uuid4()),
                               stage_labels={"needs_material_review": "Needs Material Review", "needs_production_planning": "Needs Production Planning", "planning_complete": "Planning Complete"})

    @bp.post("/lt_planning/<action>")
    def save(action):
        if action not in {"review", "assign"}:
            abort(404)
        if not secrets.compare_digest(request.headers.get("X-CSRF-Token", ""), csrf()):
            return jsonify(error="Session expired. Reload the page and try again."), 403
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            return jsonify(error="A JSON form is required."), 400
        try:
            submission = str(UUID(str(payload.get("submission_id", ""))))
            qb_num = str(payload.get("so", "")).strip()
            # Neither manual action requires operator identity. Keep neutral markers
            # for compatibility with the existing NOT NULL history columns.
            actor = "Manual review" if action == "review" else "Manual assignment"
            notes = str(payload.get("notes", "")).strip()
            if not qb_num or len(notes) > 4000:
                raise ValueError("An SO is required. Notes must be under 4,000 characters.")
            # Refresh on submission; never approve an out-of-date cached calculation.
            so, ledger, pods, loaded = data(True)
            group = so.loc[so["QB Num"].fillna("").astype(str).str.strip().eq(qb_num)]
            if group.empty:
                raise ValueError("This SO is no longer in the current MRP data. Reload the workspace.")
            fingerprint, suggestions = material_context(so, ledger, pods, qb_num)
            if payload.get("fingerprint") != fingerprint:
                raise ValueError("MRP data changed since this form was opened. Reload and review again.")
            table = "mrp_review_history" if action == "review" else "lt_planning_history"
            with engine.begin() as conn:
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:so, 0))"), {"so": qb_num})
                duplicate = conn.execute(text(f"SELECT qb_num FROM public.{table} WHERE id=CAST(:id AS uuid)"), {"id": submission}).scalar_one_or_none()
                if duplicate is not None:
                    if duplicate != qb_num:
                        raise ValueError("Invalid submission ID. Reload the page.")
                    return jsonify(ok=True, so=qb_num)
                review = latest(conn, "mrp_review_history", qb_num)
                if 'notes' not in payload and review:
                    notes = str(review.get('notes') or '')
                    if len(notes) > 4000:
                        raise ValueError('Notes must be under 4,000 characters.')
                expected = str(payload.get("review_id") or "")
                if expected != (str(review["id"]) if review else ""):
                    raise ValueError("Another review was saved. Reload to use the latest assessment.")
                if action == "review":
                    completed_plan = latest(conn, 'lt_planning_history', qb_num)
                    if completed_plan and completed_plan['planned_production_start']:
                        raise ValueError('Planning is already complete. Adjust production dates in MES.')
                    no_blockers = payload.get("no_blockers") is True
                    requirements = {r["item"]: r["required_qty"] for r in suggestions}
                    if not requirements:
                        raise ValueError("No positive component requirements are available. Check the SO data.")
                    blockers = validate_blockers(payload.get("blockers", []), no_blockers, requirements)
                    review_content(conn, qb_num, payload.get('order_content') or {})
                    conn.execute(text("""INSERT INTO public.mrp_review_history
                        (id, qb_num, run_id, source_fingerprint, reviewed_by, no_blockers, blockers, system_suggestions, notes)
                        VALUES (CAST(:id AS uuid), :so, :run, :fingerprint, :actor, :none,
                                CAST(:blockers AS jsonb), CAST(:suggestions AS jsonb), :notes)"""),
                        {"id": submission, "so": qb_num, "run": f"manual-{submission}", "fingerprint": fingerprint,
                         "actor": actor, "none": no_blockers, "blockers": json.dumps(blockers),
                         "suggestions": json.dumps(suggestions), "notes": notes})
                else:
                    if not review:
                        raise ValueError("Approve a material review before saving the production plan.")
                    if review["source_fingerprint"] != fingerprint:
                        if payload.get('material_update_approved') is not True:
                            raise ValueError('Review the current material status and confirm it in the assignment column.')
                        if any(r.get('warning') for r in suggestions):
                            raise ValueError('Material data is incomplete. Correct it before confirming an updated assessment.')
                        blockers = current_blockers(suggestions, review)
                        refresh_id = str(uuid5(NAMESPACE_URL, 'assignment-review:' + submission))
                        conn.execute(text('''INSERT INTO public.mrp_review_history
                            (id, qb_num, run_id, source_fingerprint, reviewed_by, no_blockers, blockers, system_suggestions, notes)
                            VALUES (CAST(:id AS uuid), :so, :run, :fingerprint, 'Manual review', :none,
                                    CAST(:blockers AS jsonb), CAST(:suggestions AS jsonb), :notes)'''),
                            {'id': refresh_id, 'so': qb_num, 'run': 'manual-' + refresh_id,
                             'fingerprint': fingerprint, 'none': not blockers, 'blockers': json.dumps(blockers),
                             'suggestions': json.dumps(suggestions), 'notes': notes})
                        review = {**dict(review), 'id': refresh_id, 'blockers': blockers}
                    current_plan = latest(conn, "lt_planning_history", qb_num)
                    if current_plan and current_plan['planned_production_start']:
                        raise ValueError('Planning is already complete. Adjust production dates in MES.')
                    if str(payload.get("plan_id") or "") != (str(current_plan["id"]) if current_plan else ""):
                        raise ValueError("Another production plan was saved. Reload before revising it.")
                    start = date.fromisoformat(str(payload.get("production_start", "")))
                    if start < date.today() or start.weekday() >= 5:
                        raise ValueError("MES production dates must be today or a future weekday.")
                    ship = source_ship_date(group)
                    if ship is not None and start > ship:
                        raise ValueError("Production start must not follow the ship date imported by MRP CLI.")
                    override = payload.get("blocker_override") is True
                    if review["blockers"] and (not override or not notes):
                        raise ValueError("Blockers remain. Explicitly acknowledge the override and explain it in notes.")
                    if payload.get("approved") is not True:
                        raise ValueError("Confirm the planning decision before saving.")
                    # This final MRP confirmation is the publication boundary.
                    # MES needs no prior eligibility or approval step.
                    publish_to_mes(conn, qb_num, notes)
                    # Shared schedule is the destination confirmed by MES models and
                    # production_schedule.save_production_date. Preserve its semantics.
                    conn.execute(text("""INSERT INTO public.production_overrides
                        (wo_number, production_date, is_finished_goods, schedule_updated_at, schedule_updated_by,
                         finished_goods_updated_at, finished_goods_updated_by)
                        VALUES (:so, :start, FALSE, CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'MRP Planning', NULL, NULL)
                        ON CONFLICT (wo_number) DO UPDATE SET production_date=EXCLUDED.production_date,
                            is_finished_goods=FALSE, schedule_updated_at=EXCLUDED.schedule_updated_at,
                            schedule_updated_by=EXCLUDED.schedule_updated_by,
                            finished_goods_updated_at=NULL, finished_goods_updated_by=NULL"""), {"so": qb_num, "start": start})
                    conn.execute(text("""INSERT INTO public.lt_planning_history
                        (id, qb_num, review_id, planned_production_start, assigned_ship_date, assigned_by, blocker_override, notes, mes_sent_at)
                        VALUES (CAST(:id AS uuid), :so, CAST(:review AS uuid), :start, :ship, :actor, :override, :notes, CURRENT_TIMESTAMP)"""),
                        {"id": submission, "so": qb_num, "review": str(review["id"]), "start": start, "ship": ship,
                         "actor": actor, "override": bool(review["blockers"]) and override, "notes": notes})
            return jsonify(ok=True, so=qb_num)
        except (ValueError, TypeError, KeyError) as exc:
            return jsonify(error=str(exc)), 400

    return bp
