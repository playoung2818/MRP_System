"""Legacy review bookmarks now lead to the consolidated Planning workspace."""
from flask import Blueprint, jsonify, redirect, url_for


def create_so_review(engine):
    bp = Blueprint('so_review', __name__)

    @bp.record_once
    def configure(state):
        # Compatibility for the moved SO-review service; no DDL is performed.
        state.app.extensions['so_review_engine'] = engine

    @bp.get('/sales_orders')
    def so_review_list():
        return redirect(url_for('lt_planning.workspace', status='needs_material_review'))

    @bp.get('/sales_orders/<path:sales_order>')
    def so_review_detail(sales_order):
        return redirect(url_for('lt_planning.workspace', status='needs_material_review', so=sales_order))

    @bp.post('/sales_orders/<path:sales_order>')
    def legacy_review_post(sales_order):
        return jsonify(error='Review SO content and materials together in Planning. Reload the new combined form.'), 410

    return bp
