"""Refresh independent per-SO material-readiness assessments from the ledger."""

from pathlib import Path
from sqlalchemy import text
from erp_system.runtime.db_config import get_engine

QUERY = Path(__file__).with_suffix(".sql").read_text(encoding="utf-8")


def refresh_material_readiness():
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            conn.execute(text("SET LOCAL statement_timeout = '120s'"))
            conn.execute(text("SELECT pg_advisory_xact_lock(735102942)"))
            invalid = conn.execute(text('''
                SELECT count(*) FROM public.ledger_analytics
                WHERE "Date" IS NULL AND coalesce("Delta", 0) <> 0
            ''')).scalar_one()
            inconsistent = conn.execute(text('''
                SELECT count(*) FROM (
                    SELECT upper(btrim("Item")) FROM public.ledger_analytics
                    GROUP BY 1 HAVING count(DISTINCT "Opening") > 1
                ) inconsistent
            ''')).scalar_one()
            if invalid or inconsistent:
                raise ValueError("Readiness requires dated ledger movements and consistent opening stock per item.")
            conn.execute(text('''CREATE TABLE IF NOT EXISTS public.so_material_readiness (
                "QB Num" TEXT PRIMARY KEY, customer TEXT, item_count INTEGER NOT NULL,
                earliest_material_ready_date DATE, blocking_item_count INTEGER NOT NULL,
                blocking_items TEXT NOT NULL, component_details JSONB NOT NULL,
                can_assign BOOLEAN NOT NULL, ready_today BOOLEAN NOT NULL,
                as_of_date DATE NOT NULL, calculated_at TIMESTAMPTZ NOT NULL
            )'''))
            # Build the whole replacement before touching the existing snapshot.
            conn.execute(text("CREATE TEMP TABLE readiness_refresh ON COMMIT DROP AS " + QUERY))
            conn.execute(text('''
                DO $$ BEGIN
                    IF EXISTS (SELECT 1 FROM readiness_refresh WHERE
                        (earliest_material_ready_date IS NOT NULL AND blocking_item_count > 0)
                        OR earliest_material_ready_date < as_of_date) THEN
                        RAISE EXCEPTION 'Invalid material readiness results';
                    END IF;
                END $$
            '''))
            conn.execute(text("DELETE FROM public.so_material_readiness"))
            conn.execute(text("INSERT INTO public.so_material_readiness SELECT * FROM readiness_refresh"))
            conn.execute(text("COMMENT ON TABLE public.so_material_readiness IS 'Independent per-SO snapshot. Own demand removed; all other demand retained. Assignment follows same-day movements. Unknown-date inbound supply excluded. Dates are not simultaneous reservations. Refreshed after ETL or with python -m erp_system.ledger.material_readiness.'"))
            result = dict(conn.execute(text('''SELECT count(*) AS sales_orders,
                count(*) FILTER (WHERE can_assign) AS can_assign,
                count(*) FILTER (WHERE ready_today) AS ready_today,
                count(*) FILTER (WHERE NOT can_assign) AS blocked
                FROM public.so_material_readiness''')).mappings().one())
        return result
    finally:
        engine.dispose()


if __name__ == "__main__":
    print(refresh_material_readiness())
