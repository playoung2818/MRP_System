"""Unified production overrides, with a one-time legacy-table migration."""

from sqlalchemy import inspect, text


TABLE = "production_overrides"


def ensure_table(engine) -> None:
    with engine.begin() as conn:
        if engine.dialect.name == "postgresql":
            # Serialize initialization across web workers, including migration.
            conn.execute(text("SELECT pg_advisory_xact_lock(735102941)"))
        if inspect(conn).has_table(TABLE, schema="public"):
            return
        conn.execute(text("""
            CREATE TABLE public.production_overrides (
                wo_number TEXT PRIMARY KEY,
                production_date DATE,
                is_finished_goods BOOLEAN NOT NULL DEFAULT FALSE,
                schedule_updated_at TIMESTAMP,
                schedule_updated_by TEXT,
                finished_goods_updated_at TIMESTAMP,
                finished_goods_updated_by TEXT
            )
        """))
        if inspect(conn).has_table("production_schedule_overrides", schema="public"):
            conn.execute(text("""
                INSERT INTO public.production_overrides
                    (wo_number, production_date, schedule_updated_at, schedule_updated_by)
                SELECT wo_number, production_date, updated_at, updated_by
                FROM public.production_schedule_overrides
            """))
        if inspect(conn).has_table("production_finished_goods_overrides", schema="public"):
            conn.execute(text("""
                INSERT INTO public.production_overrides
                    (wo_number, is_finished_goods,
                     finished_goods_updated_at, finished_goods_updated_by)
                SELECT wo_number, TRUE, updated_at, updated_by
                FROM public.production_finished_goods_overrides
                WHERE TRUE
                ON CONFLICT (wo_number) DO UPDATE SET
                    is_finished_goods = TRUE,
                    finished_goods_updated_at = EXCLUDED.finished_goods_updated_at,
                    finished_goods_updated_by = EXCLUDED.finished_goods_updated_by
            """))


def load_schedule(engine) -> dict[str, str]:
    ensure_table(engine)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT wo_number, production_date FROM public.production_overrides
            WHERE production_date IS NOT NULL
        """))
        return {row.wo_number.strip(): str(row.production_date)[:10] for row in rows}


def load_finished_goods(engine) -> set[str]:
    ensure_table(engine)
    with engine.connect() as conn:
        return {row.wo_number.strip() for row in conn.execute(text("""
            SELECT wo_number FROM public.production_overrides WHERE is_finished_goods
        """))}


def save_schedule(engine, wo_number, production_date, updated_by=None) -> None:
    ensure_table(engine)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO public.production_overrides
                (wo_number, production_date, schedule_updated_at, schedule_updated_by)
            VALUES (:wo_number, CAST(:production_date AS DATE), CURRENT_TIMESTAMP, :updated_by)
            ON CONFLICT (wo_number) DO UPDATE SET
                production_date = EXCLUDED.production_date,
                schedule_updated_at = CURRENT_TIMESTAMP,
                schedule_updated_by = EXCLUDED.schedule_updated_by,
                is_finished_goods = FALSE,
                finished_goods_updated_at = NULL,
                finished_goods_updated_by = NULL
        """), dict(wo_number=wo_number, production_date=production_date,
                    updated_by=updated_by or None))


def save_finished_goods(engine, wo_number, updated_by=None) -> None:
    ensure_table(engine)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO public.production_overrides
                (wo_number, is_finished_goods, finished_goods_updated_at, finished_goods_updated_by)
            VALUES (:wo_number, TRUE, CURRENT_TIMESTAMP, :updated_by)
            ON CONFLICT (wo_number) DO UPDATE SET
                is_finished_goods = TRUE,
                finished_goods_updated_at = CURRENT_TIMESTAMP,
                finished_goods_updated_by = EXCLUDED.finished_goods_updated_by
        """), dict(wo_number=wo_number, updated_by=updated_by or None))
