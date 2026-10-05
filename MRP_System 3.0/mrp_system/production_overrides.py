"""Read-only access to saved production overrides; never create or migrate tables."""

from sqlalchemy import inspect, text


def load_schedule(engine) -> dict[str, str]:
    with engine.connect() as conn:
        if engine.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
        schema = inspect(conn)
        if schema.has_table("production_overrides", schema="public"):
            query = "SELECT wo_number, production_date FROM public.production_overrides WHERE production_date IS NOT NULL"
        elif schema.has_table("production_schedule_overrides", schema="public"):
            query = "SELECT wo_number, production_date FROM public.production_schedule_overrides"
        else:
            return {}
        return {row.wo_number.strip(): str(row.production_date)[:10]
                for row in conn.execute(text(query)) if row.production_date is not None}


def load_finished_goods(engine) -> set[str]:
    with engine.connect() as conn:
        if engine.dialect.name == "postgresql":
            conn.execute(text("SET TRANSACTION READ ONLY"))
        schema = inspect(conn)
        if schema.has_table("production_overrides", schema="public"):
            query = "SELECT wo_number FROM public.production_overrides WHERE is_finished_goods"
        elif schema.has_table("production_finished_goods_overrides", schema="public"):
            query = "SELECT wo_number FROM public.production_finished_goods_overrides"
        else:
            return set()
        return {row.wo_number.strip() for row in conn.execute(text(query))}
