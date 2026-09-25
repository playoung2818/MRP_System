"""Run directly to check migration in an isolated, rolled-back PostgreSQL schema."""

import sys
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import inspect, text
from erp_system import production_overrides as overrides
from erp_system.runtime.db_config import get_engine


@unittest.skipUnless(__name__ == "__main__", "Run this database integration check directly")
class ProductionOverridesTest(unittest.TestCase):
    def test_migration_and_state_transitions(self):
        engine = get_engine(connect_args={"connect_timeout": 8})
        schema = "test_overrides_" + uuid.uuid4().hex
        with engine.connect() as conn:
            transaction = conn.begin()
            try:
                conn.execute(text(f'CREATE SCHEMA "{schema}"'))

                def scoped_text(sql):
                    return text(sql.replace("public.", f'"{schema}".'))

                class ScopedInspector:
                    def has_table(self, name, schema=None):
                        return inspect(conn).has_table(name, schema=test_schema)

                test_schema = schema

                class ScopedEngine:
                    dialect = engine.dialect

                    @contextmanager
                    def begin(self):
                        yield conn

                    connect = begin

                scoped_engine = ScopedEngine()
                for table, extra in [
                    ("production_schedule_overrides", "production_date DATE NOT NULL,"),
                    ("production_finished_goods_overrides", ""),
                ]:
                    conn.execute(scoped_text(f"""
                        CREATE TABLE public.{table} (
                            wo_number TEXT PRIMARY KEY, {extra}
                            updated_at TIMESTAMP, updated_by TEXT
                        )
                    """))
                conn.execute(scoped_text("""
                    INSERT INTO public.production_schedule_overrides VALUES
                    ('both', '2026-10-01', '2026-09-01', 'scheduler'),
                    ('schedule', '2026-10-02', '2026-09-02', 'scheduler')
                """))
                conn.execute(scoped_text("""
                    INSERT INTO public.production_finished_goods_overrides VALUES
                    ('both', '2026-09-03', 'finisher'),
                    ('finished', '2026-09-04', 'finisher')
                """))
                with patch.object(overrides, "text", scoped_text), patch.object(
                    overrides, "inspect", lambda _: ScopedInspector()
                ):
                    overrides.ensure_table(scoped_engine)
                    self.assertEqual(overrides.load_schedule(scoped_engine), {
                        "both": "2026-10-01", "schedule": "2026-10-02",
                    })
                    self.assertEqual(overrides.load_finished_goods(scoped_engine), {"both", "finished"})
                    row = conn.execute(scoped_text("""
                        SELECT * FROM public.production_overrides WHERE wo_number = 'both'
                    """)).mappings().one()
                    self.assertEqual(row["schedule_updated_by"], "scheduler")
                    self.assertEqual(row["finished_goods_updated_by"], "finisher")
                    self.assertEqual(str(row["schedule_updated_at"]), "2026-09-01 00:00:00")
                    self.assertEqual(str(row["finished_goods_updated_at"]), "2026-09-03 00:00:00")
                    overrides.save_schedule(scoped_engine, "both", "2026-10-05", "rescheduler")
                    overrides.ensure_table(scoped_engine)
                    self.assertNotIn("both", overrides.load_finished_goods(scoped_engine))
                    self.assertEqual(overrides.load_schedule(scoped_engine)["both"], "2026-10-05")
                    overrides.save_finished_goods(scoped_engine, "both", "finisher2")
                    self.assertEqual(overrides.load_schedule(scoped_engine)["both"], "2026-10-05")
                    overrides.save_finished_goods(scoped_engine, "new-finished")
                    self.assertNotIn("new-finished", overrides.load_schedule(scoped_engine))
                    overrides.save_schedule(scoped_engine, "new-schedule", "2026-10-06")
                    self.assertNotIn("new-schedule", overrides.load_finished_goods(scoped_engine))
                    self.assertEqual(conn.execute(scoped_text(
                        "SELECT count(*) FROM public.production_overrides"
                    )).scalar_one(), 5)
            finally:
                transaction.rollback()
        engine.dispose()


if __name__ == "__main__":
    unittest.main()
