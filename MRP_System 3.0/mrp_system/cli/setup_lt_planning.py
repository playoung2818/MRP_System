"""Create the two approved LT planning tables (explicit migration, no ETL DDL)."""
from pathlib import Path
from mrp_system.runtime.db_config import get_engine


def main():
    engine = get_engine()
    if engine.dialect.name != "postgresql":
        raise RuntimeError("LT planning history requires PostgreSQL.")
    sql = (Path(__file__).resolve().parents[1] / "ledger" / "lt_planning_schema.sql").read_text(encoding="utf-8")
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql(sql)
        print("LT planning tables are ready: mrp_review_history, lt_planning_history")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
