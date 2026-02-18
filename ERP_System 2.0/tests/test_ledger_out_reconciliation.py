from __future__ import annotations

from pathlib import Path
import sys
import json
from uuid import uuid4

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_config import get_engine  # noqa: E402
from core import _norm_key  # noqa: E402


EPS = 1e-9
SCHEMA = "public"
STRUCTURED_TABLE = "wo_structured"
LEDGER_TABLE = "ledger_analytics"
RUN_LOG_TABLE = "qa_ledger_out_recon_runs"
DETAIL_LOG_TABLE = "qa_ledger_out_recon_details"


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _short(df: pd.DataFrame, n: int = 20) -> str:
    if df.empty:
        return "(none)"
    return df.head(n).to_string(index=False)


def _to_jsonable(val):
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val


def _write_log_to_db(engine, report: dict[str, pd.DataFrame], checks_passed: bool) -> str:
    run_id = str(uuid4())
    created_at = pd.Timestamp.utcnow()

    summary = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "created_at_utc": created_at,
                "passed": bool(checks_passed),
                "structured_out_invalid_ship_date_count": int(len(report["structured_out_invalid_ship_date"])),
                "ledger_out_nonnegative_delta_count": int(len(report["ledger_out_nonnegative_delta"])),
                "out_missing_or_short_item_date_count": int(len(report["out_missing_or_short_item_date"])),
                "out_extra_item_date_count": int(len(report["out_extra_item_date"])),
                "out_mismatch_qb_item_date_count": int(len(report["out_mismatch_qb_item_date"])),
            }
        ]
    )
    summary.to_sql(RUN_LOG_TABLE, engine, schema=SCHEMA, if_exists="append", index=False, method="multi")

    # Ensure detail table exists even when there are no discrepancy rows.
    pd.DataFrame(
        columns=["run_id", "created_at_utc", "bucket", "row_no", "row_json"]
    ).to_sql(DETAIL_LOG_TABLE, engine, schema=SCHEMA, if_exists="append", index=False, method="multi")

    detail_rows: list[dict] = []
    for bucket, df in report.items():
        if df.empty:
            continue
        for idx, row in df.reset_index(drop=True).iterrows():
            payload = {str(k): _to_jsonable(v) for k, v in row.items()}
            detail_rows.append(
                {
                    "run_id": run_id,
                    "created_at_utc": created_at,
                    "bucket": bucket,
                    "row_no": int(idx + 1),
                    "row_json": json.dumps(payload, ensure_ascii=True, default=str),
                }
            )

    if detail_rows:
        pd.DataFrame(detail_rows).to_sql(
            DETAIL_LOG_TABLE, engine, schema=SCHEMA, if_exists="append", index=False, method="multi"
        )
    return run_id


def _build_out_report(engine) -> dict[str, pd.DataFrame]:
    so = pd.read_sql(
        f"""
        SELECT
            "Ship Date" AS ship_date_raw,
            "Item" AS item,
            "Qty(-)" AS qty,
            "QB Num" AS qb_num
        FROM {_qident(SCHEMA)}.{_qident(STRUCTURED_TABLE)}
        """,
        engine,
    )
    so["ship_date"] = pd.to_datetime(so["ship_date_raw"], errors="coerce").dt.normalize()
    so["qty"] = pd.to_numeric(so["qty"], errors="coerce").fillna(0.0)
    so["item_key"] = _norm_key(so["item"])
    so["qb_num"] = so["qb_num"].fillna("").astype(str).str.strip().str.upper()
    so = so.loc[so["qty"] > 0].copy()
    so_invalid_ship = so.loc[so["ship_date"].isna()].copy()
    so_valid = so.loc[so["ship_date"].notna()].copy()

    led = pd.read_sql(
        f"""
        SELECT
            "Date" AS event_date,
            "Item" AS item,
            "Delta" AS delta,
            "Kind" AS kind,
            "Source" AS source,
            "QB Num" AS qb_num
        FROM {_qident(SCHEMA)}.{_qident(LEDGER_TABLE)}
        WHERE "Kind" = 'OUT' AND "Source" = 'SO'
        """,
        engine,
    )
    led["event_date"] = pd.to_datetime(led["event_date"], errors="coerce").dt.normalize()
    led["delta"] = pd.to_numeric(led["delta"], errors="coerce").fillna(0.0)
    led["qty"] = -led["delta"]
    led["item_key"] = _norm_key(led["item"])
    led["qb_num"] = led["qb_num"].fillna("").astype(str).str.strip().str.upper()
    led_nonnegative = led.loc[led["delta"] >= 0].copy()
    led = led.loc[led["event_date"].notna() & (led["qty"] > 0)].copy()

    expected_item_date = (
        so_valid.groupby(["item_key", "ship_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "expected_out_qty"})
    )
    actual_item_date = (
        led.groupby(["item_key", "event_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"event_date": "ship_date", "qty": "ledger_out_qty"})
    )
    cmp_item_date = expected_item_date.merge(actual_item_date, on=["item_key", "ship_date"], how="outer")
    cmp_item_date["expected_out_qty"] = pd.to_numeric(cmp_item_date["expected_out_qty"], errors="coerce")
    cmp_item_date["ledger_out_qty"] = pd.to_numeric(cmp_item_date["ledger_out_qty"], errors="coerce")
    cmp_item_date["gap"] = cmp_item_date["ledger_out_qty"].fillna(0) - cmp_item_date["expected_out_qty"].fillna(0)

    out_missing_or_short_item_date = cmp_item_date.loc[
        cmp_item_date["expected_out_qty"].notna()
        & (
            cmp_item_date["ledger_out_qty"].isna()
            | (cmp_item_date["ledger_out_qty"] + EPS < cmp_item_date["expected_out_qty"])
        )
    ].sort_values(["ship_date", "item_key"])

    out_extra_item_date = cmp_item_date.loc[
        cmp_item_date["ledger_out_qty"].notna()
        & (
            cmp_item_date["expected_out_qty"].isna()
            | (cmp_item_date["ledger_out_qty"] - EPS > cmp_item_date["expected_out_qty"])
        )
    ].sort_values(["ship_date", "item_key"])

    expected_qb_item_date = (
        so_valid.groupby(["qb_num", "item_key", "ship_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "expected_out_qty"})
    )
    actual_qb_item_date = (
        led.groupby(["qb_num", "item_key", "event_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"event_date": "ship_date", "qty": "ledger_out_qty"})
    )
    cmp_qb_item_date = expected_qb_item_date.merge(
        actual_qb_item_date, on=["qb_num", "item_key", "ship_date"], how="outer"
    )
    cmp_qb_item_date["expected_out_qty"] = pd.to_numeric(cmp_qb_item_date["expected_out_qty"], errors="coerce")
    cmp_qb_item_date["ledger_out_qty"] = pd.to_numeric(cmp_qb_item_date["ledger_out_qty"], errors="coerce")
    cmp_qb_item_date["gap"] = cmp_qb_item_date["ledger_out_qty"].fillna(0) - cmp_qb_item_date["expected_out_qty"].fillna(0)
    out_mismatch_qb_item_date = cmp_qb_item_date.loc[
        cmp_qb_item_date["expected_out_qty"].isna()
        | cmp_qb_item_date["ledger_out_qty"].isna()
        | (cmp_qb_item_date["gap"].abs() > EPS)
    ].sort_values(["ship_date", "qb_num", "item_key"])

    return {
        "structured_out_invalid_ship_date": so_invalid_ship.sort_values(["qb_num", "item_key"]),
        "ledger_out_nonnegative_delta": led_nonnegative.sort_values(["event_date", "qb_num", "item_key"]),
        "out_missing_or_short_item_date": out_missing_or_short_item_date,
        "out_extra_item_date": out_extra_item_date,
        "out_mismatch_qb_item_date": out_mismatch_qb_item_date,
    }


def test_ledger_out_reconciliation() -> None:
    engine = get_engine()
    report = _build_out_report(engine)

    checks_passed = (
        report["ledger_out_nonnegative_delta"].empty
        and report["out_missing_or_short_item_date"].empty
        and report["out_extra_item_date"].empty
        and report["out_mismatch_qb_item_date"].empty
    )
    run_id = _write_log_to_db(engine, report, checks_passed)

    print("\n[1] Structured OUT rows with invalid Ship Date")
    print(_short(report["structured_out_invalid_ship_date"]))
    print("\n[2] Ledger OUT rows with non-negative delta (should be negative)")
    print(_short(report["ledger_out_nonnegative_delta"]))
    print("\n[3] OUT missing/short by item+date")
    print(_short(report["out_missing_or_short_item_date"]))
    print("\n[4] OUT extra by item+date")
    print(_short(report["out_extra_item_date"]))
    print("\n[5] OUT mismatch by QB+item+date")
    print(_short(report["out_mismatch_qb_item_date"]))
    print(f"\n[LOG] Supabase run_id: {run_id} (tables: {SCHEMA}.{RUN_LOG_TABLE}, {SCHEMA}.{DETAIL_LOG_TABLE})")

    assert report["ledger_out_nonnegative_delta"].empty, (
        "Found ledger OUT rows with non-negative delta."
    )
    assert report["out_missing_or_short_item_date"].empty, (
        "Some structured OUT demand is missing/short in ledger OUT (item+date)."
    )
    assert report["out_extra_item_date"].empty, (
        "Some ledger OUT rows are extra vs structured demand (item+date)."
    )
    assert report["out_mismatch_qb_item_date"].empty, (
        "Mismatch between structured and ledger OUT at QB+item+date level."
    )
