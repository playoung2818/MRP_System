from __future__ import annotations

from pathlib import Path
import sys
import json
from uuid import uuid4

import pandas as pd


# Allow running this file directly: python tests/test_pod_vs_ledger_in.py
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db_config import get_engine  # noqa: E402
from core import _norm_key  # noqa: E402
from ledger import expand_nav_preinstalled  # noqa: E402


EPS = 1e-9
SCHEMA = "public"
POD_TABLE = "Open_Purchase_Orders"
LEDGER_TABLE = "ledger_analytics"
SHIP_TABLE = "NT Shipping Schedule"
EXCLUDE_VENDOR = "Neousys Technology Incorp."
RUN_LOG_TABLE = "qa_inbound_recon_runs"
DETAIL_LOG_TABLE = "qa_inbound_recon_details"


def _qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _load_pod(engine) -> pd.DataFrame:
    q = f"""
    SELECT
        "POD#" AS pod_no,
        "Item" AS item,
        "Ship Date" AS ship_date,
        "Qty(+)" AS qty,
        "Source Name" AS source_name
    FROM {_qident(SCHEMA)}.{_qident(POD_TABLE)}
    """
    pod = pd.read_sql(q, engine)
    pod["item_key"] = _norm_key(pod["item"])
    pod["ship_date"] = pd.to_datetime(pod["ship_date"], errors="coerce").dt.normalize()
    pod["qty"] = pd.to_numeric(pod["qty"], errors="coerce").fillna(0.0)
    pod["pod_no"] = pod["pod_no"].fillna("").astype(str).str.strip()
    pod["source_name"] = pod["source_name"].fillna("").astype(str).str.strip()

    pod = pod.loc[pod["qty"] > 0].copy()
    pod = pod.loc[pod["source_name"].ne(EXCLUDE_VENDOR)].copy()
    return pod


def _load_shipping(engine) -> pd.DataFrame:
    q = f"""SELECT * FROM {_qident(SCHEMA)}.{_qident(SHIP_TABLE)}"""
    return pd.read_sql(q, engine)


def _load_ledger_in(engine) -> pd.DataFrame:
    q = f"""
    SELECT
        "Date" AS event_date,
        "Item" AS item,
        "Delta" AS qty,
        "Source" AS source
    FROM {_qident(SCHEMA)}.{_qident(LEDGER_TABLE)}
    WHERE "Kind" = 'IN'
    """
    led = pd.read_sql(q, engine)
    led["item_key"] = _norm_key(led["item"])
    led["event_date"] = pd.to_datetime(led["event_date"], errors="coerce").dt.normalize()
    led["qty"] = pd.to_numeric(led["qty"], errors="coerce").fillna(0.0)
    led["source"] = led["source"].fillna("").astype(str).str.strip()
    led = led.loc[led["qty"] > 0].copy()
    return led


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
                "shipping_rows_missing_ship_date_count": int(len(report["shipping_rows_missing_ship_date"])),
                "nav_missing_or_short_vs_shipping_count": int(len(report["nav_missing_or_short_vs_shipping"])),
                "nav_extra_vs_shipping_count": int(len(report["nav_extra_vs_shipping"])),
                "null_ship_potentially_counted_in_nav_count": int(len(report["null_ship_potentially_counted_in_nav"])),
                "pod_mismatch_vs_open_purchase_orders_count": int(len(report["pod_mismatch_vs_open_purchase_orders"])),
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


def _build_discrepancy_report(engine) -> dict[str, pd.DataFrame]:
    ship = _load_shipping(engine)
    pod = _load_pod(engine)
    led_in = _load_ledger_in(engine)

    ship_expanded = expand_nav_preinstalled(ship)
    ship_expanded["item_key"] = _norm_key(ship_expanded["Item"])
    ship_expanded["ship_date"] = pd.to_datetime(ship_expanded["Date"], errors="coerce").dt.normalize()
    ship_expanded["qty"] = pd.to_numeric(ship_expanded["Qty(+)"], errors="coerce").fillna(0.0)
    ship_expanded = ship_expanded.loc[ship_expanded["qty"] > 0].copy()

    ship_no_date = ship_expanded.loc[ship_expanded["ship_date"].isna()].copy()
    ship_with_date = ship_expanded.loc[ship_expanded["ship_date"].notna()].copy()

    expected_nav = (
        ship_with_date.groupby(["item_key", "ship_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "expected_nav_qty"})
    )
    actual_nav = (
        led_in.loc[led_in["source"].eq("NAV")]
        .groupby(["item_key", "event_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"event_date": "ship_date", "qty": "ledger_nav_qty"})
    )

    nav_compare = expected_nav.merge(actual_nav, on=["item_key", "ship_date"], how="outer")
    nav_compare["expected_nav_qty"] = pd.to_numeric(nav_compare["expected_nav_qty"], errors="coerce")
    nav_compare["ledger_nav_qty"] = pd.to_numeric(nav_compare["ledger_nav_qty"], errors="coerce")
    nav_compare["gap"] = nav_compare["ledger_nav_qty"].fillna(0) - nav_compare["expected_nav_qty"].fillna(0)

    nav_missing_or_short = nav_compare.loc[
        nav_compare["expected_nav_qty"].notna()
        & (
            nav_compare["ledger_nav_qty"].isna()
            | (nav_compare["ledger_nav_qty"] + EPS < nav_compare["expected_nav_qty"])
        )
    ].sort_values(["ship_date", "item_key"])

    nav_extra = nav_compare.loc[
        nav_compare["ledger_nav_qty"].notna()
        & (
            nav_compare["expected_nav_qty"].isna()
            | (nav_compare["ledger_nav_qty"] - EPS > nav_compare["expected_nav_qty"])
        )
    ].sort_values(["ship_date", "item_key"])

    ship_with_date_item = (
        ship_with_date.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ship_with_date_qty"})
    )
    ship_no_date_item = (
        ship_no_date.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ship_no_date_qty"})
    )
    nav_item = (
        led_in.loc[led_in["source"].eq("NAV")]
        .groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ledger_nav_qty"})
    )
    null_ship_possibly_counted = (
        ship_no_date_item.merge(nav_item, on="item_key", how="inner")
        .merge(ship_with_date_item, on="item_key", how="left")
    )
    null_ship_possibly_counted["ship_with_date_qty"] = (
        pd.to_numeric(null_ship_possibly_counted["ship_with_date_qty"], errors="coerce").fillna(0.0)
    )
    null_ship_possibly_counted["unexplained_nav_qty"] = (
        null_ship_possibly_counted["ledger_nav_qty"] - null_ship_possibly_counted["ship_with_date_qty"]
    )
    null_ship_possibly_counted = null_ship_possibly_counted.loc[
        null_ship_possibly_counted["unexplained_nav_qty"] > EPS
    ].sort_values("unexplained_nav_qty", ascending=False)

    pod_with_ship = pod.loc[pod["ship_date"].notna()].copy()
    expected_pod = (
        pod_with_ship.groupby(["item_key", "ship_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "expected_pod_qty"})
    )
    actual_pod = (
        led_in.loc[led_in["source"].eq("POD")]
        .groupby(["item_key", "event_date"], as_index=False)["qty"]
        .sum()
        .rename(columns={"event_date": "ship_date", "qty": "ledger_pod_qty"})
    )
    pod_compare = expected_pod.merge(actual_pod, on=["item_key", "ship_date"], how="outer")
    pod_compare["expected_pod_qty"] = pd.to_numeric(pod_compare["expected_pod_qty"], errors="coerce")
    pod_compare["ledger_pod_qty"] = pd.to_numeric(pod_compare["ledger_pod_qty"], errors="coerce")
    pod_compare["gap"] = pod_compare["ledger_pod_qty"].fillna(0) - pod_compare["expected_pod_qty"].fillna(0)
    pod_mismatch = pod_compare.loc[
        pod_compare["expected_pod_qty"].isna()
        | pod_compare["ledger_pod_qty"].isna()
        | (pod_compare["gap"].abs() > EPS)
    ].sort_values(["ship_date", "item_key"])

    # Item-level quantity checks across shipping / POD / ledger.
    ship_item = (
        ship_with_date.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ship_qty"})
    )
    nav_item = (
        led_in.loc[led_in["source"].eq("NAV")]
        .groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ledger_nav_qty"})
    )
    nav_item_cmp = ship_item.merge(nav_item, on="item_key", how="outer")
    nav_item_cmp["ship_qty"] = pd.to_numeric(nav_item_cmp["ship_qty"], errors="coerce").fillna(0.0)
    nav_item_cmp["ledger_nav_qty"] = pd.to_numeric(nav_item_cmp["ledger_nav_qty"], errors="coerce").fillna(0.0)
    nav_item_cmp["gap"] = nav_item_cmp["ledger_nav_qty"] - nav_item_cmp["ship_qty"]
    nav_item_mismatch = nav_item_cmp.loc[nav_item_cmp["gap"].abs() > EPS].sort_values("item_key")

    pod_item = (
        pod_with_ship.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "pod_qty"})
    )
    led_pod_item = (
        led_in.loc[led_in["source"].eq("POD")]
        .groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ledger_pod_qty"})
    )
    pod_item_cmp = pod_item.merge(led_pod_item, on="item_key", how="outer")
    pod_item_cmp["pod_qty"] = pd.to_numeric(pod_item_cmp["pod_qty"], errors="coerce").fillna(0.0)
    pod_item_cmp["ledger_pod_qty"] = pd.to_numeric(pod_item_cmp["ledger_pod_qty"], errors="coerce").fillna(0.0)
    pod_item_cmp["gap"] = pod_item_cmp["ledger_pod_qty"] - pod_item_cmp["pod_qty"]
    pod_item_overcount = pod_item_cmp.loc[pod_item_cmp["gap"] > EPS].sort_values("gap", ascending=False)
    pod_item_undercount = pod_item_cmp.loc[pod_item_cmp["gap"] < -EPS].sort_values("gap")

    ledger_in_item = (
        led_in.groupby("item_key", as_index=False)["qty"]
        .sum()
        .rename(columns={"qty": "ledger_in_qty"})
    )
    total_supply_item = ship_item.merge(pod_item, on="item_key", how="outer")
    total_supply_item["ship_qty"] = pd.to_numeric(total_supply_item["ship_qty"], errors="coerce").fillna(0.0)
    total_supply_item["pod_qty"] = pd.to_numeric(total_supply_item["pod_qty"], errors="coerce").fillna(0.0)
    total_supply_item["total_supply_qty"] = total_supply_item["ship_qty"] + total_supply_item["pod_qty"]

    ledger_total_cmp = total_supply_item.merge(ledger_in_item, on="item_key", how="outer")
    ledger_total_cmp["total_supply_qty"] = pd.to_numeric(
        ledger_total_cmp["total_supply_qty"], errors="coerce"
    ).fillna(0.0)
    ledger_total_cmp["ledger_in_qty"] = pd.to_numeric(ledger_total_cmp["ledger_in_qty"], errors="coerce").fillna(0.0)
    ledger_total_cmp["over_qty"] = ledger_total_cmp["ledger_in_qty"] - ledger_total_cmp["total_supply_qty"]
    ledger_in_over_total_supply = ledger_total_cmp.loc[
        ledger_total_cmp["over_qty"] > EPS
    ].sort_values("over_qty", ascending=False)

    return {
        "shipping_rows_missing_ship_date": ship_no_date.sort_values(["item_key", "qty"]),
        "nav_missing_or_short_vs_shipping": nav_missing_or_short,
        "nav_extra_vs_shipping": nav_extra,
        "null_ship_potentially_counted_in_nav": null_ship_possibly_counted,
        "pod_mismatch_vs_open_purchase_orders": pod_mismatch,
        "nav_item_qty_mismatch": nav_item_mismatch,
        "pod_item_qty_overcount_in_ledger": pod_item_overcount,
        "pod_item_qty_undercount_in_ledger": pod_item_undercount,
        "ledger_in_over_total_supply_by_item": ledger_in_over_total_supply,
    }


def test_inbound_reconciliation_shipping_and_pod() -> None:
    engine = get_engine()
    report = _build_discrepancy_report(engine)
    checks_passed = (
        report["nav_missing_or_short_vs_shipping"].empty
        and report["nav_extra_vs_shipping"].empty
        and report["null_ship_potentially_counted_in_nav"].empty
    )
    run_id = _write_log_to_db(engine, report, checks_passed)

    print("\n[1] NT Shipping Schedule rows that become NULL ship date after parsing")
    print(_short(report["shipping_rows_missing_ship_date"]))

    print("\n[2] Shipping-derived NAV expected but missing/short in ledger (Kind='IN', Source='NAV')")
    print(_short(report["nav_missing_or_short_vs_shipping"]))

    print("\n[3] Extra NAV rows in ledger not explained by parsed shipping schedule")
    print(_short(report["nav_extra_vs_shipping"]))

    print("\n[4] Potential NULL-Ship-Date shipping qty counted in ledger NAV (suspicious)")
    print(_short(report["null_ship_potentially_counted_in_nav"]))

    print("\n[5] Open_Purchase_Orders vs ledger (Kind='IN', Source='POD') mismatches")
    print(_short(report["pod_mismatch_vs_open_purchase_orders"]))
    print("\n[6] Item-level NAV qty mismatch: shipping(expanded, dated) vs ledger NAV")
    print(_short(report["nav_item_qty_mismatch"]))
    print("\n[7] Item-level POD overcount: ledger POD qty > Open_Purchase_Orders qty")
    print(_short(report["pod_item_qty_overcount_in_ledger"]))
    print("\n[8] Item-level POD undercount: ledger POD qty < Open_Purchase_Orders qty")
    print(_short(report["pod_item_qty_undercount_in_ledger"]))
    print("\n[9] Item-level total overcount: ledger IN qty > (shipping + POD) qty")
    print(_short(report["ledger_in_over_total_supply_by_item"]))
    print(f"\n[LOG] Supabase run_id: {run_id} (tables: {SCHEMA}.{RUN_LOG_TABLE}, {SCHEMA}.{DETAIL_LOG_TABLE})")

    assert report["nav_missing_or_short_vs_shipping"].empty, (
        "Some shipping-schedule rows (after parse) are missing/short in ledger NAV."
    )
    assert report["nav_extra_vs_shipping"].empty, (
        "Some ledger NAV rows are extra vs parsed shipping schedule."
    )
    assert report["null_ship_potentially_counted_in_nav"].empty, (
        "Potential NULL-Ship-Date shipping quantity appears counted in ledger NAV."
    )
    assert report["nav_item_qty_mismatch"].empty, (
        "Item-level NAV quantity mismatch between shipping(expanded) and ledger NAV."
    )
    assert report["pod_item_qty_overcount_in_ledger"].empty, (
        "Item-level overcount: ledger POD quantity exceeds Open_Purchase_Orders."
    )
    assert report["ledger_in_over_total_supply_by_item"].empty, (
        "Item-level overcount: ledger IN quantity exceeds (shipping + POD) supply."
    )
