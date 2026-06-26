from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from erp_system.contracts import TABLE_CONTRACTS, validate_output_table
from erp_system.ingest.io_ops import (
    save_not_assigned_so,
    write_final_sales_order_to_gsheet,
    write_to_db,
)
from erp_system.ingest.sources import (
    extract_inputs,
    fetch_pdf_orders_df_from_DB,
    fetch_word_files_df,
    validate_input_tables,
)
from erp_system.ledger.atp import build_atp_view
from erp_system.ledger.assignment_readiness import build_assignment_run_tables
from erp_system.ledger.events import _order_events, build_events, expand_nav_preinstalled
from erp_system.ledger.ledger import build_ledger_from_events
from erp_system.runtime.config import (
    DB_SCHEMA,
    TBL_INVENTORY,
    TBL_ITEM_ATP,
    TBL_ITEM_SUMMARY,
    TBL_LEDGER,
    TBL_POD,
    TBL_SALES_ORDER,
    TBL_SO_ASSIGNMENT_RUNS,
    TBL_Shipping,
    TBL_STRUCTURED,
)
from erp_system.runtime.policies import (
    GOOGLE_SHEET_SPREADSHEET,
    GOOGLE_SHEET_WORKSHEET,
    NOT_ASSIGNED_SO_EXPORT_PATH,
    WORD_FILE_API_URLS,
)
from erp_system.transform.inventory import add_onhand_minus_wip, build_wip_lookup, transform_inventory
from erp_system.transform.pod import enrich_pod_with_shipping_audit, transform_pod
from erp_system.transform.sales_order import transform_sales_order
from erp_system.transform.shipping import transform_shipping
from erp_system.transform.structured import build_structured_df, prepare_erp_view


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

VIOLATION_REPORT_COLUMNS = ["Date", "Item", "Item_raw", "Projected_NAV", "Name", "QB Num"]
VIOLATION_DIFF_KEY_COLUMNS = ["Date", "Item", "Item_raw", "QB Num"]
REPORT_DIR = Path("reports")
NEGATIVE_PROJECTED_QTY_REPORT_PATH = REPORT_DIR / "negative_projected_qty.xlsx"
VIOLATION_SNAPSHOT_PATH = REPORT_DIR / ".last_violation_report.csv"


def _validate_outputs(
    inv: pd.DataFrame,
    structured: pd.DataFrame,
    pod: pd.DataFrame,
    ship: pd.DataFrame,
    ledger: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    return (
        validate_output_table(inv, TABLE_CONTRACTS[TBL_INVENTORY]),
        validate_output_table(structured, TABLE_CONTRACTS[TBL_STRUCTURED]),
        validate_output_table(pod, TABLE_CONTRACTS[TBL_POD]),
        validate_output_table(ship, TABLE_CONTRACTS[TBL_Shipping]),
        validate_output_table(ledger, TABLE_CONTRACTS[TBL_LEDGER]),
    )


def _prepare_violation_report(violations: pd.DataFrame) -> pd.DataFrame:
    report = violations.reindex(columns=VIOLATION_REPORT_COLUMNS).copy()
    if "Date" in report.columns:
        report["Date"] = pd.to_datetime(report["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ("Item", "Item_raw", "Name", "QB Num"):
        if col in report.columns:
            report[col] = report[col].fillna("").astype(str)
    if "Projected_NAV" in report.columns:
        report["Projected_NAV"] = pd.to_numeric(report["Projected_NAV"], errors="coerce")
    return report


def _print_violation_overview(current: pd.DataFrame) -> None:
    if current.empty:
        print("\nViolations: 0 rows.")
        return

    earliest = current["Date"].dropna().min()
    worst = current["Projected_NAV"].min()
    print(
        "\nViolations: "
        f"{len(current)} rows, "
        f"{current['QB Num'].nunique()} SOs, "
        f"{current['Item'].nunique()} items, "
        f"earliest={earliest}, "
        f"worst_projected_qty={worst:g}"
    )


def _normalize_violation_report(df: pd.DataFrame) -> pd.DataFrame:
    out = df.reindex(columns=VIOLATION_REPORT_COLUMNS).copy()
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for col in ("Item", "Item_raw", "Name", "QB Num"):
        out[col] = out[col].fillna("").astype(str)
    out["Projected_NAV"] = pd.to_numeric(out["Projected_NAV"], errors="coerce")
    return out


def _write_negative_projected_qty_report(current: pd.DataFrame) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    current = _normalize_violation_report(current)
    current.to_excel(NEGATIVE_PROJECTED_QTY_REPORT_PATH, index=False)
    print(f"Negative projected qty report written to {NEGATIVE_PROJECTED_QTY_REPORT_PATH}")


def _print_violation_diff(current: pd.DataFrame) -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    current = _normalize_violation_report(current)

    previous = pd.DataFrame(columns=VIOLATION_REPORT_COLUMNS)
    had_previous = VIOLATION_SNAPSHOT_PATH.exists()
    if had_previous:
        try:
            previous = _normalize_violation_report(pd.read_csv(VIOLATION_SNAPSHOT_PATH))
        except Exception:
            previous = pd.DataFrame(columns=VIOLATION_REPORT_COLUMNS)
            had_previous = False

    if not had_previous:
        print("Violation diff vs last run: no previous snapshot found. This run is now the baseline.")
        current.to_csv(VIOLATION_SNAPSHOT_PATH, index=False)
        return

    prev_by_key = previous.set_index(VIOLATION_DIFF_KEY_COLUMNS, drop=False)
    curr_by_key = current.set_index(VIOLATION_DIFF_KEY_COLUMNS, drop=False)
    prev_keys = set(prev_by_key.index)
    curr_keys = set(curr_by_key.index)

    added = curr_by_key.loc[sorted(curr_keys - prev_keys)].reset_index(drop=True) if curr_keys - prev_keys else current.iloc[0:0].copy()
    resolved = prev_by_key.loc[sorted(prev_keys - curr_keys)].reset_index(drop=True) if prev_keys - curr_keys else previous.iloc[0:0].copy()

    changed_rows = []
    for key in sorted(curr_keys & prev_keys):
        prev_row = prev_by_key.loc[key]
        curr_row = curr_by_key.loc[key]
        if isinstance(prev_row, pd.DataFrame):
            prev_row = prev_row.iloc[0]
        if isinstance(curr_row, pd.DataFrame):
            curr_row = curr_row.iloc[0]
        prev_nav = prev_row.get("Projected_NAV")
        curr_nav = curr_row.get("Projected_NAV")
        if pd.isna(prev_nav) and pd.isna(curr_nav):
            continue
        if pd.isna(prev_nav) or pd.isna(curr_nav) or float(prev_nav) != float(curr_nav):
            row = curr_row.to_dict()
            row["Previous_Projected_NAV"] = prev_nav
            row["Current_Projected_NAV"] = curr_nav
            changed_rows.append(row)
    changed = pd.DataFrame(changed_rows)

    print(
        "Violation diff vs last run: "
        f"current={len(current)}, previous={len(previous)}, "
        f"new={len(added)}, resolved={len(resolved)}, changed={len(changed)}"
    )
    if not added.empty:
        print("\nNew violation rows:")
        print(added.loc[:, VIOLATION_REPORT_COLUMNS])
    if not resolved.empty:
        print("\nResolved violation rows:")
        print(resolved.loc[:, VIOLATION_REPORT_COLUMNS])
    if not changed.empty:
        changed_cols = VIOLATION_DIFF_KEY_COLUMNS + ["Previous_Projected_NAV", "Current_Projected_NAV", "Name"]
        print("\nChanged projected qty rows:")
        print(changed.reindex(columns=changed_cols))

    current.to_csv(VIOLATION_SNAPSHOT_PATH, index=False)


def main() -> None:
    so_raw, inv_raw, ship_raw, pod_raw = extract_inputs()
    validate_input_tables(ship_raw, pod_raw)
    word_files_df = fetch_word_files_df(WORD_FILE_API_URLS)
    pdf_orders_df = fetch_pdf_orders_df_from_DB()

    so_full = transform_sales_order(so_raw)
    wip_lookup = build_wip_lookup(so_full, word_files_df)
    inv = transform_inventory(inv_raw, wip_lookup)
    pod = transform_pod(pod_raw)
    ship = transform_shipping(ship_raw)
    pod = enrich_pod_with_shipping_audit(pod, ship)

    structured, final_sales_order = build_structured_df(so_full, word_files_df, inv, pdf_orders_df, pod)
    inv = add_onhand_minus_wip(inv, structured)

    nav_exp = expand_nav_preinstalled(ship)
    events_all = _order_events(build_events(structured, nav_exp, pod))
    ledger, item_summary, violations = build_ledger_from_events(structured, events_all, inv)

    violation_report = _prepare_violation_report(violations)
    _print_violation_overview(violation_report)
    _write_negative_projected_qty_report(violation_report)
    _print_violation_diff(violation_report)

    inv, structured, pod, ship, ledger = _validate_outputs(inv, structured, pod, ship, ledger)

    atp_view = build_atp_view(ledger)
    assignment_runs = build_assignment_run_tables(structured, ledger)
    erp_df = prepare_erp_view(structured)
    not_assigned_so = erp_df.loc[~erp_df["AssignedFlag"]].copy()

    summary = save_not_assigned_so(
        not_assigned_so.copy(),
        output_path=NOT_ASSIGNED_SO_EXPORT_PATH,
        band_by_col="QB Num",
        shortage_col="Component_Status",
        shortage_value="Shortage",
        pod_watchlist_df=pd.DataFrame(columns=["QB Num", "Item", "Component_Status", "POD#"]),
    )
    print(summary)

    write_to_db(inv, schema=DB_SCHEMA, table=TBL_INVENTORY)
    write_to_db(so_full, schema=DB_SCHEMA, table=TBL_SALES_ORDER)
    write_to_db(structured, schema=DB_SCHEMA, table=TBL_STRUCTURED)
    write_to_db(pod, schema=DB_SCHEMA, table=TBL_POD)
    write_to_db(ship, schema=DB_SCHEMA, table=TBL_Shipping)
    write_to_db(ledger, schema=DB_SCHEMA, table=TBL_LEDGER)
    write_to_db(item_summary, schema=DB_SCHEMA, table=TBL_ITEM_SUMMARY)
    write_to_db(atp_view, schema=DB_SCHEMA, table=TBL_ITEM_ATP)
    write_to_db(assignment_runs, schema=DB_SCHEMA, table=TBL_SO_ASSIGNMENT_RUNS)

    print(
        f"Loaded: {DB_SCHEMA}.{TBL_SALES_ORDER}={len(so_full)}; "
        f"{DB_SCHEMA}.{TBL_INVENTORY}={len(inv)}; "
        f"{DB_SCHEMA}.{TBL_STRUCTURED}={len(structured)}; "
        f"{DB_SCHEMA}.{TBL_POD}={len(pod)}; "
        f"{DB_SCHEMA}.{TBL_Shipping}={len(ship)}; "
        f"{DB_SCHEMA}.{TBL_LEDGER}={len(ledger)}; "
        f"{DB_SCHEMA}.{TBL_ITEM_ATP}={len(atp_view)}; "
        f"{DB_SCHEMA}.{TBL_SO_ASSIGNMENT_RUNS}={len(assignment_runs)}; "
    )


    ## Write SO sheet to Google sheet
    if not final_sales_order.empty:
        so_for_sheet = final_sales_order.assign(
            Lead_Time = pd.to_datetime(final_sales_order["Lead Time"], errors="coerce").dt.date
        )
        try:
            write_final_sales_order_to_gsheet(
                so_for_sheet,
                spreadsheet_name=GOOGLE_SHEET_SPREADSHEET,
                worksheet_name=GOOGLE_SHEET_WORKSHEET,
            )
        except Exception as exc:
            logging.warning("Skipping Open Sales Order export: %s", exc)


if __name__ == "__main__":
    logging.info("MRP pipeline online. Beginning data ingestion....")
    main()
    logging.info("Done.")
