from __future__ import annotations

import logging

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

    cols = ["Date", "Item_raw", "Projected_NAV", "Name", "QB Num"]
    print(violations.loc[:, cols])

    inv, structured, pod, ship, ledger = _validate_outputs(inv, structured, pod, ship, ledger)

    atp_view = build_atp_view(ledger)
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

    print(
        f"Loaded: {DB_SCHEMA}.{TBL_SALES_ORDER}={len(so_full)}; "
        f"{DB_SCHEMA}.{TBL_INVENTORY}={len(inv)}; "
        f"{DB_SCHEMA}.{TBL_STRUCTURED}={len(structured)}; "
        f"{DB_SCHEMA}.{TBL_POD}={len(pod)}; "
        f"{DB_SCHEMA}.{TBL_Shipping}={len(ship)}; "
        f"{DB_SCHEMA}.{TBL_LEDGER}={len(ledger)}; "
        f"{DB_SCHEMA}.{TBL_ITEM_ATP}={len(atp_view)}; "
    )

    if not final_sales_order.empty:
        so_for_sheet = final_sales_order.assign(
            **{"Lead Time": pd.to_datetime(final_sales_order["Lead Time"], errors="coerce").dt.date}
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
