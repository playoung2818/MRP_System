from __future__ import annotations

import json

import pandas as pd
import requests

from erp_system.runtime.config import POD_FILE, SALES_ORDER_FILE, SHIPPING_SCHEDULE_FILE, WAREHOUSE_INV_FILE
from erp_system.runtime.db_config import get_engine
from erp_system.transform.sales_order import normalize_wo_number

from ._helpers import read_excel_safe


def extract_inputs():
    df_sales_order = pd.read_csv(str(SALES_ORDER_FILE), encoding="ISO-8859-1", engine="python")
    inventory_df = pd.read_csv(str(WAREHOUSE_INV_FILE))
    df_shipping_schedule = read_excel_safe(SHIPPING_SCHEDULE_FILE)
    df_pod = pd.read_csv(str(POD_FILE), encoding="ISO-8859-1", engine="python")
    return df_sales_order, inventory_df, df_shipping_schedule, df_pod


def validate_input_tables(df_shipping_schedule: pd.DataFrame, df_pod: pd.DataFrame) -> None:
    missing: list[str] = []
    if "Model Name" not in df_shipping_schedule.columns:
        missing.append("shipping table missing required column: 'Model Name'")
    if "Inventory Site" not in df_pod.columns:
        missing.append("POD table missing required column: 'Inventory Site'")
    if missing:
        raise ValueError("; ".join(missing))


def fetch_word_files_df(api_url: str | list[str] | tuple[str, ...]) -> pd.DataFrame:
    urls = [api_url] if isinstance(api_url, str) else list(api_url)
    wf = pd.DataFrame(columns=["file_name", "order_id", "status"])
    for url in urls:
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            wf = pd.DataFrame(r.json().get("word_files", []))
            break
        except Exception:
            continue
    if "order_id" in wf.columns:
        wf = wf.rename(columns={"order_id": "WO_Number"})
    if "WO_Number" not in wf.columns:
        wf["WO_Number"] = ""
    wf["WO_Number"] = wf["WO_Number"].astype(str).apply(normalize_wo_number)
    return wf


def fetch_pdf_orders_df_from_DB() -> pd.DataFrame:
    eng = get_engine()
    rows = pd.read_sql("SELECT order_id, extracted_data FROM public.pdf_file_log", eng)

    def rows_from_json(extracted_data, order_id=""):
        if isinstance(extracted_data, str):
            try:
                extracted_data = json.loads(extracted_data)
            except Exception:
                extracted_data = {}
        data = extracted_data or {}
        wo = normalize_wo_number(data.get("wo", order_id))
        items = data.get("items") or []
        if not items:
            return [{"WO": wo, "Product Number": ""}]
        out = []
        for it in items:
            pn = it.get("product_number") or it.get("part_number") or it.get("product") or it.get("part") or ""
            out.append({"WO": wo, "Product Number": pn})
        return out

    all_rows = []
    for _, r in rows.iterrows():
        all_rows.extend(rows_from_json(r.get("extracted_data"), r.get("order_id")))
    return pd.DataFrame(all_rows, columns=["WO", "Product Number"])


__all__ = [
    "extract_inputs",
    "fetch_pdf_orders_df_from_DB",
    "fetch_word_files_df",
    "read_excel_safe",
    "validate_input_tables",
]
