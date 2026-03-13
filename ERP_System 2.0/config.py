import os
from pathlib import Path

# === File paths ===
# Detect OneDrive root for the current user
ONEDRIVE = Path(os.getenv("OneDrive"))

BASE = ONEDRIVE / "Share NTA Warehouse" / "Daily Update"

SALES_ORDER_FILE = BASE / "Open Sales Order 03_12_2026.CSV"
WAREHOUSE_INV_FILE = BASE / "WH01S_03_12.CSV"
SHIPPING_SCHEDULE_FILE = BASE / "NTA_Shipping schedule_20260312.xlsx"
POD_FILE = BASE / "POD_03_12.CSV"
GOOGLE_SHEETS_CRED_PATH = os.getenv("GOOGLE_SHEETS_CRED_PATH")


# === Supabase/Postgres (pooled) ===
# DSN is now provided via environment for security.
# Re-export from db_config so existing imports continue to work.
from db_config import DATABASE_DSN  

# === Target tables (pick schema you actually use) ===
DB_SCHEMA = "public"
TBL_INVENTORY = "inventory_status"
TBL_STRUCTURED = "wo_structured"
TBL_SALES_ORDER = "open_sales_orders"
TBL_POD = "Open_Purchase_Orders"
TBL_Shipping = "NT Shipping Schedule"
TBL_LEDGER = "ledger_analytics"
TBL_ITEM_SUMMARY = "item_summary"
TBL_ITEM_ATP = "item_atp"
TBL_SO_ASSIGN_READY = "so_assignment_readiness"
TBL_SO_ASSIGN_BLOCKERS = "so_assignment_blockers"
TBL_SO_REFERENCE_MAIN = "so_reference_main"
TBL_SO_REFERENCE_STAGE = "so_reference_stage"
TBL_SO_REFERENCE_DIFF = "so_reference_diff"
TBL_SO_ASSIGN_RUNS = "so_assignment_runs"
TBL_SO_ASSIGN_RUN_BLOCKERS = "so_assignment_run_blockers"
TBL_SO_ASSIGN_RUN_DIFF = "so_assignment_run_diff"

