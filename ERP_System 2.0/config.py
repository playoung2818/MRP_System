import os
from pathlib import Path

# === File paths ===
# Detect OneDrive root for the current user
ONEDRIVE = Path(os.getenv("OneDrive"))

BASE = ONEDRIVE / "Share NTA Warehouse" / "Daily Update"

SALES_ORDER_FILE = BASE / "Open Sales Order 02_23_2026.CSV"
WAREHOUSE_INV_FILE = BASE / "WH01S_02_23.CSV"
SHIPPING_SCHEDULE_FILE = BASE / "NTA_Shipping schedule_20260223.xlsx"
POD_FILE = BASE / "POD_02_23.CSV"


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
