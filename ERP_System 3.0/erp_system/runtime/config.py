from .db_config import DATABASE_DSN
from .paths import (
    GOOGLE_SHEETS_CRED_PATH,
    POD_FILE,
    SALES_ORDER_FILE,
    SHIPPING_SCHEDULE_FILE,
    WAREHOUSE_INV_FILE,
)

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
TBL_SO_ASSIGNMENT_RUNS = "so_assignment_runs"
