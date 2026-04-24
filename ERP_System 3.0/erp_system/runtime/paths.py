import os
from pathlib import Path


ONEDRIVE = Path(os.getenv("OneDrive"))
BASE = ONEDRIVE / "Share NTA Warehouse" / "Daily Update"

SALES_ORDER_FILE = BASE / "Open Sales Order 04_24_2026.CSV"
WAREHOUSE_INV_FILE = BASE / "WH01S_04_24.CSV"
SHIPPING_SCHEDULE_FILE = BASE / "NTA_Shipping schedule_20260423.xlsx"
POD_FILE = BASE / "POD_04_24.CSV"
GOOGLE_SHEETS_CRED_PATH = os.getenv("GOOGLE_SHEETS_CRED_PATH")
