import os
from pathlib import Path


ONEDRIVE = Path(os.getenv("OneDrive"))
BASE = ONEDRIVE / "Share NTA Warehouse" / "Daily Update"

SALES_ORDER_FILE = BASE / "Open Sales Order_07_16_2026.CSV"
WAREHOUSE_INV_FILE = BASE / "WH01S_07_16_2026.CSV"
SHIPPING_SCHEDULE_FILE = BASE / "NTA_Shipping schedule_20260716.xlsx"
POD_FILE = BASE / "POD_07_16_2026.CSV"
PERIPHERAL_STATUS_FILE = BASE / "Peripheral Status Update_20260713.xlsx"
GOOGLE_SHEETS_CRED_PATH = os.getenv("GOOGLE_SHEETS_CRED_PATH")
