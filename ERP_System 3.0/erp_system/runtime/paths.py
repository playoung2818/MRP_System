import os
from pathlib import Path


ONEDRIVE = Path(os.getenv("OneDrive"))
BASE = ONEDRIVE / "Share NTA Warehouse" / "Daily Update"


# Update Date in here
DAILY_DATE = "07_21_2026"

SALES_ORDER_FILE = BASE / f"Open Sales Order_{DAILY_DATE}.CSV"
WAREHOUSE_INV_FILE = BASE / f"WH01S_{DAILY_DATE}.CSV"
POD_FILE = BASE / f"POD_{DAILY_DATE}.CSV"

SHIPPING_SCHEDULE_FILE = max(
    BASE.glob("NTA_Shipping schedule_*.xlsx")
)

PERIPHERAL_STATUS_FILE = max(
    BASE.glob("Peripheral Status Update_*.xlsx")
)
GOOGLE_SHEETS_CRED_PATH = os.getenv("GOOGLE_SHEETS_CRED_PATH")
