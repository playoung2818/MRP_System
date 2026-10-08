import os
from pathlib import Path


GOOGLE_SHEET_SPREADSHEET = "PDF_WO"
GOOGLE_SHEET_WORKSHEET = "Open Sales Order"

NOT_ASSIGNED_SO_EXPORT_PATH = os.getenv(
    "NOT_ASSIGNED_SO_EXPORT_PATH",
    str(Path(__file__).resolve().parents[3] / "Not_assigned_SO.xlsx"),
)

POD_REFERENCE_PATH = (
    r"C:\Users\Admin\OneDrive - neousys-tech\Share NTA Warehouse\01 Incoming\POD-Reference.xlsx"
)

EXCLUDED_PREINSTALLED_PO_VENDORS = frozenset(
    {
        "Neousys Technology Incorp.",
        "Amazon",
        "Newegg Business, Inc.",
        "Newegg.com",
        "Kontron America, Inc.",
        "Provantage LLC",
        "SMART Modular Technologies, Inc.",
        "Spectrum Sourcing",
        "Arrow Electronics, Inc.",
        "ASI Computer Technologies, Inc.",
        "B&H",
        "PhyTools",
        "Mouser Electronics",
        "Genoedge Corporation DBA SabrePC.COM",
        "CoastIPC, Inc.",
        "Industrial PC, Inc.",
    }
)

EXCLUDED_POD_SOURCE_NAMES = frozenset({"Neousys Technology Incorp."})

PREINSTALL_MODEL_PREFIXES = ("N", "SEMIL", "POC", "F", "S1", "S2", "FLYC")
PREINSTALL_KEEP_MODEL_SKIP_FIRST_COMPONENT_PREFIXES = ("NRU-1", "NRU-5") # NRU-52+-JON16-NS, only expand the peripherals, keep the SOM
