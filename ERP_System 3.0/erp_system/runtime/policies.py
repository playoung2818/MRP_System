WORD_FILE_API_URLS = (
    "http://127.0.0.1:5001/api/word-files",
    "http://localhost:5001/api/word-files",
    "http://192.168.60.133:5001/api/word-files",
)

GOOGLE_SHEET_SPREADSHEET = "PDF_WO"
GOOGLE_SHEET_WORKSHEET = "Open Sales Order"

NOT_ASSIGNED_SO_EXPORT_PATH = (
    r"C:\Users\Admin\OneDrive - neousys-tech\Desktop\Python\ERP_System\Not_assigned_SO.xlsx"
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

PREINSTALL_MODEL_PREFIXES = ("N", "SEMIL", "POC", "F", "S1", "S2")
PREINSTALL_EXCLUDED_PREFIXES = ("NRU-52S-NX",)
PREINSTALL_MODEL_EXCLUSIONS = frozenset(
    {
        "NRU-120S-AGX32G",
        "NRU-120S-JAXI32GB",
        "NRU-154-JON16-NS",
        "NRU-154-JON8-NS",
        "NRU-156-JON8-128GB",
        "NRU-156-JON8-NS",
        "NRU-161V-AWP-JON16-NS",
        "NRU-162S-AWP-JON16-NS",
        "NRU-171V-PPC-JON16-NS",
        "NRU-172S-PPC-JON16-NS",
    }
)
