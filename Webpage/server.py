# server.py
import os
import sys
import json
from datetime import datetime
from pathlib import Path
from flask import Flask, request, render_template_string, jsonify, abort, redirect, url_for, send_file, Response
import pandas as pd
from sqlalchemy import text

from ui import (
    ERR_TPL,
    INDEX_TPL,
    SUBPAGE_TPL,
    ITEM_TPL,
    INVENTORY_TPL,
    PRODUCTION_TPL,
)
from quote_ui import QUOTE_TPL

REPO_ROOT = Path(__file__).resolve().parents[1]
ERP_MODULE_DIR = REPO_ROOT / "ERP_System 2.0"
if str(ERP_MODULE_DIR) not in sys.path:
    sys.path.append(str(ERP_MODULE_DIR))

from erp_normalize import normalize_item
from atp import build_atp_view, earliest_atp_strict
from db_config import get_engine, DATABASE_DSN

app = Flask(__name__)

# =========================
# DB ENGINE
# =========================
engine = get_engine()

# =========================
# Data cache
# =========================
SO_INV: pd.DataFrame | None = None
NAV: pd.DataFrame | None = None
OPEN_PO: pd.DataFrame | None = None
FINAL_SO: pd.DataFrame | None = None
LEDGER: pd.DataFrame | None = None
ITEM_ATP: pd.DataFrame | None = None
_LAST_LOAD_ERR: str | None = None
_LAST_LOADED_AT: datetime | None = None

# =========================
# PDF settings/cache
# =========================
# Configure a root folder that contains PDF files named by order id (e.g. SO-12345.pdf)
PDF_FOLDER = os.getenv("PDF_FOLDER", "")
# Map of order_id (stem of filename) -> {file_name, file_path}
PDF_MAP: dict[str, dict[str, str]] = {}

TABLE_HEADER_LABELS = {
    "Item": "Item",
    "Qty(-)": "Qty (-)",
    "Available": "Available",
    "Available + Pre-installed PO": "Avail + Pre-PO",
    "On Hand": "On Hand",
    "On Sales Order": "On SO",
    "On PO": "On PO",
    "Assigned Q'ty": "Assigned Qty",
    "On Hand - WIP": "On Hand - WIP",
    "Available + On PO": "Avail + On PO",
    "Sales/Week": "Sales / Week",
    "Recommended Restock Qty": "Restock Qty",
    "Component_Status": "Status",
    "Ship Date": "Ship Date",
}

# -------- helpers --------
def _safe_date_col(df: pd.DataFrame, col: str):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

def _to_date_str(s: pd.Series, fmt="%Y-%m-%d") -> pd.Series:
    s = pd.to_datetime(s, errors="coerce")
    return s.apply(lambda x: x.strftime(fmt) if pd.notnull(x) else "")

def _read_table(schema: str, table: str) -> pd.DataFrame:
    sql = f'SELECT * FROM "{schema}"."{table}"'
    return pd.read_sql_query(text(sql), con=engine)


def _reorder_df_out_by_output(output_df: pd.DataFrame, df_out: pd.DataFrame) -> pd.DataFrame:
    """
    Reorder df_out to match the line ordering found in output_df.
    Both frames are expected to use columns: ['QB Num', 'Item'].
    """
    if output_df is None or output_df.empty:
        return df_out.sort_values(["QB Num", "Item"]).reset_index(drop=True)

    ref = output_df.copy()
    ref["__pos_out"] = ref.groupby("QB Num").cumcount()
    ref["__occ"] = ref.groupby(["QB Num", "Item"]).cumcount()
    ref_key = ref[["QB Num", "Item", "__occ", "__pos_out"]]

    tgt = df_out.copy()
    tgt["__occ"] = tgt.groupby(["QB Num", "Item"]).cumcount()

    merged = tgt.merge(ref_key, on=["QB Num", "Item", "__occ"], how="left")
    merged["__fallback"] = merged.groupby("QB Num").cumcount()
    merged["__pos_out"] = merged["__pos_out"].fillna(float("inf"))

    ordered = (
        merged.sort_values(["QB Num", "__pos_out", "__fallback"])
        .drop(columns=["__occ", "__pos_out", "__fallback"])
        .reset_index(drop=True)
    )
    return ordered


def _build_pdf_orders_df() -> pd.DataFrame:
    """
    Build ['WO','Product Number'] from public.pdf_file_log.extracted_data JSON.
    Mirrors io_ops.fetch_pdf_orders_df_from_supabase but kept local to avoid extra deps.
    """
    try:
        rows = pd.read_sql('SELECT order_id, extracted_data FROM public.pdf_file_log', engine)
    except Exception:
        return pd.DataFrame(columns=["WO", "Product Number"])

    def rows_from_json(extracted_data, order_id=""):
        if isinstance(extracted_data, str):
            try:
                extracted_data = json.loads(extracted_data)
            except Exception:
                extracted_data = {}
        data = extracted_data or {}
        wo = data.get("wo", order_id)
        items = data.get("items") or []
        if not items:
            return [{"WO": wo, "Product Number": ""}]
        out = []
        for it in items:
            pn = (
                it.get("product_number")
                or it.get("part_number")
                or it.get("product")
                or it.get("part")
                or ""
            )
            out.append({"WO": wo, "Product Number": pn})
        return out

    all_rows = []
    for _, r in rows.iterrows():
        all_rows.extend(rows_from_json(r.get("extracted_data"), r.get("order_id")))
    return pd.DataFrame(all_rows, columns=["WO", "Product Number"])


def _build_final_sales_order_from_db() -> pd.DataFrame:
    """
    Rebuild final_sales_order from DB tables so it can be used
    for the Production Planning calendar.
    """
    try:
        df_sales_order = _read_table("public", "open_sales_orders")
    except Exception:
        return pd.DataFrame()

    pdf_orders_df = _build_pdf_orders_df()

    needed_cols = {
        "Order Date": "SO Entry Date",
        "Name": "Customer",
        "P. O. #": "Customer PO",
        "QB Num": "QB Num",
        "Item": "Item",
        "Qty(-)": "Qty",
        "Ship Date": "Lead Time",
    }
    for src in list(needed_cols.keys()):
        if src not in df_sales_order.columns:
            df_sales_order[src] = "" if src not in ("Qty(-)",) else 0

    df_out = (
        df_sales_order.rename(columns=needed_cols)[list(needed_cols.values())].copy()
    )

    df_out["WO"] = ""
    for alt in ["WO", "WO_Number", "NTA Order ID", "SO Number"]:
        if alt in df_sales_order.columns:
            df_out["WO"] = df_sales_order[alt].astype(str)
            break

    df_out = df_out.sort_values(["QB Num", "Item"]).reset_index(drop=True)

    pdf_ref = pdf_orders_df.rename(columns={"WO": "QB Num", "Product Number": "Item"})
    final_sales_order = _reorder_df_out_by_output(pdf_ref, df_out)

    final_sales_order["Item"] = final_sales_order["Item"].map(normalize_item)
    final_sales_order = final_sales_order.loc[:, ~final_sales_order.columns.duplicated()]

    return final_sales_order

# ---------- PDF DB helpers (no Flask-SQLAlchemy) ----------
def _pdf_db_search_by_filename(search_query: str, limit: int = 10) -> list[dict]:
    """Search pdf_file_log by file_name ILIKE %search_query% using SQLAlchemy Core.
    Returns list of dict rows; empty if table missing or error.
    """
    if not search_query:
        return []
    try:
        sql = text(
            'SELECT id, order_id, file_name, file_path, extracted_data '
            'FROM "public"."pdf_file_log" WHERE file_name ILIKE :q '
            'ORDER BY id DESC LIMIT :lim'
        )
        with engine.connect() as conn:
            res = conn.execute(sql, {"q": f"%{search_query}%", "lim": limit})
            return [dict(row) for row in res.mappings().all()]
    except Exception:
        # Table might not exist, or permissions issues; return empty silently
        return []

def _pdf_db_get_by_id(pdf_id: int) -> dict | None:
    try:
        sql = text(
            'SELECT id, order_id, file_name, file_path, extracted_data '
            'FROM "public"."pdf_file_log" WHERE id = :id'
        )
        with engine.connect() as conn:
            res = conn.execute(sql, {"id": pdf_id})
            row = res.mappings().first()
            return dict(row) if row else None
    except Exception:
        return None
def _validate_paths(paths: list[str]):
    for p in paths:
        if not os.path.exists(p):
            # Using print to avoid coupling to any logger; environment prints to console
            print(f"[pdf] Path does not exist: {p}")
        else:
            print(f"[pdf] Valid path: {p}")

def _scan_pdf_folder(folder_path: str) -> dict[str, dict[str, str]]:
    data: dict[str, dict[str, str]] = {}
    if not folder_path:
        return data
    if not os.path.isdir(folder_path):
        print(f"[pdf] PDF_FOLDER is not a directory: {folder_path}")
        return data
    for root, _dirs, files in os.walk(folder_path):
        for name in files:
            if name.lower().endswith(".pdf"):
                path = os.path.join(root, name)
                order_id = os.path.splitext(name)[0]
                data[order_id.upper()] = {"file_name": name, "file_path": path}
    print(f"[pdf] scanned {len(data)} PDF(s) under {folder_path}")
    return data

def _load_pdf_map(force: bool = False):
    global PDF_MAP
    if PDF_MAP and not force:
        return
    if PDF_FOLDER:
        _validate_paths([PDF_FOLDER])
        PDF_MAP = _scan_pdf_folder(PDF_FOLDER)
    else:
        PDF_MAP = {}

def _load_from_db(force: bool = False):
    global SO_INV, NAV, OPEN_PO, FINAL_SO, LEDGER, ITEM_ATP, _LAST_LOAD_ERR, _LAST_LOADED_AT
    try:
        if (
            force
            or SO_INV is None
            or NAV is None
            or OPEN_PO is None
            or FINAL_SO is None
            or LEDGER is None
            or ITEM_ATP is None
        ):
            so = _read_table("public", "wo_structured")
            nav = _read_table("public", "NT Shipping Schedule")
            open_po = _read_table("public", "Open_Purchase_Orders")
            ledger = _read_table("public", "ledger_analytics")
            # item_atp is optional; if missing, fall back to empty frame
            try:
                item_atp = _read_table("public", "item_atp")
            except Exception:
                item_atp = pd.DataFrame(columns=["Item", "Date", "Projected_NAV", "FutureMin_NAV"])

            for c in ("Ship Date", "Order Date"):
                _safe_date_col(so, c)
                _safe_date_col(nav, c)
            for col in open_po.columns:
                if "date" in col.lower():
                    _safe_date_col(open_po, col)
            if "Date" in ledger.columns:
                _safe_date_col(ledger, "Date")

            SO_INV, NAV, OPEN_PO = so, nav, open_po
            FINAL_SO = _build_final_sales_order_from_db()
            LEDGER = ledger
            ITEM_ATP = item_atp
            _LAST_LOAD_ERR = None
            _LAST_LOADED_AT = datetime.now()
    except Exception as e:
        SO_INV = None
        NAV = None
        OPEN_PO = None
        FINAL_SO = None
        LEDGER = None
        ITEM_ATP = None
        _LAST_LOAD_ERR = f"DB load error: {e}"

def _ensure_loaded():
    if (
        SO_INV is None
        or NAV is None
        or OPEN_PO is None
        or FINAL_SO is None
        or LEDGER is None
        or ITEM_ATP is None
    ):
        _load_from_db(force=True)
    # Load PDF map on demand as well
    _load_pdf_map()

def lookup_on_po_by_item(item: str) -> int | None:
    df = SO_INV[SO_INV["Item"] == item]
    if "On PO" not in df.columns:
        return None
    s = pd.to_numeric(df["On PO"], errors="coerce").dropna()
    return int(s.iloc[0]) if not s.empty else None

def lookup_on_sales_by_item(item: str) -> int | float | None:
    df = SO_INV[SO_INV["Item"] == item]
    col_name = None
    for candidate in ("On Sales Order", "On Sales", "On SO"):
        if candidate in df.columns:
            col_name = candidate
            break
    if not col_name:
        return None
    s = pd.to_numeric(df[col_name], errors="coerce").dropna()
    if s.empty:
        return None
    first = s.iloc[0]
    return int(first) if s.eq(first).all() else int(s.sum())

def _coerce_total(val):
    if pd.isna(val):
        return None
    as_float = float(val)
    return int(as_float) if as_float.is_integer() else as_float

def _format_intish(val: object) -> str:
    if val is None or val == "" or pd.isna(val):
        return ""
    try:
        fval = float(val)
    except Exception:
        return str(val)
    return str(int(fval)) if fval.is_integer() else str(fval)

def _aggregate_metric(series: pd.Series) -> int | float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty:
        return None
    first = numeric.iloc[0]
    if numeric.eq(first).all():
        return _coerce_total(first)
    total = numeric.sum()
    return _coerce_total(total)


def _lookup_earliest_atp_date(item: str, qty: float = 1.0) -> datetime | None:
    """
    Best-effort ATP lookup.

    Preferred source: precomputed item_atp (faster, computed in ETL).
    Fallback: derive from ledger_analytics for the specific item when
    item_atp is missing or empty.
    """
    today = datetime.today().date()
    from_date = pd.Timestamp(today)

    # -------- primary: compute from ledger (exclude placeholder dates) --------
    if LEDGER is None or LEDGER.empty:
        # fallback to precomputed item_atp if ledger is unavailable
        if ITEM_ATP is None or ITEM_ATP.empty:
            return None
        atp_dt = earliest_atp_strict(ITEM_ATP, item, qty, from_date=from_date, allow_zero=True)
        if atp_dt is None:
            return None
        return atp_dt.to_pydatetime()

    df_ledger = LEDGER.copy()
    atp_view = build_atp_view(df_ledger)

    # Ensure a "today" row exists for this item so ATP can be today
    if not atp_view.empty and "Date" in atp_view.columns:
        today_ts = pd.Timestamp(today)
        item_mask = atp_view["Item"].astype(str) == str(item)
        has_today = False
        if item_mask.any():
            has_today = atp_view.loc[item_mask, "Date"].dt.normalize().eq(today_ts).any()
        if item_mask.any() and not has_today:
            df_item = df_ledger.loc[df_ledger["Item"].astype(str) == str(item)].copy()
            df_item["Date"] = pd.to_datetime(df_item["Date"], errors="coerce")
            df_item = df_item.loc[df_item["Date"].notna()]
            if not df_item.empty and "Projected_NAV" in df_item.columns:
                df_item.sort_values("Date", inplace=True)
                past = df_item.loc[df_item["Date"] <= today_ts]
                if not past.empty:
                    proj_nav = past.iloc[-1]["Projected_NAV"]
                else:
                    proj_nav = df_item.iloc[0]["Projected_NAV"]
                if pd.notna(proj_nav):
                    future_min = proj_nav
                    future_rows = atp_view.loc[item_mask & (atp_view["Date"] >= today_ts), "FutureMin_NAV"]
                    future_rows = pd.to_numeric(future_rows, errors="coerce").dropna()
                    if not future_rows.empty:
                        future_min = min(float(proj_nav), float(future_rows.min()))
                    add_row = pd.DataFrame(
                        {
                            "Item": [item],
                            "Date": [today_ts],
                            "Projected_NAV": [proj_nav],
                            "FutureMin_NAV": [future_min],
                        }
                    )
                    atp_view = pd.concat([add_row, atp_view], ignore_index=True, sort=False)
    atp_dt = earliest_atp_strict(atp_view, item, qty, from_date=from_date, allow_zero=True)
    if atp_dt is None:
        return None
    return atp_dt.to_pydatetime()


def _find_pdf_url_for_so(so_num: str, po_num: str | None = None) -> str | None:
    """
    Best-effort PDF link lookup for a given SO/QB number.
    Mirrors the logic used on the main index page.
    """
    so_num = (so_num or "").strip()
    if not so_num:
        return None

    search_keys = [so_num]
    so_upper = so_num.upper()
    if so_upper.startswith("SO-"):
        search_keys.append(so_num[3:])
    if po_num:
        search_keys.append(str(po_num))

    pdf_record = None
    for key in search_keys:
        recs = _pdf_db_search_by_filename(key, limit=1)
        if recs:
            pdf_record = recs[0]
            break

    if pdf_record:
        return f"/pdfid/{pdf_record['id']}"

    keys_to_try = [so_num, so_upper.replace("SO-", ""), so_upper.replace("SO", "").strip("- ")]
    pdf_info = None
    for k in keys_to_try:
        pdf_info = PDF_MAP.get(k.upper())
        if pdf_info:
            break
    if pdf_info:
        return f"/pdf/{(pdf_info['file_name'][:-4])}"
    return None

def _so_table_for_item(item: str) -> tuple[list[str], list[dict], dict[str, int | float | None]]:
    need_cols = ["Name", "QB Num", "Item", "Qty(-)", "On Hand - WIP", "Ship Date", "Picked"]
    g = SO_INV[SO_INV["Item"] == item].copy()
    for c in need_cols:
        if c not in g.columns:
            g[c] = ""
    # Fallback for WIP column if missing in data
    if "On Hand - WIP" not in SO_INV.columns and "In Stock(Inventory)" in SO_INV.columns:
        g["On Hand - WIP"] = SO_INV.loc[g.index, "In Stock(Inventory)"]
    if "Ship Date" in g.columns:
        ship_dates = pd.to_datetime(g["Ship Date"], errors="coerce")
        g = (
            g.assign(_ship_date_sort=ship_dates)
            .sort_values("_ship_date_sort", na_position="last")
            .drop(columns="_ship_date_sort")
        )
        g["Ship Date"] = _to_date_str(g["Ship Date"])
    rows = g[need_cols].fillna("").astype(str).to_dict(orient="records") if not g.empty else []
    totals = {"on_sales_order": None, "on_po": None}
    if not g.empty:
        if "On Sales Order" in g.columns:
            totals["on_sales_order"] = _aggregate_metric(g["On Sales Order"])
        if "On PO" in g.columns:
            totals["on_po"] = _aggregate_metric(g["On PO"])
    return need_cols, rows, totals

def _so_table_for_so(so_num: str, item: str | None = None) -> tuple[list[str], list[dict]]:
    need_cols = ["Name", "QB Num", "Item", "Qty(-)", "On Hand - WIP", "Ship Date", "Picked"]
    g = SO_INV.copy()
    mask = g["QB Num"].astype(str).str.upper() == so_num.upper()
    if item:
        mask &= g["Item"].astype(str) == item
    g = g.loc[mask].copy()
    for c in need_cols:
        if c not in g.columns:
            g[c] = ""
    # Fallback for WIP column if missing in data
    if "On Hand - WIP" not in g.columns and "In Stock(Inventory)" in g.columns:
        g["On Hand - WIP"] = g["In Stock(Inventory)"]
    if "Ship Date" in g.columns:
        ship_dates = pd.to_datetime(g["Ship Date"], errors="coerce")
        g = (
            g.assign(_ship_date_sort=ship_dates)
            .sort_values("_ship_date_sort", na_position="last")
            .drop(columns="_ship_date_sort")
        )
        g["Ship Date"] = _to_date_str(g["Ship Date"])
    rows = g[need_cols].fillna("").astype(str).to_dict(orient="records") if not g.empty else []
    return need_cols, rows

def _compute_on_hand_metrics(df: pd.DataFrame) -> tuple[int | float | None, int | float | None]:
    if df is None or df.empty:
        return None, None
    on_hand = _aggregate_metric(df.get("On Hand", pd.Series(dtype=float)))
    # Fall back if "On Hand - WIP" is missing
    col_wip = "On Hand - WIP" if "On Hand - WIP" in df.columns else ("In Stock(Inventory)" if "In Stock(Inventory)" in df.columns else None)
    on_hand_wip = _aggregate_metric(df.get(col_wip, pd.Series(dtype=float))) if col_wip else None
    return on_hand, on_hand_wip

def _po_table_for_item(item: str) -> tuple[list[str], list[dict]]:
    if "Item" not in NAV.columns:
        raise ValueError("NAV table missing 'Item' column.")
    item_lower = item.lower()
    item_upper = item.upper()
    nav_item_series = NAV["Item"].astype(str)
    mask = nav_item_series.str.lower() == item_lower
    allow_desc_lookup = not item_upper.startswith(("N", "SEMIL", "POC"))
    if allow_desc_lookup and "Description" in NAV.columns:
        desc_mask = NAV["Description"].astype(str).str.lower().str.contains(item_lower, na=False)
        mask |= desc_mask
    g = NAV[mask].copy()
    for dc in ("Ship Date", "Order Date", "ETA"):
        if dc in g.columns:
            g[dc] = _to_date_str(g[dc])
    cols = list(g.columns) if not g.empty else list(NAV.columns)
    g = g.fillna("").astype(str)
    rows = g[cols].to_dict(orient="records") if not g.empty else []
    return cols, rows

def _open_po_table_for_item(item: str) -> tuple[list[str], list[dict]]:
    if OPEN_PO is None or OPEN_PO.empty:
        return [], []

    item_lower = item.lower()
    item_upper = item.upper()

    df = OPEN_PO
    item_col = next((c for c in df.columns if c.lower() == "item"), None)
    desc_col = next((c for c in df.columns if c.lower() == "description"), None)

    if item_col is None and desc_col is None:
        return list(df.columns), []

    mask = pd.Series(False, index=df.index)
    if item_col:
        mask |= df[item_col].astype(str).str.lower() == item_lower

    allow_desc_lookup = not item_upper.startswith(("N", "SEMIL", "POC"))
    if allow_desc_lookup and desc_col:
        mask |= df[desc_col].astype(str).str.lower().str.contains(item_lower, na=False)

    result = df.loc[mask].copy()
    if result.empty:
        return list(df.columns), []

    for col in result.columns:
        if "date" in col.lower():
            _safe_date_col(result, col)

    result = result.fillna("").astype(str)
    return list(result.columns), result.to_dict(orient="records")

# initial load
_load_from_db(force=True)

# =========================
# Routes
# =========================
@app.route("/", methods=["GET", "POST"])
def index():
    if request.args.get("reload") == "1":
        _load_from_db(force=True)
        _load_pdf_map(force=True)
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    # ---- read inputs (work with GET or POST) ----
    so_input = (request.values.get("so") or "").strip()
    customer_input = (request.values.get("customer") or "").strip()

    # Flexible SO handling: allow "20251368", "SO20251368", or "so-20251368"
    so_num = ""
    so_upper = so_input.upper()
    if so_upper:
        if so_upper.startswith("SO-"):
            so_num = so_upper
        elif so_upper.startswith("SO") and so_upper[2:].replace("-", "").isdigit():
            so_num = f"SO-{so_upper[2:].lstrip('-')}"
        elif so_upper.replace("-", "").isdigit():
            so_num = f"SO-{so_upper.replace('-', '')}"

    rows, count = None, 0
    order_summary = None
    table_headers = None
    customer_options = None
    customer_query = None
    if so_input:
        rows_df = pd.DataFrame()

        if so_num:
            mask = SO_INV["QB Num"].astype(str).str.upper() == so_num
            rows_df = SO_INV.loc[mask].copy()

        if (rows_df is None or rows_df.empty) and "Name" in SO_INV.columns:
            name_mask = SO_INV["Name"].astype(str).str.contains(so_input, case=False, na=False)
            rows_df = SO_INV.loc[name_mask].copy()

        count = len(rows_df)

        if "On Hand - WIP" not in rows_df.columns and "In Stock(Inventory)" in rows_df.columns:
            rows_df["On Hand - WIP"] = rows_df["In Stock(Inventory)"]

        required_headers = [
            "Order Date","Name","P. O. #","QB Num","Item","Qty(-)","Available",
            "Available + On PO","Available + Pre-installed PO","On Hand","On Sales Order","On PO",
            "Assigned Q'ty","On Hand - WIP","Sales/Week",
            "Recommended Restock Qty","Component_Status","Ship Date"
        ]
        for h in required_headers:
            if h not in rows_df.columns: rows_df[h] = ""

        qb_for_pdf = None
        if "QB Num" in rows_df.columns:
            qb_vals = rows_df["QB Num"].dropna().astype(str).unique().tolist()
            if len(qb_vals) == 1:
                qb_for_pdf = qb_vals[0]

        summary_cols = ["Order Date", "Name", "P. O. #", "QB Num", "Ship Date"]
        summary_fields = []
        for col in summary_cols:
            col_vals = rows_df[col].dropna().astype(str) if col in rows_df.columns else pd.Series(dtype=str)
            summary_fields.append({
                "label": col,
                "value": col_vals.iloc[0] if not col_vals.empty else "",
            })
        order_summary = {
            "qb_num": qb_for_pdf or so_input,
            "row_count": count,
            "fields": summary_fields,
        }

        # Attach PDF link by DB search first (ILIKE on filename); fallback to filesystem map
        po_num = ""
        if "P. O. #" in rows_df.columns:
            ser = rows_df["P. O. #"].dropna().astype(str)
            po_num = ser.iloc[0] if not ser.empty else ""

        search_keys = []
        if qb_for_pdf:
            search_keys.append(qb_for_pdf)
            qb_upper = qb_for_pdf.upper()
            if qb_upper.startswith("SO-"):
                search_keys.append(qb_upper[3:])  # numeric only
            if po_num:
                search_keys.append(str(po_num))

        pdf_record = None
        if search_keys:
            for key in search_keys:
                recs = _pdf_db_search_by_filename(key, limit=1)
                if recs:
                    pdf_record = recs[0]
                    break

        if pdf_record:
            order_summary["pdf_url"] = f"/pdfid/{pdf_record['id']}"
            order_summary["pdf_name"] = pdf_record.get("file_name")
        else:
            # Fallback to filesystem map (exact filename stems)
            keys_to_try = []
            if qb_for_pdf:
                keys_to_try = [qb_for_pdf, qb_for_pdf.replace("SO-", ""), qb_for_pdf.replace("SO", "").strip("- ")]
            pdf_info = None
            for k in keys_to_try:
                pdf_info = PDF_MAP.get(k.upper())
                if pdf_info:
                    break
            if pdf_info:
                order_summary["pdf_url"] = f"/pdf/{(pdf_info['file_name'][:-4])}"
                order_summary["pdf_name"] = pdf_info["file_name"]

        for c in ("Ship Date", "Order Date"):
            if c in rows_df.columns: rows_df[c] = _to_date_str(rows_df[c])
        table_headers = [h for h in required_headers if h not in ("Order Date","Name","P. O. #","QB Num","Ship Date")]
        table_df = rows_df[table_headers].copy()
        rows = table_df.fillna("").astype(str).to_dict(orient="records")
    elif customer_input:
        customer_query = customer_input
        customer_options = []
        if "Name" in SO_INV.columns:
            name_mask = SO_INV["Name"].astype(str).str.contains(customer_input, case=False, na=False)
            cust_df = SO_INV.loc[name_mask].copy()
            if not cust_df.empty:
                if "QB Num" not in cust_df.columns:
                    cust_df["QB Num"] = ""
                for c in ("Ship Date", "Order Date"):
                    if c in cust_df.columns:
                        cust_df[c] = _to_date_str(cust_df[c])
                for qb_num, grp in cust_df.groupby("QB Num"):
                    qb_str = str(qb_num).strip()
                    if not qb_str:
                        continue
                    first = grp.iloc[0]
                    customer_options.append({
                        "qb_num": qb_str,
                        "name": first.get("Name", ""),
                        "ship_date": first.get("Ship Date", ""),
                        "order_date": first.get("Order Date", ""),
                    })
                customer_options.sort(key=lambda x: x.get("qb_num", ""))

    return render_template_string(
        INDEX_TPL,
        so_num=so_input,           # show original entry
        customer_val=customer_input,
        customer_query=customer_query,
        customer_options=customer_options,
        rows=rows,
        count=count,
        loaded_at=_LAST_LOADED_AT.strftime("%Y-%m-%d %H:%M:%S") if _LAST_LOADED_AT else "—",
        order_summary=order_summary,
        headers=table_headers,
        header_labels=TABLE_HEADER_LABELS,
        numeric_cols=[
            "Qty(-)","Available","Available + Pre-installed PO","On Hand",
            "On Sales Order","On PO","Assigned Q'ty","On Hand - WIP",
            "Available + On PO","Sales/Week","Recommended Restock Qty"
        ],
    )

@app.route("/api/reload", methods=["POST"])
def api_reload():
    _load_from_db(force=True)
    _load_pdf_map(force=True)
    if _LAST_LOAD_ERR:
        return jsonify({"ok": False, "error": _LAST_LOAD_ERR}), 500
    return jsonify({"ok": True, "loaded_at": _LAST_LOADED_AT.isoformat()})

@app.route("/pdf/<order_id>")
def serve_pdf(order_id: str):
    """Serve a PDF by order id (stem of filename). Only serves files under PDF_FOLDER.
    """
    _load_pdf_map()
    if not PDF_FOLDER:
        abort(404)
    info = PDF_MAP.get(order_id.upper())
    if not info:
        # try variants: with SO- prefix or stripped
        variants = [order_id, f"SO-{order_id}", order_id.replace("SO-", ""), order_id.replace("SO", "").strip("- ")]
        for v in variants:
            info = PDF_MAP.get(v.upper())
            if info:
                break
    if not info:
        abort(404)
    path = info["file_path"]
    if not os.path.isfile(path):
        abort(404)
    # Send file directly; let browser handle PDF
    return send_file(path, mimetype="application/pdf", as_attachment=False, download_name=info["file_name"])

@app.route("/favicon.ico")
def favicon():
    """Serve favicon from static if present, else fallback to inline SVG."""
    static_ico = os.path.join(app.root_path, "static", "favicon.ico")
    if os.path.isfile(static_ico):
        return send_file(static_ico, mimetype="image/x-icon")
    # Fallback simple SVG
    svg = (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'>"
        "<rect width='64' height='64' rx='12' fill='#0d6efd'/>"
        "<text x='50%' y='52%' dominant-baseline='middle' text-anchor='middle'"
        " font-family='Segoe UI, Roboto, Arial, sans-serif' font-size='34' fill='white'>LT</text>"
        "</svg>"
    )
    return Response(svg, mimetype="image/svg+xml")

@app.route("/pdfid/<int:pdf_id>")
def serve_pdf_by_id(pdf_id: int):
    """Serve a PDF using a record in pdf_file_log by id."""
    rec = _pdf_db_get_by_id(pdf_id)
    if not rec:
        abort(404)
    path = rec.get("file_path")
    name = rec.get("file_name") or os.path.basename(path or "") or f"file-{pdf_id}.pdf"
    if not path or not os.path.isfile(path):
        abort(404)
    return send_file(path, mimetype="application/pdf", as_attachment=False, download_name=name)

@app.route("/api/pdf_search")
def api_pdf_search():
    q = (request.args.get("q") or request.args.get("query") or "").strip()
    if not q:
        return jsonify({"ok": False, "error": "Missing query"}), 400
    rows = _pdf_db_search_by_filename(q, limit=50)
    return jsonify({"ok": True, "count": len(rows), "rows": rows})

@app.route("/api/item_overview")
def api_item_overview():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return jsonify({"ok": False, "error": _LAST_LOAD_ERR}), 503

    item = (request.args.get("item") or "").strip()
    if not item:
        abort(400, "Missing item")

    columns_so, rows_so, so_totals = _so_table_for_item(item)
    try:
        columns_po, rows_po = _po_table_for_item(item)
        open_po_cols, open_po_rows = _open_po_table_for_item(item)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500

    on_po_val = lookup_on_po_by_item(item)

    return jsonify(
        {
            "ok": True,
            "item": item,
            "so": {
                "columns": columns_so,
                "rows": rows_so,
                "total_on_sales": so_totals.get("on_sales_order"),
                "total_on_po": so_totals.get("on_po"),
            },
            "po": {
                "columns": columns_po,
                "rows": rows_po,
            },
            "open_po": {
                "columns": open_po_cols,
                "rows": open_po_rows,
            },
            "on_po_label": on_po_val,
        }
    )

@app.route("/so_lines")
def so_lines():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    item = (request.args.get("item") or "").strip()
    if not item:
        abort(400, "Missing item")

    columns, rows, _ = _so_table_for_item(item)

    on_po_val = lookup_on_po_by_item(item)

    return render_template_string(
        SUBPAGE_TPL,
        title=f"On Sales Order — {item}",
        columns=columns,
        rows=rows,
        extra_note="Source: public.wo_structured",
        on_po=on_po_val,
        open_po_columns=[],
        open_po_rows=[],
        extra_note_open_po='Source: public.Open_Purchase_Orders',
    )

@app.route("/po_lines")
def po_lines():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    item = (request.args.get("item") or "").strip()
    if not item:
        abort(400, "Missing item")

    try:
        cols, rows = _po_table_for_item(item)
        open_cols, open_rows = _open_po_table_for_item(item)
    except ValueError as exc:
        return render_template_string(ERR_TPL, error=str(exc)), 500

    on_po_val = lookup_on_po_by_item(item)

    return render_template_string(
        SUBPAGE_TPL,
        title=f"On PO — {item}",
        columns=cols,
        rows=rows,
        extra_note='Source: public."NT Shipping Schedule"',
        on_po=on_po_val,
        open_po_columns=open_cols,
        open_po_rows=open_rows,
        extra_note_open_po='Source: public.Open_Purchase_Orders',
    )

@app.route("/item_details")
def item_details():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    item = (request.args.get("item") or "").strip()
    if not item:
        abort(400, "Missing item")

    columns_so, rows_so, so_totals = _so_table_for_item(item)
    try:
        columns_po, rows_po = _po_table_for_item(item)
        open_po_cols, open_po_rows = _open_po_table_for_item(item)
    except ValueError as exc:
        return render_template_string(ERR_TPL, error=str(exc)), 500

    on_po_val = lookup_on_po_by_item(item)

    return render_template_string(
        ITEM_TPL,
        item=item,
        on_po=on_po_val,
        so_columns=columns_so,
        so_rows=rows_so,
        po_columns=columns_po,
        po_rows=rows_po,
        open_po_columns=open_po_cols,
        open_po_rows=open_po_rows,
        extra_note_so="Source: public.wo_structured",
        extra_note_po='Source: public."NT Shipping Schedule"',
        extra_note_open_po='Source: public.Open_Purchase_Orders',
        so_total_on_sales=so_totals.get("on_sales_order"),
        so_total_on_po=so_totals.get("on_po"),
    )

@app.route("/inventory_count")
def inventory_count():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    if request.args.get("reload") == "1":
        _load_from_db(force=True)

    so_input = (request.values.get("so") or "").strip()
    item_input = (request.values.get("item") or "").strip()

    so_num = so_input.upper()
    if so_num and not so_num.startswith("SO-"):
        so_num = f"SO-{so_num}"

    so_columns: list[str] | None = None
    so_rows: list[dict] | None = None
    on_hand: int | float | None = None
    on_hand_wip: int | float | None = None
    open_po_columns: list[str] | None = None
    open_po_rows: list[dict] | None = None

    filtered_df = SO_INV.copy()
    if item_input:
        filtered_df = filtered_df[filtered_df["Item"].astype(str) == item_input]
    if so_num:
        filtered_df = filtered_df[filtered_df["QB Num"].astype(str).str.upper() == so_num]

    if not filtered_df.empty:
        on_hand, on_hand_wip = _compute_on_hand_metrics(filtered_df)

    # Build the "On Sales Order" table depending on provided filters
    if item_input:
        so_columns, so_rows, _ = _so_table_for_item(item_input)
        # If SO also provided, further filter rows to that SO
        if so_num and so_rows:
            so_rows = [r for r in so_rows if str(r.get("QB Num", "")).upper() == so_num]
        # Add extra columns for Inventory module view
        on_so_val = lookup_on_sales_by_item(item_input)
        on_po_val = lookup_on_po_by_item(item_input)
        if so_rows is None:
            so_rows = []
        for r in so_rows:
            if on_so_val is not None:
                r["On SO"] = on_so_val
            if on_po_val is not None:
                r["On PO"] = on_po_val
        if so_columns is None:
            so_columns = []
        for extra in ("On SO", "On PO"):
            if extra not in so_columns:
                so_columns.append(extra)
        # Also load Open Purchase Orders table for the item
        try:
            open_po_columns, open_po_rows = _open_po_table_for_item(item_input)
        except Exception:
            open_po_columns, open_po_rows = [], []
    elif so_num:
        so_columns, so_rows = _so_table_for_so(so_num)
    else:
        so_columns, so_rows = [], []

    return render_template_string(
        INVENTORY_TPL,
        loaded_at=_LAST_LOADED_AT.strftime("%Y-%m-%d %H:%M:%S") if _LAST_LOADED_AT else "�?",
        so_val=so_input,
        item_val=item_input,
        on_hand=on_hand,
        on_hand_wip=on_hand_wip,
        so_columns=so_columns,
        so_rows=so_rows,
        open_po_columns=open_po_columns or [],
        open_po_rows=open_po_rows or [],
    )


@app.route("/production_planning")
def production_planning():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    if request.args.get("reload") == "1":
        _load_from_db(force=True)

    if FINAL_SO is None or FINAL_SO.empty:
        return render_template_string(ERR_TPL, error="No final_sales_order data available."), 503

    df = FINAL_SO.copy()
    if "Lead Time" not in df.columns:
        return render_template_string(ERR_TPL, error="final_sales_order missing 'Lead Time' column."), 500

    df["Lead Time"] = pd.to_datetime(df["Lead Time"], errors="coerce")
    df = df.dropna(subset=["Lead Time"])
    if df.empty:
        return render_template_string(ERR_TPL, error="No valid Lead Time rows in final_sales_order."), 503

    df["lead_date_str"] = df["Lead Time"].dt.strftime("%Y-%m-%d")

    date_groups: list[dict] = []
    for date_str, date_group in df.sort_values(["Lead Time", "QB Num"]).groupby(
        "lead_date_str", sort=True
    ):
        orders: list[dict] = []
        for qb_num, so_group in date_group.groupby("QB Num"):
            first = so_group.iloc[0]
            customer = first.get("Customer") or first.get("Name") or ""
            qty_val = first.get("Qty")
            try:
                qty_float = float(qty_val)
                qty_str = str(int(qty_float)) if qty_float.is_integer() else str(qty_float)
            except Exception:
                qty_str = str(qty_val) if qty_val is not None else ""
            item_name = first.get("Item") or ""
            line = f"{item_name} x {qty_str}".strip()
            po_num = first.get("Customer PO") or first.get("P. O. #") or ""
            pdf_url = _find_pdf_url_for_so(str(qb_num), po_num)
            orders.append(
                {
                    "qb_num": str(qb_num),
                    "customer": customer,
                    "line": line,
                    "pdf_url": pdf_url,
                }
            )
        orders.sort(key=lambda r: r["qb_num"])
        date_groups.append({"date": date_str, "orders": orders})

    date_groups.sort(key=lambda g: g["date"])

    return render_template_string(
        PRODUCTION_TPL,
        loaded_at=_LAST_LOADED_AT.strftime("%Y-%m-%d %H:%M:%S") if _LAST_LOADED_AT else "",
        date_groups=date_groups,
    )

@app.route("/api/item_suggest")
def api_item_suggest():
    _ensure_loaded()
    q = (request.args.get("q") or request.args.get("query") or "").strip()
    if not q:
        return jsonify({"ok": True, "items": []})
    try:
        items = SO_INV["Item"].astype(str).dropna().unique().tolist()
        ql = q.lower()
        starts = [i for i in items if i.lower().startswith(ql)]
        contains = [i for i in items if ql in i.lower() and i not in starts]
        out = (starts + contains)[:20]
        return jsonify({"ok": True, "items": out})
    except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/quotation_lookup")
def quotation_lookup():
    _ensure_loaded()
    if _LAST_LOAD_ERR:
        return render_template_string(ERR_TPL, error=_LAST_LOAD_ERR), 503

    item_input = (request.values.get("item") or "").strip()
    qty_raw = (request.values.get("qty") or "").strip()
    try:
        qty_val = int(float(qty_raw)) if qty_raw else 1
        if qty_val <= 0:
            qty_val = 1
    except ValueError:
        qty_val = 1

    ledger_columns: list[str] = []
    ledger_rows: list[dict] = []
    opening_qty = None
    earliest_atp = None

    if item_input and LEDGER is not None and not LEDGER.empty:
        df = LEDGER.copy()
        df_item = df.loc[df["Item"].astype(str) == item_input].copy()
        if not df_item.empty:
            # Opening snapshot:
            # 1) Prefer explicit OPEN rows; 2) if none, fall back to any Opening values.
            if "Opening" in df_item.columns:
                open_rows = df_item.loc[df_item["Kind"].astype(str) == "OPEN"].copy()
                if not open_rows.empty:
                    open_rows = open_rows.sort_values("Date")
                    opening_qty = _aggregate_metric(open_rows["Opening"])
                else:
                    opening_series = pd.to_numeric(df_item["Opening"], errors="coerce").dropna()
                    if not opening_series.empty:
                        opening_qty = _aggregate_metric(opening_series)

            # Prepare ledger table rows (date, kind, delta, projected)
            keep_cols: list[str] = []
            for c in (
                "Date",
                "Kind",
                "Source",
                "Delta",
                "Projected_NAV",
                "NAV_before",
                "NAV_after",
                "QB Num",
                "P. O. #",
                "Name",
            ):
                if c in df_item.columns and c not in keep_cols:
                    keep_cols.append(c)

            if keep_cols:
                df_item = df_item.sort_values(["Date", "Kind"])
                date_vals = pd.to_datetime(df_item["Date"], errors="coerce")
                df_item["Date"] = date_vals.dt.strftime("%Y-%m-%d")

                # Determine minimum Projected_NAV for highlighting (after sort so alignment is correct)
                min_nav_value = None
                proj_series = None
                if "Projected_NAV" in df_item.columns:
                    proj_series = pd.to_numeric(df_item["Projected_NAV"], errors="coerce")
                    if not proj_series.dropna().empty:
                        min_nav_value = proj_series.min()

                if "Projected_NAV" in df_item.columns:
                    df_item.rename(columns={"Projected_NAV": "Projected_Qty"}, inplace=True)
                    keep_cols = ["Projected_Qty" if c == "Projected_NAV" else c for c in keep_cols]

                for c in ("Delta", "Projected_NAV", "NAV_before", "NAV_after"):
                    if c in df_item.columns:
                        df_item[c] = df_item[c].apply(_format_intish)
                if "Projected_Qty" in df_item.columns:
                    df_item["Projected_Qty"] = df_item["Projected_Qty"].apply(_format_intish)

                records = df_item[keep_cols].fillna("").astype(str).to_dict(orient="records")

                # Attach _is_min_nav flag for UI highlighting
                if min_nav_value is not None and proj_series is not None:
                    proj_vals = proj_series.tolist()
                    for rec, v in zip(records, proj_vals):
                        try:
                            fv = float(v)
                        except Exception:
                            fv = None
                        rec["_is_min_nav"] = (fv == float(min_nav_value))
                else:
                    for rec in records:
                        rec["_is_min_nav"] = False

                ledger_columns = keep_cols
                ledger_rows = records


        earliest_atp_dt = _lookup_earliest_atp_date(item_input, qty=qty_val)
        if earliest_atp_dt is not None:
            earliest_atp = earliest_atp_dt.strftime("%Y-%m-%d")
        else:
            earliest_atp = "Out of Stock"

    return render_template_string(
        QUOTE_TPL,
        item_val=item_input,
        qty_val=qty_val,
        opening_qty=opening_qty,
        earliest_atp=earliest_atp,
        ledger_columns=ledger_columns,
        ledger_rows=ledger_rows,
        loaded_at=_LAST_LOADED_AT.strftime("%Y-%m-%d %H:%M:%S") if _LAST_LOADED_AT else "",
    )

if __name__ == "__main__":
    # Flask dev server
    # Preload PDF map on startup for faster first-hit
    _load_pdf_map(force=True)
    app.run(debug=True, host="0.0.0.0", port=5002)
