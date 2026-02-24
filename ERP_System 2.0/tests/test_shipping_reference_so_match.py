from __future__ import annotations

from pathlib import Path
import sys
import json
from uuid import uuid4
import re

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import SHIPPING_SCHEDULE_FILE  # noqa: E402
from io_ops import read_excel_safe  # noqa: E402
from core import transform_shipping, _norm_key  # noqa: E402
from ledger import expand_nav_preinstalled  # noqa: E402
from db_config import get_engine  # noqa: E402
from erp_normalize import normalize_item  # noqa: E402


SCHEMA = "public"
STRUCTURED_TABLE = "wo_structured"
RUN_LOG_TABLE = "qa_shipping_ref_so_runs"
DETAIL_LOG_TABLE = "qa_shipping_ref_so_details"
SO_RE = re.compile(r"SO-?\d{8}", re.IGNORECASE)


def _short(df: pd.DataFrame, n: int = 20) -> str:
    if df.empty:
        return "(none)"
    return df.head(n).to_string(index=False)


def _to_jsonable(val):
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    return val


def _extract_so(text: str) -> str:
    m = SO_RE.search(str(text))
    if not m:
        return ""
    so = m.group(0).upper()
    if not so.startswith("SO-"):
        so = "SO-" + so.replace("SO", "").replace("-", "")
    return so


def _write_log_to_db(engine, report: dict[str, pd.DataFrame], checks_passed: bool) -> str:
    run_id = str(uuid4())
    created_at = pd.Timestamp.utcnow()

    summary = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "created_at_utc": created_at,
                "passed": bool(checks_passed),
                "rows_with_so_reference_count": int(len(report["rows_with_so_reference"])),
                "so_reference_missing_in_structured_count": int(len(report["so_reference_missing_in_structured"])),
                "component_missing_for_existing_so_count": int(len(report["component_missing_for_existing_so"])),
                "suspicious_component_tokens_count": int(len(report["suspicious_component_tokens"])),
            }
        ]
    )
    summary.to_sql(RUN_LOG_TABLE, engine, schema=SCHEMA, if_exists="append", index=False, method="multi")

    pd.DataFrame(columns=["run_id", "created_at_utc", "bucket", "row_no", "row_json"]).to_sql(
        DETAIL_LOG_TABLE, engine, schema=SCHEMA, if_exists="append", index=False, method="multi"
    )

    rows = []
    for bucket, df in report.items():
        if df.empty:
            continue
        for idx, row in df.reset_index(drop=True).iterrows():
            payload = {str(k): _to_jsonable(v) for k, v in row.items()}
            rows.append(
                {
                    "run_id": run_id,
                    "created_at_utc": created_at,
                    "bucket": bucket,
                    "row_no": int(idx + 1),
                    "row_json": json.dumps(payload, ensure_ascii=True, default=str),
                }
            )
    if rows:
        pd.DataFrame(rows).to_sql(DETAIL_LOG_TABLE, engine, schema=SCHEMA, if_exists="append", index=False, method="multi")
    return run_id


def _build_report(engine) -> dict[str, pd.DataFrame]:
    raw = read_excel_safe(SHIPPING_SCHEDULE_FILE)
    if "Reference" not in raw.columns:
        raise AssertionError("Shipping schedule file does not contain a 'Reference' column.")

    ship = transform_shipping(raw)

    # Rebuild the same key fields from raw so we can carry Reference into transformed rows.
    src = raw.copy()
    for c in ["Customer PO No.", "Model Name", "Description", "Ship Date", "Confirmed Qty", "Reference"]:
        if c not in src.columns:
            src[c] = pd.NA
    src["QB Num"] = src["Customer PO No."].astype(str).str.split("(").str[0].str.strip()
    src["Item"] = src["Model Name"].astype(str).str.strip()
    src["Description"] = src["Description"].astype(str)
    src["Ship Date"] = pd.to_datetime(src["Ship Date"], errors="coerce")
    src["Qty(+)"] = pd.to_numeric(src["Confirmed Qty"], errors="coerce").fillna(0).astype(int)
    src["Reference"] = src["Reference"].astype(str).str.strip()
    src = src[["QB Num", "Item", "Description", "Ship Date", "Qty(+)", "Reference"]]

    ship_ref = ship.merge(src, on=["QB Num", "Item", "Description", "Ship Date", "Qty(+)"], how="left")
    nav_exp = expand_nav_preinstalled(ship_ref)
    nav_exp = nav_exp.loc[nav_exp["Pre/Bare"].astype(str).str.strip().str.upper().eq("PRE")].copy()
    nav_exp["IsParent"] = nav_exp["IsParent"].fillna(False).astype(bool)
    nav_exp["Reference"] = nav_exp["Reference"].fillna("").astype(str)
    nav_exp["SO_Ref"] = nav_exp["Reference"].map(_extract_so)

    rows_with_so = nav_exp.loc[nav_exp["SO_Ref"].ne("")].copy()
    component_rows = rows_with_so.loc[~rows_with_so["IsParent"]].copy()
    component_rows["item_key"] = _norm_key(component_rows["Item"].map(normalize_item))

    structured = pd.read_sql(
        f'SELECT "QB Num" AS qb_num, "Item" AS item FROM "{SCHEMA}"."{STRUCTURED_TABLE}"', engine
    )
    structured["SO_Ref"] = structured["qb_num"].fillna("").astype(str).str.strip().str.upper()
    structured["item_key"] = _norm_key(structured["item"].map(normalize_item))

    so_ref_set = set(structured["SO_Ref"].dropna().astype(str))
    so_missing = (
        component_rows[["SO_Ref", "Reference"]]
        .drop_duplicates()
        .loc[lambda d: ~d["SO_Ref"].isin(so_ref_set)]
        .sort_values("SO_Ref")
    )

    existing_component_rows = component_rows.loc[component_rows["SO_Ref"].isin(so_ref_set)].copy()
    merged = existing_component_rows.merge(
        structured[["SO_Ref", "item_key"]].drop_duplicates(),
        on=["SO_Ref", "item_key"],
        how="left",
        indicator=True,
    )
    missing_component = (
        merged.loc[merged["_merge"].ne("both"), ["SO_Ref", "Reference", "Item", "Parent_Item", "Description"]]
        .drop_duplicates()
        .sort_values(["SO_Ref", "Item"])
    )

    suspicious = component_rows.loc[
        component_rows["Item"].astype(str).str.contains(r"_x000D_|\\n|^\s*\d+\s*x\s+", case=False, regex=True, na=False),
        ["SO_Ref", "Reference", "Item", "Parent_Item", "Description"],
    ].drop_duplicates().sort_values(["SO_Ref", "Item"])

    return {
        "rows_with_so_reference": rows_with_so[["SO_Ref", "Reference", "Parent_Item", "Item", "IsParent"]],
        "so_reference_missing_in_structured": so_missing,
        "component_missing_for_existing_so": missing_component,
        "suspicious_component_tokens": suspicious,
    }


def test_shipping_reference_components_match_structured_so() -> None:
    engine = get_engine()
    report = _build_report(engine)

    checks_passed = (
        report["so_reference_missing_in_structured"].empty
        and report["component_missing_for_existing_so"].empty
    )
    run_id = _write_log_to_db(engine, report, checks_passed)

    print("\n[1] Expanded PRE rows with SO reference")
    print(_short(report["rows_with_so_reference"]))
    print("\n[2] SO references missing in wo_structured.QB Num")
    print(_short(report["so_reference_missing_in_structured"]))
    print("\n[3] Component rows missing for existing SO in wo_structured (possible parsing mismatch)")
    print(_short(report["component_missing_for_existing_so"]))
    print("\n[4] Suspicious parsed component tokens")
    print(_short(report["suspicious_component_tokens"]))
    print(f"\n[LOG] Supabase run_id: {run_id} (tables: {SCHEMA}.{RUN_LOG_TABLE}, {SCHEMA}.{DETAIL_LOG_TABLE})")

    rows = report["rows_with_so_reference"]
    assert rows["SO_Ref"].eq("SO-20251788").any(), (
        "SO-20251788 was not found in shipping Reference-expanded PRE rows."
    )

    so_mismatch = report["component_missing_for_existing_so"]
    so_20251788_mismatch = so_mismatch.loc[so_mismatch["SO_Ref"].eq("SO-20251788")]
    assert so_20251788_mismatch.empty, (
        "SO-20251788 has PRE components from shipping Reference that do not match wo_structured items."
    )

    assert report["component_missing_for_existing_so"].empty, (
        "Some PRE components from shipping (with SO reference) do not match wo_structured items for that SO."
    )
