from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from pod_allocation import ALLOCATION_COLUMNS, MANUAL_COLUMNS, _add_row_key, _prepare_current_allocation


SYSTEM_COLUMNS = [col for col in ALLOCATION_COLUMNS if col not in MANUAL_COLUMNS]
MAIN_FILE_NAME = "POD_allocation.xlsx"
DIFF_FILE_NAME = "POD_allocation_diff.xlsx"
SNAPSHOT_FILE_NAME = "POD_allocation_new_snapshot.xlsx"


def _prepare_snapshot(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=ALLOCATION_COLUMNS + ["row_key"])

    out = df.copy()
    for col in ALLOCATION_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA

    for col in [c for c in ALLOCATION_COLUMNS if c != "Qty(-)"]:
        out[col] = out[col].fillna("").astype(str).str.strip()
    out["Qty(-)"] = pd.to_numeric(out["Qty(-)"], errors="coerce").fillna(0.0)
    return _add_row_key(out[ALLOCATION_COLUMNS].copy())


def _stringify(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


@dataclass
class ReviewArtifacts:
    current_df: pd.DataFrame
    snapshot_df: pd.DataFrame
    merged_df: pd.DataFrame
    records: list[dict[str, Any]]
    summary: dict[str, int]


def build_review_artifacts(current_df: pd.DataFrame | None, snapshot_df: pd.DataFrame) -> ReviewArtifacts:
    current_k = _prepare_current_allocation(current_df)
    current_k = _add_row_key(current_k) if not current_k.empty else pd.DataFrame(columns=ALLOCATION_COLUMNS + ["row_key"])
    snapshot_k = _prepare_snapshot(snapshot_df)

    current_map = (
        current_k.drop_duplicates(subset=["row_key"], keep="last").set_index("row_key")[ALLOCATION_COLUMNS].to_dict("index")
        if not current_k.empty
        else {}
    )
    snapshot_map = (
        snapshot_k.drop_duplicates(subset=["row_key"], keep="last").set_index("row_key")[ALLOCATION_COLUMNS].to_dict("index")
        if not snapshot_k.empty
        else {}
    )

    merged = snapshot_k[ALLOCATION_COLUMNS + ["row_key"]].copy()
    if not current_k.empty:
        carry = current_k[["row_key"] + MANUAL_COLUMNS].drop_duplicates(subset=["row_key"], keep="last")
        merged = merged.merge(carry, on="row_key", how="left", suffixes=("", "_old"))
        for col in MANUAL_COLUMNS:
            old_col = f"{col}_old"
            if old_col in merged.columns:
                merged[col] = merged[old_col].fillna(merged[col]).fillna("").astype(str).str.strip()
                merged.drop(columns=[old_col], inplace=True)

    merged = merged[ALLOCATION_COLUMNS + ["row_key"]].copy()
    merged_map = merged.set_index("row_key")[ALLOCATION_COLUMNS].to_dict("index") if not merged.empty else {}

    records: list[dict[str, Any]] = []
    for row_key in sorted(set(current_map) | set(snapshot_map)):
        old_row = current_map.get(row_key)
        new_row = snapshot_map.get(row_key)
        final_row = merged_map.get(row_key)

        if old_row is None:
            status = "ADDED"
            changed_cols = SYSTEM_COLUMNS.copy()
        elif new_row is None:
            status = "REMOVED"
            changed_cols = SYSTEM_COLUMNS.copy()
        else:
            changed_cols = [
                col for col in SYSTEM_COLUMNS
                if _stringify(old_row.get(col, "")) != _stringify(new_row.get(col, ""))
            ]
            status = "CHANGED" if changed_cols else "UNCHANGED"

        display_row = new_row if new_row is not None else old_row if old_row is not None else {}
        record = {
            "row_key": row_key,
            "status": status,
            "changed_cols": changed_cols,
            "changed_cols_label": ", ".join(changed_cols) if changed_cols else "",
            "qb_num": _stringify(display_row.get("QB Num", "")),
            "item": _stringify(display_row.get("Item", "")),
            "ship_date": _stringify(display_row.get("Ship Date", "")),
            "qty": _stringify(display_row.get("Qty(-)", "")),
            "old_row": {col: _stringify((old_row or {}).get(col, "")) for col in ALLOCATION_COLUMNS},
            "new_row": {col: _stringify((new_row or {}).get(col, "")) for col in ALLOCATION_COLUMNS},
            "final_row": {col: _stringify((final_row or {}).get(col, "")) for col in ALLOCATION_COLUMNS},
        }
        records.append(record)

    summary = {
        "added": sum(1 for r in records if r["status"] == "ADDED"),
        "removed": sum(1 for r in records if r["status"] == "REMOVED"),
        "changed": sum(1 for r in records if r["status"] == "CHANGED"),
        "unchanged": sum(1 for r in records if r["status"] == "UNCHANGED"),
        "final_rows": int(len(merged)),
    }

    return ReviewArtifacts(
        current_df=current_k,
        snapshot_df=snapshot_k,
        merged_df=merged,
        records=records,
        summary=summary,
    )


def _excel_master_path(output_dir: str | Path) -> Path:
    return Path(output_dir) / MAIN_FILE_NAME


def _excel_diff_path(output_dir: str | Path) -> Path:
    return Path(output_dir) / DIFF_FILE_NAME


def _excel_snapshot_path(output_dir: str | Path) -> Path:
    return Path(output_dir) / SNAPSHOT_FILE_NAME


def load_excel_master(output_dir: str | Path) -> pd.DataFrame | None:
    path = _excel_master_path(output_dir)
    if not path.exists():
        return None
    df = pd.read_excel(path, sheet_name="Main")
    return _prepare_current_allocation(df)


def _write_excel_master(df: pd.DataFrame, output_dir: str | Path) -> Path:
    path = _excel_master_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = _prepare_current_allocation(df)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="Main", index=False)
    return path


def _write_snapshot_excel(df: pd.DataFrame, output_dir: str | Path) -> Path:
    path = _excel_snapshot_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = _prepare_current_allocation(df)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="New Snapshot", index=False)
    return path


def _build_diff_rows(records: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in records:
        if record["status"] == "UNCHANGED":
            continue
        base = {
            "status": record["status"],
            "row_key": record["row_key"],
            "Ship Date": record["ship_date"],
            "Name": record["new_row"].get("Name", "") or record["old_row"].get("Name", ""),
            "QB Num": record["qb_num"],
            "Item": record["item"],
            "Qty(-)": record["qty"],
            "changed_columns": record["changed_cols_label"],
        }
        if record["status"] == "ADDED":
            row = base.copy()
            row["old_value"] = ""
            row["new_value"] = "new row"
            rows.append(row)
            continue
        if record["status"] == "REMOVED":
            row = base.copy()
            row["old_value"] = "old row"
            row["new_value"] = ""
            rows.append(row)
            continue

        for col in record["changed_cols"]:
            row = base.copy()
            row["column"] = col
            row["old_value"] = record["old_row"].get(col, "")
            row["new_value"] = record["new_row"].get(col, "")
            rows.append(row)

    if not rows:
        rows.append(
            {
                "status": "NO_CHANGE",
                "row_key": "",
                "Ship Date": "",
                "Name": "",
                "QB Num": "",
                "Item": "",
                "Qty(-)": "",
                "changed_columns": "",
                "column": "",
                "old_value": "",
                "new_value": "",
            }
        )
    return pd.DataFrame(rows)


def _write_diff_excel(artifacts: ReviewArtifacts, output_dir: str | Path) -> Path:
    path = _excel_diff_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    summary_df = pd.DataFrame([artifacts.summary])
    diff_df = _build_diff_rows(artifacts.records)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        diff_df.to_excel(writer, sheet_name="Diff", index=False)
        artifacts.snapshot_df[ALLOCATION_COLUMNS].to_excel(writer, sheet_name="New Snapshot", index=False)
        artifacts.current_df[ALLOCATION_COLUMNS].to_excel(writer, sheet_name="Current Main", index=False)
    return path


def review_pod_allocation(
    current_df: pd.DataFrame | None,
    snapshot_df: pd.DataFrame,
    *,
    artifacts_dir: str | Path,
) -> pd.DataFrame:
    base = Path(artifacts_dir)
    base.mkdir(parents=True, exist_ok=True)

    excel_master_df = load_excel_master(base)
    source_df = excel_master_df if excel_master_df is not None else current_df
    artifacts = build_review_artifacts(source_df, snapshot_df)

    if excel_master_df is None:
        _write_excel_master(artifacts.merged_df[ALLOCATION_COLUMNS], base)

    master_path = _excel_master_path(base)
    diff_path = _write_diff_excel(artifacts, base)
    snapshot_path = _write_snapshot_excel(snapshot_df, base)

    print("")
    print(f"Editable POD allocation file: {master_path}")
    print(f"Diff report file: {diff_path}")
    print(f"New snapshot file: {snapshot_path}")
    print("Update POD_allocation.xlsx manually as needed. ETL will keep using the Excel main file as the source of truth.")
    print("")

    final_df = load_excel_master(base)
    if final_df is None:
        final_df = artifacts.merged_df[ALLOCATION_COLUMNS].copy()
    return final_df
