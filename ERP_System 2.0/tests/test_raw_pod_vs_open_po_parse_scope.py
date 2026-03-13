from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import POD_FILE  # noqa: E402
from core import _norm_key, normalize_item  # noqa: E402
from db_config import get_engine  # noqa: E402


EPS = 1e-9
SCHEMA = "public"
POD_TABLE = "Open_Purchase_Orders"


def _short(df: pd.DataFrame, n: int = 30) -> str:
    if df.empty:
        return "(none)"
    return df.head(n).to_string(index=False)


def _load_raw_pod_source() -> pd.DataFrame:
    raw = pd.read_csv(str(POD_FILE), encoding="ISO-8859-1", engine="python")
    raw = raw.copy()

    first_col = raw.columns[0] if len(raw.columns) > 0 else None
    if first_col is not None and first_col in raw.columns:
        labels = (
            raw[first_col]
            .astype(str)
            .str.replace("\u00A0", " ", regex=False)
            .str.strip()
        )
        labels = labels.mask(labels.str.lower().isin(["nan", "none", ""]))
        is_total = labels.str.match(r"(?i)^total\b", na=False)
        is_header = labels.notna() & ~is_total

        current_item: str | None = None
        section_item: list[str | None] = []
        for lbl, total, header in zip(labels.tolist(), is_total.tolist(), is_header.tolist()):
            if total:
                current_item = None
            elif header:
                current_item = str(lbl).strip()
            section_item.append(current_item)
        raw["section_item"] = pd.Series(section_item, index=raw.index, dtype="string")
    else:
        raw["section_item"] = pd.NA

    raw["pod_no"] = raw.get("Num", pd.Series("", index=raw.index)).astype(str)
    raw["pod_no"] = raw["pod_no"].str.split("(", expand=True)[0].str.strip()
    raw["qty"] = pd.to_numeric(raw.get("Backordered", 0), errors="coerce").fillna(0.0)
    raw["memo"] = raw.get("Memo", pd.Series("", index=raw.index)).astype(str).str.strip()

    memo_item = raw["memo"].str.split(" ", expand=True)[0]
    memo_item = pd.Series(memo_item, index=raw.index, dtype="string").str.replace("*", "", regex=False).str.strip()
    raw["item"] = raw["section_item"].fillna(memo_item)
    raw["item"] = raw["item"].astype(str).str.strip()
    raw["item"] = raw["item"].map(normalize_item)
    raw["item_key"] = _norm_key(raw["item"])

    raw = raw.loc[raw["pod_no"].ne("") & raw["item_key"].ne("NAN") & (raw["qty"] > 0)].copy()
    return raw


def _load_parsed_open_po(eng) -> pd.DataFrame:
    pod = pd.read_sql(
        f"""
        SELECT
            "POD#" AS pod_no,
            "Item" AS item,
            "Qty(+)" AS qty
        FROM public."{POD_TABLE}"
        """,
        eng,
    )
    pod["pod_no"] = pod["pod_no"].fillna("").astype(str).str.strip()
    pod["item_key"] = _norm_key(pod["item"])
    pod["qty"] = pd.to_numeric(pod["qty"], errors="coerce").fillna(0.0)
    pod = pod.loc[pod["pod_no"].ne("") & pod["item_key"].ne("NAN") & (pod["qty"] > 0)].copy()
    return pod


def test_raw_pod_source_vs_parsed_open_purchase_orders() -> None:
    eng = get_engine()

    raw = _load_raw_pod_source()
    parsed = _load_parsed_open_po(eng)

    raw_item = raw.groupby("item_key", as_index=False)["qty"].sum().rename(columns={"qty": "raw_qty"})
    parsed_item = parsed.groupby("item_key", as_index=False)["qty"].sum().rename(columns={"qty": "parsed_qty"})

    raw_pods = (
        raw.loc[raw["pod_no"].ne(""), ["item_key", "pod_no"]]
        .drop_duplicates()
        .sort_values(["item_key", "pod_no"])
        .groupby("item_key")["pod_no"]
        .agg(", ".join)
        .to_dict()
    )

    cmp = raw_item.merge(parsed_item, on="item_key", how="outer")
    cmp["raw_qty"] = pd.to_numeric(cmp["raw_qty"], errors="coerce").fillna(0.0)
    cmp["parsed_qty"] = pd.to_numeric(cmp["parsed_qty"], errors="coerce").fillna(0.0)
    cmp["gap_qty"] = cmp["parsed_qty"] - cmp["raw_qty"]
    cmp["pod_list"] = cmp["item_key"].map(raw_pods).fillna("")

    dropped_in_parse = cmp.loc[cmp["gap_qty"] < -EPS].copy()
    dropped_in_parse["dropped_qty"] = dropped_in_parse["raw_qty"] - dropped_in_parse["parsed_qty"]
    dropped_in_parse = dropped_in_parse.sort_values("dropped_qty", ascending=False)

    extra_in_parse = cmp.loc[cmp["gap_qty"] > EPS].copy()
    extra_in_parse["extra_qty"] = extra_in_parse["parsed_qty"] - extra_in_parse["raw_qty"]
    extra_in_parse = extra_in_parse.sort_values("extra_qty", ascending=False)

    print("\n[Compare] raw POD source file vs parsed Open_Purchase_Orders")
    print(f"[Source file] {POD_FILE}")
    print("\n[Dropped in parse] raw POD qty > parsed Open_Purchase_Orders qty")
    print(_short(dropped_in_parse[["item_key", "raw_qty", "parsed_qty", "dropped_qty", "pod_list"]]))
    print("\n[Extra in parse] parsed Open_Purchase_Orders qty > raw POD qty")
    print(_short(extra_in_parse[["item_key", "raw_qty", "parsed_qty", "extra_qty", "pod_list"]]))

    assert extra_in_parse.empty, (
        "Parsed Open_Purchase_Orders has item quantity that exceeds the raw POD source."
    )
