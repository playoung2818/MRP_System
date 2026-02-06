from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from sqlalchemy import text

from atp import earliest_atp_strict
from config import DB_SCHEMA, TBL_INVENTORY, TBL_ITEM_ATP, TBL_STRUCTURED
from db_config import get_engine
from erp_normalize import normalize_item


@dataclass
class ToolResult:
    ok: bool
    data: dict[str, Any]
    trace: list[str]
    error: str | None = None


class DataCache:
    def __init__(self) -> None:
        self.engine = get_engine()
        self.inventory: pd.DataFrame | None = None
        self.item_atp: pd.DataFrame | None = None
        self.structured: pd.DataFrame | None = None
        self.loaded_at: datetime | None = None

    def _read_table(self, schema: str, table: str) -> pd.DataFrame:
        sql = text(f'SELECT * FROM "{schema}"."{table}"')
        return pd.read_sql_query(sql, con=self.engine)

    def reload(self) -> None:
        self.inventory = self._read_table(DB_SCHEMA, TBL_INVENTORY)
        self.item_atp = self._read_table(DB_SCHEMA, TBL_ITEM_ATP)
        self.structured = self._read_table(DB_SCHEMA, TBL_STRUCTURED)
        self.loaded_at = datetime.now()

    def ensure_loaded(self) -> None:
        if self.inventory is None or self.item_atp is None or self.structured is None:
            self.reload()


class OllamaClient:
    def __init__(self, base_url: str | None = None, model: str | None = None) -> None:
        self.base_url = base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        self.model = model or os.getenv("OLLAMA_MODEL", "llama3.1")

    def chat(self, system: str, user: str) -> str:
        url = self.base_url.rstrip("/") + "/api/chat"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": False,
        }
        resp = requests.post(url, json=payload, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("message", {}).get("content", "")


def _safe_json(text_in: str) -> dict[str, Any] | None:
    try:
        return json.loads(text_in)
    except Exception:
        pass
    match = re.search(r"\{.*\}", text_in, flags=re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except Exception:
        return None


def _parse_item_from_text(text_in: str) -> str | None:
    tokens = re.findall(r"[A-Za-z0-9][A-Za-z0-9\-_/+.]*", text_in)
    stop = {
        "how", "many", "right", "now", "and", "the", "is", "are", "atp", "available",
        "to", "promise", "what", "whats", "qty", "quantity", "on", "hand", "in", "stock",
    }
    candidates = [t for t in tokens if t.lower() not in stop and any(c.isdigit() for c in t)]
    if not candidates:
        return None
    candidates.sort(key=len, reverse=True)
    return candidates[0]


def _parse_qty_from_text(text_in: str) -> float | None:
    patterns = [
        r"\b(?:additional|add)\s+(\d+(?:\.\d+)?)\b",
        r"\bneed\s+(\d+(?:\.\d+)?)\b",
        r"\bfor\s+(\d+(?:\.\d+)?)\s*(?:pcs|pc|pieces|units)?\b",
        r"\b(\d+(?:\.\d+)?)\s*(?:pcs|pc|pieces|units)\b",
        r"\bqty\s*(\d+(?:\.\d+)?)\b",
        r"\bquantity\s*(\d+(?:\.\d+)?)\b",
    ]
    for pat in patterns:
        match = re.search(pat, text_in, flags=re.IGNORECASE)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                return None
    return None


def _parse_so_from_text(text_in: str) -> str | None:
    match = re.search(r"\bSO-?\d{8}\b", text_in, flags=re.IGNORECASE)
    if not match:
        return None
    so = match.group(0).upper()
    if not so.startswith("SO-"):
        so = "SO-" + so.replace("SO", "").replace("-", "")
    return so


def _format_number(val: Any) -> str:
    try:
        num = float(val)
    except Exception:
        return str(val)
    if num.is_integer():
        return str(int(num))
    return f"{num:.2f}"


def tool_inventory_snapshot(cache: DataCache, item: str) -> ToolResult:
    cache.ensure_loaded()
    inv = cache.inventory
    if inv is None or inv.empty:
        return ToolResult(False, {}, ["inventory: empty"], "inventory table is empty")

    key = normalize_item(item)
    trace = [f"inventory: item={key}"]
    part_col = "Part_Number" if "Part_Number" in inv.columns else "Item"
    df = inv.copy()
    df[part_col] = df[part_col].astype(str)
    row = df.loc[df[part_col] == key]
    if row.empty:
        return ToolResult(False, {}, trace, "item not found in inventory")

    rec = row.iloc[0].to_dict()
    data = {
        "item": key,
        "on_hand": rec.get("On Hand", 0),
        "on_hand_wip": rec.get("On Hand - WIP", rec.get("On Hand", 0)),
        "on_so": rec.get("On Sales Order", 0),
        "on_po": rec.get("On PO", 0),
        "available": rec.get("Available", rec.get("On Hand", 0)),
    }
    return ToolResult(True, data, trace)


def tool_atp_snapshot(cache: DataCache, item: str) -> ToolResult:
    cache.ensure_loaded()
    atp = cache.item_atp
    if atp is None or atp.empty:
        return ToolResult(False, {}, ["atp: empty"], "item_atp table is empty")

    key = normalize_item(item)
    trace = [f"atp: item={key}", f"atp: source={DB_SCHEMA}.{TBL_ITEM_ATP}"]
    df = atp.copy()
    df["Item"] = df["Item"].astype(str)
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.loc[(df["Item"] == key) & df["Date"].notna()]
    if df.empty:
        return ToolResult(False, {}, trace, "item not found in item_atp")

    today = pd.Timestamp.today().normalize()
    df_future = df.loc[df["Date"] >= today].sort_values("Date")
    if df_future.empty:
        df_future = df.sort_values("Date")
    first = df_future.iloc[0]
    future_min = pd.to_numeric(first.get("FutureMin_NAV"), errors="coerce")
    future_min = float(future_min) if pd.notna(future_min) else 0.0

    data = {
        "item": key,
        "atp_qty": future_min,
        "as_of": first["Date"].strftime("%Y-%m-%d"),
    }
    trace.append(f"atp: as_of={data['as_of']}")
    return ToolResult(True, data, trace)


def tool_earliest_atp_date(cache: DataCache, item: str, qty: float) -> ToolResult:
    cache.ensure_loaded()
    atp = cache.item_atp
    if atp is None or atp.empty:
        return ToolResult(False, {}, ["atp-date: empty"], "item_atp table is empty")

    key = normalize_item(item)
    trace = [
        f"atp-date: item={key}",
        f"atp-date: qty={qty}",
        "atp-date: rule=earliest Date where FutureMin_NAV >= qty",
        f"atp-date: source={DB_SCHEMA}.{TBL_ITEM_ATP}",
    ]
    dt = earliest_atp_strict(atp, key, qty)
    if dt is None:
        return ToolResult(False, {}, trace, "no feasible ATP date found")
    return ToolResult(True, {"item": key, "date": dt.strftime("%Y-%m-%d")}, trace)


def tool_so_waiting_items(cache: DataCache, so_num: str) -> ToolResult:
    cache.ensure_loaded()
    structured = cache.structured
    if structured is None or structured.empty:
        return ToolResult(False, {}, ["so: empty"], "structured table is empty")

    so = so_num.upper().strip()
    trace = [f"so: qb_num={so}", "so: status=Waiting"]
    df = structured.copy()
    if "QB Num" not in df.columns:
        return ToolResult(False, {}, trace, "structured table missing QB Num")
    if "Component_Status" not in df.columns:
        return ToolResult(False, {}, trace, "structured table missing Component_Status")
    df["QB Num"] = df["QB Num"].astype(str).str.upper()
    df["Component_Status"] = df["Component_Status"].astype(str)
    rows = df.loc[(df["QB Num"] == so) & (df["Component_Status"] == "Waiting")]
    if rows.empty:
        return ToolResult(True, {"so": so, "items": []}, trace)
    items = rows["Item"].dropna().astype(str).tolist() if "Item" in rows.columns else []
    return ToolResult(True, {"so": so, "items": items}, trace)


def interpret_question(text_in: str) -> dict[str, Any]:
    provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    if provider != "ollama":
        return {
            "intent": None,
            "item": None,
            "qty": None,
            "so": None,
            "error": "LLM parsing is disabled; set LLM_PROVIDER=ollama.",
        }
    system = (
        "You are an ERP assistant. Extract intent, item, qty, and so. "
        "Return ONLY a JSON object and nothing else. "
        "Required keys: intent, item, qty, so. "
        "intent must be one of: inventory_atp, atp_date, inventory_only, so_waiting. "
        "item must be the part number string as it appears in the question (or null). "
        "so must be the SO/QB number like SO-20260159 (or null). "
        "qty must be a number or null. "
        "Do not add extra keys or commentary."
    )
    client = OllamaClient()
    try:
        raw = client.chat(system, text_in)
        parsed = _safe_json(raw)
        if parsed and parsed.get("intent"):
            return parsed
        return {
            "intent": None,
            "item": None,
            "qty": None,
            "so": None,
            "error": "LLM returned invalid JSON.",
        }
    except Exception as exc:
        return {
            "intent": None,
            "item": None,
            "qty": None,
            "so": None,
            "error": f"LLM request failed: {exc}",
        }


def answer_question(cache: DataCache, text_in: str) -> dict[str, Any]:
    parsed = interpret_question(text_in)
    if parsed.get("error"):
        return {"ok": False, "answer": parsed["error"], "trace": ["parse: llm_error"]}
    intent = parsed.get("intent") or "inventory_atp"
    item = parsed.get("item")
    qty = parsed.get("qty")
    so = parsed.get("so")
    trace: list[str] = []
    out: dict[str, Any] = {"ok": True}
    if intent == "so_waiting":
        if not so:
            return {"ok": False, "answer": "Please provide an SO number.", "trace": ["parse: so missing"]}
        res = tool_so_waiting_items(cache, so)
        trace += res.trace
        if not res.ok:
            return {"ok": False, "answer": res.error or "SO lookup failed", "trace": trace}
        items = res.data.get("items", [])
        if not items:
            return {"ok": True, "answer": f"{so}: no items in Waiting status.", "trace": trace}
        out["answer"] = f"{so}: waiting items -> {', '.join(items)}."
        out["trace"] = trace
        return out

    if not item:
        return {
            "ok": False,
            "answer": "I could not find an item in that question.",
            "trace": ["parse: item not found"],
        }

    if intent == "inventory_only":
        inv = tool_inventory_snapshot(cache, item)
        trace += inv.trace
        if not inv.ok:
            return {"ok": False, "answer": inv.error or "inventory lookup failed", "trace": trace}
        data = inv.data
        out["answer"] = (
            f"{data['item']}: on hand {_format_number(data['on_hand'])}, "
            f"on hand - wip {_format_number(data['on_hand_wip'])}, "
            f"available {_format_number(data['available'])}."
        )
        out["trace"] = trace
        return out

    if intent == "atp_date":
        if qty is None:
            return {
                "ok": False,
                "answer": "Please provide a quantity for ATP date.",
                "trace": ["parse: qty missing"],
            }
        atp_date = tool_earliest_atp_date(cache, item, float(qty))
        trace += atp_date.trace
        if not atp_date.ok:
            return {"ok": False, "answer": atp_date.error or "ATP date lookup failed", "trace": trace}
        out["answer"] = f"{item}: earliest ATP date for qty {qty} is {atp_date.data['date']}."
        out["trace"] = trace
        return out

    inv = tool_inventory_snapshot(cache, item)
    atp = tool_atp_snapshot(cache, item)
    trace += inv.trace + atp.trace
    if not inv.ok:
        return {"ok": False, "answer": inv.error or "inventory lookup failed", "trace": trace}
    if not atp.ok:
        return {"ok": False, "answer": atp.error or "ATP lookup failed", "trace": trace}
    inv_data = inv.data
    atp_data = atp.data
    out["answer"] = (
        f"{inv_data['item']}: on hand {_format_number(inv_data['on_hand'])}, "
        f"ATP {_format_number(atp_data['atp_qty'])} (as of {atp_data['as_of']})."
    )
    out["trace"] = trace
    return out
