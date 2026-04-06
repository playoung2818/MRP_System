from __future__ import annotations

import pandas as pd

from erp_system.normalize.erp_normalize import normalize_item
from erp_system.runtime.policies import EXCLUDED_POD_SOURCE_NAMES


def transform_pod(df_pod: pd.DataFrame) -> pd.DataFrame:
    pod = df_pod.copy()
    pod = pod.drop(columns=["Open Balance"], errors="ignore")
    first_col = pod.columns[0] if len(pod.columns) > 0 else None
    if "Num" in pod.columns and "POD#" not in pod.columns:
        pod["POD#"] = pod["Num"]
    pod.rename(columns={"Date": "Order Date", "Num": "QB Num", "Backordered": "Qty(+)"}, inplace=True)
    pod = pod.dropna(axis=0, how="all", subset=None, inplace=False)
    pod["QB Num"] = pod["QB Num"].astype(str).str.split("(", expand=True)[0].str.strip()

    if first_col is not None and first_col in pod.columns:
        labels = pod[first_col].astype(str).str.replace("\u00A0", " ", regex=False).str.strip()
        labels = labels.mask(labels.str.lower().isin(["nan", "none", ""]))
        is_total = labels.str.match(r"(?i)^total\b", na=False)
        is_header = labels.notna() & ~is_total

        section_item: list[str | None] = []
        current_item: str | None = None
        for lbl, total, header in zip(labels.tolist(), is_total.tolist(), is_header.tolist()):
            if total:
                current_item = None
            elif header:
                current_item = str(lbl).strip()
            section_item.append(current_item)

        pod["Item"] = pd.Series(section_item, index=pod.index, dtype="string")
    else:
        pod["Item"] = pd.NA

    pod = pod.dropna(thresh=5)

    if "Memo" in pod.columns:
        memo = pod["Memo"].astype(str).str.strip()
        memo_item = memo.str.split(" ", expand=True)[0]
        memo_item = pd.Series(memo_item, index=pod.index, dtype="string").str.replace("*", "", regex=False).str.strip()
        pod["Item"] = pod["Item"].fillna(memo_item)

    pod = pod.loc[pod["QB Num"].notna() & pod["QB Num"].ne("")].copy()
    pod["Order Date"] = pd.to_datetime(pod["Order Date"])
    if "Deliv Date" in pod.columns:
        pod["Deliv Date"] = pd.to_datetime(pod["Deliv Date"], errors="coerce")
    if "Ship Date" not in pod.columns:
        pod["Ship Date"] = pd.NaT
    if "Source Name" in pod.columns and "Deliv Date" in pod.columns:
        mask = ~pod["Source Name"].astype(str).isin(EXCLUDED_POD_SOURCE_NAMES)
        pod.loc[mask, "Ship Date"] = pod.loc[mask, "Deliv Date"]
    pod["Item"] = pod["Item"].astype(str).str.strip().map(normalize_item)
    for c in ["Qty(+)", "Qty", "Rcv'd", "Amount"]:
        if c in pod.columns:
            pod[c] = pd.to_numeric(pod[c], errors="coerce")
    return pd.DataFrame(pod)


def enrich_pod_with_shipping_audit(df_pod: pd.DataFrame, df_shipping: pd.DataFrame) -> pd.DataFrame:
    pod = df_pod.copy()
    if pod.empty:
        for col in [
            "POD Ship Date Raw",
            "Shipping Schedule Ship Date",
            "Shipping Schedule Qty(+)",
            "Shipping Schedule Order Qty",
            "Shipping Schedule Item",
            "Shipping Schedule Reference",
        ]:
            if col not in pod.columns:
                pod[col] = pd.NA
        return pod

    pod["POD#"] = pod.get("POD#", pod.get("QB Num", pd.Series("", index=pod.index))).fillna("").astype(str).str.strip()
    pod["Ship Date"] = pd.to_datetime(pod.get("Ship Date", pd.NaT), errors="coerce")
    pod["POD Ship Date Raw"] = pod["Ship Date"].dt.strftime("%Y-%m-%d").fillna("")

    if df_shipping is None or df_shipping.empty:
        for col in [
            "Shipping Schedule Ship Date",
            "Shipping Schedule Qty(+)",
            "Shipping Schedule Order Qty",
            "Shipping Schedule Item",
            "Shipping Schedule Reference",
        ]:
            if col not in pod.columns:
                pod[col] = pd.NA
        pod["Shipping Schedule Ship Date"] = ""
        pod["Shipping Schedule Qty(+)"] = 0.0
        pod["Shipping Schedule Order Qty"] = 0.0
        pod["Shipping Schedule Item"] = ""
        pod["Shipping Schedule Reference"] = ""
        return pod

    ship = df_shipping.copy()
    for col in ["QB Num", "Ship Date", "Qty(+)", "Order Qty", "Item", "Reference"]:
        if col not in ship.columns:
            ship[col] = pd.NA

    ship["QB Num"] = ship["QB Num"].fillna("").astype(str).str.strip()
    ship["Ship Date"] = pd.to_datetime(ship["Ship Date"], errors="coerce")
    ship["Qty(+)"] = pd.to_numeric(ship["Qty(+)"], errors="coerce").fillna(0.0)
    ship["Order Qty"] = pd.to_numeric(ship["Order Qty"], errors="coerce").fillna(0.0)
    ship["Item"] = ship["Item"].fillna("").astype(str).str.strip()
    ship["Reference"] = ship["Reference"].fillna("").astype(str).str.strip()
    ship = ship.loc[ship["QB Num"].ne("")].copy()

    if ship.empty:
        pod["Shipping Schedule Ship Date"] = ""
        pod["Shipping Schedule Qty(+)"] = 0.0
        pod["Shipping Schedule Order Qty"] = 0.0
        pod["Shipping Schedule Item"] = ""
        pod["Shipping Schedule Reference"] = ""
        return pod

    def _join_unique(series: pd.Series) -> str:
        vals = [str(v).strip() for v in series if pd.notna(v) and str(v).strip()]
        return ", ".join(dict.fromkeys(vals))

    ship_agg = (
        ship.groupby("QB Num", as_index=False)
        .agg(
            shipping_ship_date=("Ship Date", "min"),
            shipping_qty=("Qty(+)", "sum"),
            shipping_order_qty=("Order Qty", "sum"),
            shipping_item=("Item", _join_unique),
            shipping_reference=("Reference", _join_unique),
        )
        .rename(columns={"QB Num": "POD#"})
    )
    ship_agg["Shipping Schedule Ship Date"] = ship_agg["shipping_ship_date"].dt.strftime("%Y-%m-%d").fillna("")
    ship_agg.rename(
        columns={
            "shipping_qty": "Shipping Schedule Qty(+)",
            "shipping_order_qty": "Shipping Schedule Order Qty",
            "shipping_item": "Shipping Schedule Item",
            "shipping_reference": "Shipping Schedule Reference",
        },
        inplace=True,
    )

    existing_audit = (
        pod[
            [
                c
                for c in [
                    "POD#",
                    "Shipping Schedule Ship Date",
                    "Shipping Schedule Qty(+)",
                    "Shipping Schedule Order Qty",
                    "Shipping Schedule Item",
                    "Shipping Schedule Reference",
                ]
                if c in pod.columns
            ]
        ].copy()
        if any(
            c in pod.columns
            for c in [
                "Shipping Schedule Ship Date",
                "Shipping Schedule Qty(+)",
                "Shipping Schedule Order Qty",
                "Shipping Schedule Item",
                "Shipping Schedule Reference",
            ]
        )
        else pd.DataFrame(columns=["POD#"])
    )
    pod = pod.drop(
        columns=[
            c
            for c in [
                "Shipping Schedule Ship Date",
                "Shipping Schedule Qty(+)",
                "Shipping Schedule Order Qty",
                "Shipping Schedule Item",
                "Shipping Schedule Reference",
            ]
            if c in pod.columns
        ],
        errors="ignore",
    )

    pod = pod.merge(
        ship_agg[
            [
                "POD#",
                "shipping_ship_date",
                "Shipping Schedule Ship Date",
                "Shipping Schedule Qty(+)",
                "Shipping Schedule Order Qty",
                "Shipping Schedule Item",
                "Shipping Schedule Reference",
            ]
        ],
        on="POD#",
        how="left",
    )
    if not existing_audit.empty and "POD#" in existing_audit.columns:
        existing_audit = existing_audit.drop_duplicates(subset=["POD#"], keep="last")
        pod = pod.merge(existing_audit, on="POD#", how="left", suffixes=("", "_old"))
        for col in [
            "Shipping Schedule Ship Date",
            "Shipping Schedule Qty(+)",
            "Shipping Schedule Order Qty",
            "Shipping Schedule Item",
            "Shipping Schedule Reference",
        ]:
            old_col = f"{col}_old"
            if old_col not in pod.columns:
                continue
            if col in {"Shipping Schedule Qty(+)", "Shipping Schedule Order Qty"}:
                pod[col] = pd.to_numeric(pod[col], errors="coerce").fillna(pd.to_numeric(pod[old_col], errors="coerce"))
            else:
                pod[col] = pod[col].fillna(pod[old_col])
            pod.drop(columns=[old_col], inplace=True)
    pod["Ship Date"] = pod["shipping_ship_date"].combine_first(pod["Ship Date"])
    pod["shipping_ship_date"] = pd.to_datetime(pod["shipping_ship_date"], errors="coerce")
    pod["Shipping Schedule Ship Date"] = pod["Shipping Schedule Ship Date"].fillna("")
    pod["Shipping Schedule Qty(+)"] = pd.to_numeric(pod["Shipping Schedule Qty(+)"], errors="coerce").fillna(0.0)
    pod["Shipping Schedule Order Qty"] = pd.to_numeric(pod["Shipping Schedule Order Qty"], errors="coerce").fillna(0.0)
    pod["Shipping Schedule Item"] = pod["Shipping Schedule Item"].fillna("")
    pod["Shipping Schedule Reference"] = pod["Shipping Schedule Reference"].fillna("")
    pod.drop(columns=["shipping_ship_date"], inplace=True)
    return pod


__all__ = ["enrich_pod_with_shipping_audit", "transform_pod"]
