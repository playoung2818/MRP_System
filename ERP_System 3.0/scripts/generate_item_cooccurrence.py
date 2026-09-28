"""Generate the editable companion file explicitly; never run from the web app."""

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sqlalchemy import text
from erp_system.runtime.db_config import get_engine
from erp_system.quotation_cards import COOCCURRENCE_FILE


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--overwrite", action="store_true", help="Replace the file, including manual edits")
    args = parser.parse_args()
    if COOCCURRENCE_FILE.exists() and not args.overwrite:
        parser.error("File exists. Preserve manual edits, or explicitly pass --overwrite.")
    builds = defaultdict(set)
    labels = {}
    engine = get_engine()
    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text("SET TRANSACTION READ ONLY"))
                rows = conn.execute(text("SELECT id, order_id, product_details FROM public.word_file_log")).mappings()
                for row in rows:
                    order = (row["order_id"] or "").strip().upper()
                    key = ("order", order) if order else ("file", row["id"])
                    details = row["product_details"]
                    if not isinstance(details, list):
                        continue
                    for detail in details:
                        if not isinstance(detail, dict):
                            continue
                        name = str(detail.get("product_number") or "").strip()
                        item_key = name.upper()
                        if not name or item_key == "END OF PART":
                            continue
                        labels[item_key] = min(name, labels.get(item_key, name))
                        builds[key].add(item_key)
    finally:
        engine.dispose()
    counts = defaultdict(Counter)
    for items in builds.values():
        for item in items:
            counts[item].update(items - {item})
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "description": "Edit items below manually. List order controls card order; only the first five distinct companions are shown. Frequency is distinct shared work orders, not units. The website never regenerates this file.",
        "items": {
            labels[item]: [
                {"item": labels[other], "frequency": frequency}
                for other, frequency in sorted(counts[item].items(), key=lambda pair: (-pair[1], labels[pair[0]]))[:5]
            ]
            for item in sorted(labels)
        },
    }
    COOCCURRENCE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with COOCCURRENCE_FILE.open("w" if args.overwrite else "x", encoding="utf-8") as handle:
        json.dump(output, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(f"Generated {len(output['items'])} item entries: {COOCCURRENCE_FILE}")


if __name__ == "__main__":
    main()
