"""Read manually maintained companion selections; never calculate associations."""

import json
from pathlib import Path

COOCCURRENCE_FILE = Path(__file__).resolve().parents[1] / "data" / "item_cooccurrence.json"


def load_item_cards(item, inventory_rows, path=COOCCURRENCE_FILE):
    if not item or not item.strip():
        return [], None
    selected = item.strip()
    companions = []
    error = None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        mapping = data["items"]
        if not isinstance(mapping, dict):
            raise ValueError("items must be an object")
        entry = next((v for k, v in mapping.items() if k.strip().casefold() == selected.casefold()), [])
        if not isinstance(entry, list):
            raise ValueError("companions must be a list")
        seen = {selected.casefold()}
        for value in entry:
            name = value if isinstance(value, str) else value.get("item") if isinstance(value, dict) else None
            if not isinstance(name, str) or not name.strip():
                raise ValueError("each companion needs an item name")
            name = name.strip()
            if name.casefold() not in seen:
                companions.append(name)
                seen.add(name.casefold())
            if len(companions) == 5:
                break
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        companions = []
        error = "Companion items could not be loaded. Check data/item_cooccurrence.json."

    inventory = {str(row.get("item", "")).strip().casefold(): row for row in inventory_rows}
    cards = []
    for index, name in enumerate([selected, *companions]):
        row = inventory.get(name.casefold(), {})
        cards.append({
            "item": row.get("item") or name,
            "selected": index == 0,
            "on_hand": row.get("on_hand"),
            "available": row.get("available"),
        })
    return cards, error
