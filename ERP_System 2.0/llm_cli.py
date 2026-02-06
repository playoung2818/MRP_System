from __future__ import annotations

import sys

from llm_backend import DataCache, answer_question


HELP = """Commands:
  help         Show this help
  reload       Reload DB tables
  exit         Quit

Examples:
  How many i9-14900 right now, and how many i9-14900 ATP?
  ATP date for i9-14900 qty 5
"""


def main() -> int:
    cache = DataCache()
    cache.ensure_loaded()
    print("LLM CLI ready. Type 'help' for commands.")
    while True:
        try:
            text = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not text:
            continue
        cmd = text.lower()
        if cmd in {"exit", "quit"}:
            return 0
        if cmd == "help":
            print(HELP)
            continue
        if cmd == "reload":
            cache.reload()
            print("Reloaded.")
            continue

        result = answer_question(cache, text)
        if result.get("ok"):
            print(result.get("answer", "OK"))
            trace = result.get("trace") or []
            if trace:
                print("Trace:")
                for t in trace:
                    print(f"  - {t}")
        else:
            print(result.get("answer", "Error"))
            trace = result.get("trace") or []
            if trace:
                print("Trace:")
                for t in trace:
                    print(f"  - {t}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
