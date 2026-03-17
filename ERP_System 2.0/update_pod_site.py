from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd

from config import POD_FILE
from erp_normalize import detect_pod_site, format_pod_site_entries


POD_SITE_BLOCK_RE = re.compile(
    r"POD_SITE: dict\[str, str\] = \{\n.*?\n\}",
    re.DOTALL,
)


def build_pod_site_block(site_map: dict[str, str]) -> str:
    lines = format_pod_site_entries(site_map)
    if lines:
        return f"POD_SITE: dict[str, str] = {{\n{lines}\n}}"
    return "POD_SITE: dict[str, str] = {\n}"


def update_erp_normalize(pod_file: str | Path, erp_normalize_path: str | Path) -> dict[str, str]:
    df_pod = pd.read_csv(str(pod_file), encoding="ISO-8859-1", engine="python")
    site_map = detect_pod_site(df_pod)

    target = Path(erp_normalize_path)
    text = target.read_text(encoding="utf-8")
    new_block = build_pod_site_block(site_map)
    updated_text, count = POD_SITE_BLOCK_RE.subn(new_block, text, count=1)
    if count != 1:
        raise RuntimeError("Could not find POD_SITE block in erp_normalize.py")
    target.write_text(updated_text, encoding="utf-8")
    return site_map


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Regenerate POD_SITE in erp_normalize.py from POD Inventory Site values."
    )
    parser.add_argument(
        "--pod-file",
        default=str(POD_FILE),
        help="Path to the POD CSV file. Defaults to config.POD_FILE.",
    )
    parser.add_argument(
        "--target",
        default=str(Path(__file__).with_name("erp_normalize.py")),
        help="Path to erp_normalize.py to update.",
    )
    args = parser.parse_args()

    site_map = update_erp_normalize(args.pod_file, args.target)
    print(f"Updated POD_SITE with {len(site_map)} entries.")
    if site_map:
        print(format_pod_site_entries(site_map))


if __name__ == "__main__":
    main()
