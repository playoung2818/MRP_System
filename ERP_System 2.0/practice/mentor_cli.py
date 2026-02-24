from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from practice.challenges import CHALLENGES, Challenge


ROOT = Path(__file__).resolve().parents[1]
PRACTICE_DIR = ROOT / "practice"
PROGRESS_FILE = PRACTICE_DIR / ".mentor_progress.json"


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"highest_passed_level": 0}


def _save_progress(progress: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding="utf-8")


def _challenge_by_level(level: int) -> Challenge:
    for c in CHALLENGES:
        if c.level == level:
            return c
    raise ValueError(f"Unknown level: {level}")


def cmd_next() -> int:
    p = _load_progress()
    level = min(p.get("highest_passed_level", 0) + 1, len(CHALLENGES))
    c = _challenge_by_level(level)
    print(f"Level {c.level}: {c.title}")
    print(f"Goal: {c.goal}")
    print(f"Target: {c.target}")
    print(f"Why: {c.why_it_matters}")
    print("")
    print("When done, run:")
    print(f"python \"ERP_System 2.0/practice/mentor_cli.py\" check --level {c.level}")
    return 0


def _run_test_node(test_node: str) -> tuple[int, str]:
    cmd = [sys.executable, "-m", "pytest", "-q", test_node]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout


def cmd_check(level: int) -> int:
    c = _challenge_by_level(level)
    code, out = _run_test_node(c.test_node)

    print(f"[Level {level}] {c.title}")
    if code == 0:
        progress = _load_progress()
        progress["highest_passed_level"] = max(progress.get("highest_passed_level", 0), level)
        _save_progress(progress)
        print("Result: PASS")
        if level < len(CHALLENGES):
            nxt = _challenge_by_level(level + 1)
            print(f"Unlocked: Level {nxt.level} - {nxt.title}")
        else:
            print("Track complete: You finished all mentor levels.")
    else:
        print("Result: FAIL")
        print("Feedback:")
        print("- Re-check function contract in practice/student_tasks.py")
        print("- Reproduce locally with:")
        print(f"  {sys.executable} -m pytest -q {c.test_node}")
        print("")
    print("---- pytest output ----")
    print(out.strip())
    return code


def cmd_status() -> int:
    p = _load_progress()
    highest = int(p.get("highest_passed_level", 0))
    print(f"Highest passed level: {highest}/{len(CHALLENGES)}")
    for c in CHALLENGES:
        flag = "PASS" if c.level <= highest else "LOCKED" if c.level > highest + 1 else "OPEN"
        print(f"Level {c.level}: {flag} - {c.title}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Mentor practice CLI for ERP_System 2.0")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("next", help="Show the next challenge")
    sub.add_parser("status", help="Show progress")

    p_check = sub.add_parser("check", help="Run tests for a challenge level")
    p_check.add_argument("--level", type=int, required=True)

    args = parser.parse_args()

    if args.cmd == "next":
        return cmd_next()
    if args.cmd == "status":
        return cmd_status()
    if args.cmd == "check":
        return cmd_check(args.level)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
