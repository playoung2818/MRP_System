from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Challenge:
    level: int
    title: str
    goal: str
    target: str
    why_it_matters: str
    test_node: str


CHALLENGES: list[Challenge] = [
    Challenge(
        level=1,
        title="Normalize WO Numbers",
        goal="Implement SO/WO normalization so multiple text formats map to one canonical key.",
        target="practice/student_tasks.py -> level1_normalize_wo",
        why_it_matters="This key normalization is the base join key across SO, word pick logs, and reports.",
        test_node="practice/tests/test_mentor_levels.py::test_level1_normalize_wo",
    ),
    Challenge(
        level=2,
        title="Build Pick Flags",
        goal="Aggregate word-file rows into a per-SO Picked flag exactly like production logic.",
        target="practice/student_tasks.py -> level2_build_pick_flags",
        why_it_matters="Picked status drives WIP, On Hand - WIP, and assignment decisions.",
        test_node="practice/tests/test_mentor_levels.py::test_level2_build_pick_flags",
    ),
    Challenge(
        level=3,
        title="Shipping Qty Source",
        goal="Use Confirmed Qty as inbound qty source, with Qty fallback for backward compatibility.",
        target="practice/student_tasks.py -> level3_select_shipping_qty",
        why_it_matters="Shipping qty source directly affects ledger IN events and ATP.",
        test_node="practice/tests/test_mentor_levels.py::test_level3_select_shipping_qty",
    ),
    Challenge(
        level=4,
        title="Open PO vs Ledger Reconciliation",
        goal="Build a mismatch summary table by normalized item key with real business filters.",
        target="practice/student_tasks.py -> level4_reconcile_open_po_vs_ledger",
        why_it_matters="This is a core QA control used to catch PO/ledger drift.",
        test_node="practice/tests/test_mentor_levels.py::test_level4_reconcile_open_po_vs_ledger",
    ),
    Challenge(
        level=5,
        title="ETL Design Basics (DAG + Validation)",
        goal="Produce a valid ETL order from dependency graph and fail fast on cycles.",
        target="practice/student_tasks.py -> level5_topological_order",
        why_it_matters="System design starts with explicit dependencies and safety checks.",
        test_node="practice/tests/test_mentor_levels.py::test_level5_topological_order",
    ),
]

