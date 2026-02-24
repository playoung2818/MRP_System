# Mentor Practice Module

This module is a guided coding track to help you learn this ERP system by building/debugging realistic pieces of it.

## Learning goals

- Become fluent with this project's data model and ETL flow.
- Practice debugging with small, focused tasks.
- Build system design basics (dependencies, fail-fast checks).

## Difficulty path

1. WO key normalization
2. Picked status aggregation
3. Shipping qty source selection (`Confirmed Qty` first)
4. Open PO vs ledger reconciliation
5. ETL dependency graph + cycle detection

## Files

- `ERP_System 2.0/practice/student_tasks.py`:
  your coding workspace (implement each level here).
- `ERP_System 2.0/practice/tests/test_mentor_levels.py`:
  auto-checks + feedback.
- `ERP_System 2.0/practice/mentor_cli.py`:
  command runner (next task, check, progress).

## Usage

From project root:

```powershell
python "ERP_System 2.0/practice/mentor_cli.py" status
python "ERP_System 2.0/practice/mentor_cli.py" next
python "ERP_System 2.0/practice/mentor_cli.py" check --level 1
```

You can also run all mentor tests:

```powershell
python -m pytest -q "ERP_System 2.0/practice/tests/test_mentor_levels.py"
```

## Mentor workflow suggestion

1. Run `next`.
2. Implement only that level in `student_tasks.py`.
3. Run `check --level N`.
4. If fail, fix and re-run.
5. Move to next level after pass.

