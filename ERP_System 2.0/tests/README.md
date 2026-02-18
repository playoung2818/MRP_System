# Ledger Test Guide

## Prerequisites
- Python environment with project dependencies installed.
- Run commands from repo root: `C:\Users\Admin\Desktop\ERP_System`.
- For DB reconciliation tests, `DATABASE_DSN` must be set and reachable.

## Test Files

### `test_ledger_e2e_snapshot.py`
- Type: local fixture-based end-to-end snapshot test (no DB required).
- Uses fixtures in `tests/fixtures/`:
  - `items.csv`, `bom_2level.csv`, `inventory.csv`, `open_so.csv`, `open_po.csv`, `shipping_schedule.csv`
  - Expected snapshots:
    - `expected_planned_orders_snapshot.csv`
    - `expected_shortages_snapshot.csv`
- Validates:
  - `expand_nav_preinstalled -> build_events -> build_ledger_from_events`
  - planned orders snapshot
  - shortage snapshot

Run:
```powershell
python -m pytest -q ".\ERP_System 2.0\tests\test_ledger_e2e_snapshot.py"
```

### `test_pod_vs_ledger_in.py`
- Type: DB reconciliation test for inbound flow.
- Compares:
  - `NT Shipping Schedule` (parsed/expanded) vs `ledger_analytics` (`Kind='IN'`, `Source='NAV'`)
  - `Open_Purchase_Orders` vs `ledger_analytics` (`Kind='IN'`, `Source='POD'`)
  - item-level supply overcount checks
- Writes logs to:
  - `public.qa_inbound_recon_runs`
  - `public.qa_inbound_recon_details`

Run:
```powershell
python -m pytest -s -q ".\ERP_System 2.0\tests\test_pod_vs_ledger_in.py"
```

### `test_ledger_out_reconciliation.py`
- Type: DB reconciliation test for outbound flow.
- Compares:
  - `wo_structured` demand rows (`Qty(-)`) vs `ledger_analytics` (`Kind='OUT'`, `Source='SO'`)
  - signs (`OUT` delta must be negative)
  - item/date and QB/item/date quantity matching
- Writes logs to:
  - `public.qa_ledger_out_recon_runs`
  - `public.qa_ledger_out_recon_details`

Run:
```powershell
python -m pytest -s -q ".\ERP_System 2.0\tests\test_ledger_out_reconciliation.py"
```

## Run All Current Tests
```powershell
python -m pytest -q ".\ERP_System 2.0\tests"
```

Verbose with print output:
```powershell
python -m pytest -s ".\ERP_System 2.0\tests"
```
