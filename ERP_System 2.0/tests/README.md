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
  - `NT Shipping Schedule` (parsed/expanded) vs `ledger_analytics` (`Kind='IN'`, `Source='NAV'`, SAP inbound stream)
  - `Open_Purchase_Orders` vs `ledger_analytics` (`Kind='IN'`, `Source='POD'`)
  - item-level supply overcount checks
- Writes logs to:
  - `public.qa_inbound_recon_runs`
  - `public.qa_inbound_recon_details`

Run:
```powershell
python -m pytest -s -q ".\ERP_System 2.0\tests\test_pod_vs_ledger_in.py"
```

### `test_shipping_reference_so_match.py`
- Type: DB + source-file reconciliation test for shipping `Reference` to SO mapping.
- Reads shipping source Excel (`config.SHIPPING_SCHEDULE_FILE`) to use `Reference` text, then:
  - runs `transform_shipping`
  - runs `expand_nav_preinstalled`
  - extracts SO numbers from `Reference` (e.g. `NTA_Applied Intuition_SO-20251788`)
  - checks `Pre` component rows can be matched to `wo_structured` by `QB Num` + item
- Writes logs to:
  - `public.qa_shipping_ref_so_runs`
  - `public.qa_shipping_ref_so_details`

Run:
```powershell
python -m pytest -s -q ".\ERP_System 2.0\tests\test_shipping_reference_so_match.py"
```

Current expected behavior:
- This test may fail if parsing artifacts exist in shipping descriptions (for example `_x000D_` token fragments), which indicates real mismatch risk between parsed components and `wo_structured`.

## Run All Current Tests
```powershell
python -m pytest -q ".\ERP_System 2.0\tests"
```

Verbose with print output:
```powershell
python -m pytest -s ".\ERP_System 2.0\tests"
```
