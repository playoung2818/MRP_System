# ERP_System 3.0

Package-first ERP/MRP backend.

Run commands:

```powershell
python -m erp_system.cli.etl
python -m erp_system.cli.llm_cli
```

Negative projected qty details can be reviewed directly in the database:

```sql
SELECT "Date", "Item", "Item_raw", "Projected_NAV", "Name", "QB Num"
FROM public.ledger_analytics
WHERE "Projected_NAV" < 0
ORDER BY "Date", "Item", "QB Num";
```

The ETL command prints a compact negative projected-qty overview and the diff versus the previous run.
It also writes the full current negative projected-qty report to Excel:

```text
reports/negative_projected_qty.xlsx
```

The previous-run snapshot used for the diff is stored locally at `reports/.last_violation_report.csv`.

Notes:
- no flat top-level script shims
- no allocation module
- webpage stays separate and should import this package
