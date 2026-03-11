# To Do

## Skill.md Module Candidates

### Best candidates

1. `erp-ledger-maintainer`
   Covers parser and reconciliation work already centered in `ERP_System 2.0/SKILL.md`, `ERP_System 2.0/core.py`, `ERP_System 2.0/ledger.py`, and the DB reconciliation tests in `ERP_System 2.0/tests`.
   This should stay as the maintenance and debugging skill for POD, shipping, normalization, and ledger mismatches.

2. `assignment-readiness`
   Based on `ERP_System 2.0/assignment_readiness.py`.
   This is a separate business workflow: strict vs loose assignment mode, cutoff date `2099-07-04`, blockers, reference tables, diff tables, and manual carry-forward fields.

3. `atp-analysis`
   Based on `ERP_System 2.0/atp.py` and the ATP parts of `ERP_System 2.0/ledger.py`.
   Good for workflows that answer whether an item or SO can be promised on a given date without future negative NAV.

4. `etl-runbook`
   Based on `ERP_System 2.0/etl.py`.
   This should describe the full pipeline: extract inputs, transform source files, build structured ERP view, expand shipping preinstalls, build ledger, build ATP, build assignment tables, write DB, and push Google Sheets and exports.

5. `data-io-integrations`
   Based on `ERP_System 2.0/io_ops.py`, `ERP_System 2.0/config.py`, and `ERP_System 2.0/db_config.py`.
   This should cover file locations, OneDrive inputs, Supabase or Postgres or DuckDB access, Google Sheets writes, and credential resolution.

6. `llm-erp-query`
   Based on `ERP_System 2.0/llm_backend.py` and `ERP_System 2.0/llm_cli.py`.
   This is a standalone skill for natural-language ERP questions, cache loading, tool selection, ATP date lookup, and SO waiting-item lookup.

7. `mentor-practice`
   Based on `ERP_System 2.0/practice/README.md`, `ERP_System 2.0/practice/student_tasks.py`, and `ERP_System 2.0/practice/mentor_cli.py`.
   This is optional, but it can be its own training skill for onboarding new contributors to the ERP logic.

### Recommended split

- Keep the current `SKILL.md` focused on parser and ledger reconciliation only.
- Move ATP and assignment-readiness into separate skills.
- Put config, table names, and test commands into `references/` files instead of expanding the main skill body.
