# Agent notes index

Keep agent implementation memory and work records in this directory, not in the
human-facing root README. Repository-wide behavior rules remain in `AGENTS.md`.

- [System implementation notes](system-notes.md): material/production planning,
  MRP/MES ownership, source presence, history/schema, normalization, PDF item
  ordering, POD site snapshots, and shipping expansion notes moved from README.
- [Change notes](change-notes.md): recent decisions, changes, validation, and known
  limitations. Add future work records here or create a focused note and index it.

- [SO Planning KPI](planning-kpi.md): calendar-day definitions, legacy coverage,
  creation-date retention, read-only dashboard, navigation, and validation.

- [Purchase orders and receiving](purchase-receiving.md): POD search/detail,
  append-only receiving, workbook/manual validation, locking, and tests.

Read the relevant notes before making related changes, then verify current code.
Historical schema commands are not authorization to create database tables.
Never put credentials, secret configuration values, or private business exports
in this directory.
