# Agent notes index

Keep agent implementation memory and work records in this directory, not in the
human-facing root README.

Project skill: [`frontend-design`](skills/frontend-design/SKILL.md), installed from `anthropics/skills` (`skills/frontend-design`) with its bundled Apache-2.0 license. Use Pi `/reload` after installation, then `/skill:frontend-design` to invoke explicitly. Studio remains the user-selected visual direction; adding this skill does not change the application. Repository-wide behavior rules remain in `AGENTS.md`.

- [System implementation notes](system-notes.md): material/production planning,
  MRP/MES ownership, source presence, history/schema, normalization, PDF item
  ordering, POD site snapshots, and shipping expansion notes moved from README.
- [Change notes](change-notes.md): recent decisions, changes, validation, and known
  limitations. Add future work records here or create a focused note and index it.

- [SO Planning KPI](planning-kpi.md): calendar-day definitions, legacy coverage,
  creation-date retention, read-only dashboard, navigation, and validation.

- [Purchase orders and receiving](purchase-receiving.md): POD search/detail,
  append-only receiving, workbook/manual validation, locking, and tests.

- [Item price history](pod-price-history.md): `public.item_cost` table (user-approved), loader from daily POD_*.CSV snapshots, coverage limits, initial load.

- [Quotation ledger presentation](quotation-ledger.md): orange minimum Projected_OnHand within confirmed dates, pending-date exclusion, and regression coverage.

- [Studio homepage demo](studio-home-demo.md): standalone Studio opening-screen preview, real module/search links, validation, and integration limitations; live homepage unchanged.

- [Studio KPI UI pilot](studio-kpi-ui.md): user-selected Hallmark Studio direction, page-scoped restyle, browser checks, and known test/lint limitations.

- [Safety-stock continuous improvement](safety-stock-improvement.md): Markdown-only roadmap and project memory, SO Planning KPI outcome measurement, proposed milestones, and open decisions. Do not connect to Notion for this project.

Read the relevant notes before making related changes, then verify current code.
Historical schema commands are not authorization to create database tables.
Never put credentials, secret configuration values, or private business exports
in this directory.
