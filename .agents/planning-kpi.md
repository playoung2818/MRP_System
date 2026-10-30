# SO Planning KPI

## Scope and definitions

- User requested only average, median, P90 calendar days to first assignment,
  awaiting-first-assignment count/ages, and six detail columns: SO, Customer,
  Created date, First assigned at, Days to assign / waiting, and (latest user
  revision replacing Stage) Blocking items · uncovered / required.
- Blockers come from the workflow's exact linked `material_review_id`, not a fresh
  assessment or an arbitrary latest review. Canonical item display uses the shared
  `canonical_item` filter; saved quantities, POD#, reasons/warnings remain intact.
  A missing linked review is distinguished from a saved review with no blockers.
- Endpoint: `/planning/kpi`. Access through the Planning sidebar Reports section.
  The user explicitly requested NO KPI link in the homepage sidebar. The KPI
  page itself uses the Planning sidebar (two planning stages plus the active KPI
  report), not the homepage's quotation/item/purchase-order workspace navigation.
- Uses earliest `lt_planning_history.assigned_at`, never selected production start,
  latest schedule overrides, review time, or import timestamps. UTC calendar-day
  boundaries are explicit. Same-day assignment is zero days; weekends count.
- Waiting records are excluded from assignment statistics. P90 uses linear
  interpolation. Missing/invalid dates remain Unknown; no fabricated zeros.
- Active source-present review/planning SOs without a first assignment are waiting.
  Material/content approval (approved_snapshot) does NOT prove date assignment.
  Previously MES-released/returned orders with no history have unknown original
  assignment, not a newly restarted waiting clock. Missing-source/closed records
  without history are not active first-assignment waiters. Canonical SO aliases
  count once; prefer the canonical workflow row.

## Implementation and retention

- Pure calculations: `mrp_system/planning_kpi.py`.
- Read-only blueprint: `Webpage/planning_kpi.py`; three SELECT queries in a
  repeatable-read snapshot of existing workflow, planning history and open SOs.
- Dashboard template/static CSS: `planning_kpi.html`, `planning-kpi.css?v=2`.
  Flex layout fills available desktop height without guessed header allowances.
- Source original Order Date / final CLI SO Entry Date is retained once in
  `metadata_fields.planning_kpi.created_date`, with source provenance. CLI staging
  preserves existing metadata and does not add dates to amendment signatures or
  snapshots. Normal staging retains dates after source membership disappears.
- Actual new assignment also retains an available original date atomically with
  publication/history, covering SOs imported before this feature. The metadata
  merge updates only the KPI namespace, preserving notes/content metadata.
- Existing historical closed SO creation dates may be unavailable. Current open
  import provides a read-only fallback; do not backfill using inferred dates.
- No new tables, schema migrations, historical rewrites, or implicit GET writes.

## Validation

- 257 tests passed in the broader MRP suite, with four unrelated legacy cases
  deselected and the missing rename-sales-order-workflow module test ignored.
  Existing shipping concat warnings remain unrelated.
- Actual headless Edge verified the four cards, exactly six columns, waiting-row
  count, and a viewport-fitting page at 1440x1000. Operational screenshots remain
  outside Git in the user's temporary directory.
- Homepage navigation follow-up: 20 UI/KPI tests passed after removal of the
  homepage KPI link. Planning report navigation and direct route remain intact.
- KPI sidebar follow-up: 21 UI/KPI tests passed; live GET confirmed that only
  Planning stages and the KPI report appear in the sidebar.
- No live POST, ETL run, metadata seeding, assignment, or database table creation
  was executed during implementation/verification. Future normal user saves and
  CLI imports perform the new creation-date retention.
