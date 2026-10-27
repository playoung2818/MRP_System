# Change notes

Record agent decisions, implementation context, validation, and unresolved work
here (or in focused notes linked from INDEX.md), not in the human-facing README.
Do not treat test results below as a current production audit.

## 2026-10-22 — Documentation policy

- User requested that accumulated implementation notes be removed from README.
- Moved the sections from `Manual LT planning workspace` through `Remark`,
  including normalization, PDF-only ordering, and POD site snapshots, into
  `system-notes.md`. Existing descriptions were retained, not freshly audited.
- Root `AGENTS.md` now directs agent memory/work records to `.agents/` and keeps
  README reserved for human-facing usage/setup documentation.
- Known historical wording in the moved snapshot: the sidebar description and
  manual blocker-reason input description are stale. Current Planning sidebar
  does not link directly to stages; manual blocking-reason inputs were removed.
  Verify code before relying on other UI descriptions or schema commands.

## 2026-10-22 — Local service endpoints

- Use `192.168.60.215`, not loopback names/addresses, for local service URLs and
  connectivity checks; preserve the intended ports and paths.
- MRP MES quantity API default:
  `http://192.168.60.215:5001/api/wo-quantities`.
  `MES_WO_QUANTITIES_URL` still supports an explicit endpoint override.
- Local MRP database host was aligned to the same IP, preserving port 5432 and
  existing credentials. No secret configuration values are recorded here.
- MRP web port is 5002. Preserve `0.0.0.0` as the server bind address so the LAN
  address remains reachable; it is not a client URL.
- MES must be running before ETL. Do not assume zero WO/WIP quantities when its
  API cannot be reached. Earlier connection checks found port 5001 unavailable;
  that was a point-in-time observation, not a permanent service status.

## 2026-10-22 — Shared CLI report presentation

- `mrp_system/runtime/cli_reports.py::format_cli_table(report, columns=None)`
  provides the shared left-aligned ASCII-bordered DataFrame formatter.
- Preserves row order and values; no DataFrame index or row truncation. Optional
  columns select/order displayed fields. Missing values are blank, and embedded
  tabs/newlines are sanitized. No extra formatting dependency was introduced.
- Used for partial-shipment SO/customer lists, violation overview/change counts,
  new/resolved/changed violation details, and loaded-database-table row counts.
- Removed the dedicated `format_partial_shipment_report` function. Reconciliation
  still happens at item/site level internally; display is unique QB Num/Customer
  rows, not all partially shipped SOs regardless of reconciliation.
- `runtime/console_logging.py` supplies aligned timestamp/severity/message output:
  bold cyan INFO, yellow WARNING, red ERROR, secondary timestamps. Supports
  Windows ANSI console enabling; redirected output stays plain. `NO_COLOR`
  disables color. Tracebacks are retained.

## 2026-10-22 — Violation output and pandas warning cleanup

- Overview is now a Metric/Value table; last-run comparison is a
  Current/Previous/New/Resolved/Changed table using the shared formatter.
- Sort both comparison MultiIndexes before tuple lookups. Duplicate-key handling
  retains the existing first-row comparison rule. Baseline snapshots and
  new/resolved/changed detection behavior remain unchanged.
- `ledger/events.py` excludes empty frames and all-null columns from concat dtype
  inference, then restores the full event schema. Movement rows are not removed.
  Metadata columns remain object-typed to prevent numeric PO identifiers becoming
  strings such as `123.0` and breaking POD-site exclusion matching.
- Warnings were addressed at their causes, not globally suppressed.
- Validation: 63 targeted tests passed; one preexisting mixed-site Drop Ship test
  was deselected because it expects obsolete whole-SO exclusion. Desired policy
  excludes only Drop Ship lines and retains warehouse lines on mixed SOs.
- Tests cover warnings-as-errors for concat and unsorted duplicate-key lookups,
  numeric PO exclusion, report formatting, snapshot behavior, ledger ordering,
  console contrast, and shipment reconciliation. No full ETL was executed and no
  live database writes or schema changes were performed for this cleanup.

## 2026-10-26 — Immediate production assignment after no-blocker review

- On an initial material-review form, checking the existing no-material-blockers
  checkbox reveals a required production date and changes the submit button to
  `Save Review & Assign Production Date`. Unchecking hides/disables that field
  and restores the normal review action. No extra confirmation/operator input.
- One review POST can save SO content, append material review, assign production,
  append planning history linked to that exact new review, and publish to MES in
  the same locked transaction. Both history rows use the submission UUID in their
  separate tables, allowing retry protection through the existing review ID.
- Combined submission requires explicit no-blockers approval and an empty blocker
  list. Date validation is unchanged: today/future weekday, no later than a known
  imported ship date. Validate before saving review rows. Failures roll back the
  transaction; publication failure never reaches schedule/history assignment.
- The material-review handoff timestamp is populated for this immediate release.
  Standard blocked reviews still proceed to Needs Production Planning. A legacy
  review-only API request without a date remains supported and does not release.
- Returned-from-MES reviews do not show a date field and reject an attempted
  combined reschedule; revision approval retains existing schedules/FG/WOs.
- Current workflow code uses `public.sales_order_workflow`; older notes mentioning
  `approved_work_orders` are historical names, not a reason to recreate tables.
- Updated JS cache version to v5. Validation: 84 targeted tests passed, including
  checkbox/payload behavior via Node, date/owner/stale-review checks, duplicate
  submission and rollback-context tests. Live read-only review GET returned 200
  and contained the new controls/script version. No live POST/database writes or
  schema changes were performed. README and unrelated runtime path edits retained.
