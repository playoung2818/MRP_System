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

## Planning sidebar search

- Moved the existing SO/customer GET search from the main results card into the
  Planning sidebar below the brand. Search submits natively on Enter; there are
  no Search/Clear buttons. Empty the field and press Enter to reset. Current query,
  current stage, and cross-stage current-MRP search behavior are preserved.
- Added compact sidebar form styling and vertical sidebar overflow; mobile
  sidebar layout is retained.
- Validation: 49 Planning/UI/JS tests passed, including search location and
  stage/query preservation. No backend/database changes.

## Shared Planning table layout

- Production Planning uses the same `.table-wrap` layout as Material Review.
  Both use a CSS height variable measured from actual available browser space,
  with a 60vh no-JS fallback and a 240px minimum. Scrolling and sticky headers
  remain. A prior hard-coded 300px header allowance was removed because it could
  not reliably align the card bottom across browser sizes and header reflows.
  Tables explicitly use full width without relying on CDN CSS.
  Removed the production-only 65vh minimum/75vh maximum override.
- Blocker details and inline date/notes assignment controls remain unchanged.
- CSS cache version is v10; JS cache version is v6. Validation: 51 Planning/UI/JS
  tests passed, including
  identical wrapper styling and retention of production assignment controls.
  No backend/database changes; README is unchanged.

## Live-browser verification and compact production rows

- Checked the actual 192.168.60.215:5002 Planning pages in headless Edge at
  1440x1000, with DOM measurements and screenshots (temporary files outside Git).
- Both stages already had 700px table viewports. The real density difference was
  the stacked assignment form: production rows were about 164px, versus 50px in
  material review; only four production rows were fully visible.
- Production assignment date, Notes, and Assign button now share a horizontal
  flex row. Notes retains its accessible label, saved multiline contents, and
  vertical resizing, but starts at one visible line. No extra submission controls
  or changes to date/material/ownership validation.
- Verified the updated live page again: same 700px viewport, production rows
  about 54–89px for the sampled rows, and eleven fully visible rows. Material
  Review remained unchanged at thirteen visible rows. Counts are measurements
  for the tested viewport/current dataset, not fixed UI guarantees.
- CSS cache version v9. All browser checks were GET/read-only; no assignment was
  submitted. Screenshots contain operational data and must not be committed.

## Viewport bottom alignment and invisible-label overflow

- `lt-planning.js` sizes the primary `.planning-orders-card .table-wrap` from its
  document-space top, actual footer height, main bottom padding, and card border/
  margin. Recalculate on window resize/load and ResizeObserver updates for the
  header, summaries, error area, and footer. Scroll position does not change the
  allocation. The card has no trailing margin, but a selected review keeps its
  normal gap beneath the primary card.
- Actual Edge inspection found a second root cause: absolutely positioned
  `.visually-hidden` labels in unpositioned assignment forms caused document
  overflow outside the scrollable table. At a 1000px viewport, Production Planning
  had a 2109px document despite the card itself being near the viewport bottom.
  `position:relative` on `.inline-plan-form` contains those labels correctly.
- Rechecked both live stages at 1440x1000, 1920x1080, and 1920x1440. Card bottoms
  were consistently about 21px above the viewport bottom (normal 20px padding
  plus rounding), and document height equaled viewport height in all six checks.
- Dedicated Node regression covers measured heights, resize, header/footer reflow,
  scroll independence, and the small-screen minimum. No live POST/DB writes.

## Education Discount is nonproduction

- Added exact, trimmed, case-insensitive `Education Discount` exclusion to the
  shared MRP production policy and MES execution policy, alongside charges/services.
  CLI import/staging, material requirements, order signatures and publication use
  this classification; similarly named real products are not excluded by prefix.
- Combined review's `warehouse_source` now filters all nonproduction lines as well
  as Drop Ship lines. This handles existing cached snapshots immediately without
  rewriting source snapshots, saved history, schedules, or WOs in the database.
- Keep the MRP policy import inside `warehouse_source`, not at module scope:
  `server.py` imports web review helpers before adding the pipeline directory to
  sys.path. Added a subprocess regression test for that startup order. A reload
  startup failure was corrected and the MRP web process restored.
- SO-20261613 live GET returned 200 with 14 production items and no Education
  Discount; ownership remained Needs Material Review. No stored approval
  signatures contained the discount at the time of the read-only check.
- Validation: 108 targeted MRP tests passed (one existing obsolete whole-SO Drop
  Ship test deselected), plus four pure MES scope tests. The MES tests perform no
  app startup, database/table creation, or live writes. No full CLI or live POST
  was executed. Raw imported rows are filtered on the next normal CLI run;
  original snapshots/history are not manually deleted.

## Sidebar footer removal

- Removed the three-line Planning sidebar instruction footer at the user's
  request. Navigation/search and the table's loaded-data/source footer remain.
- Validation: 53 Planning/UI/JS tests passed, including absence of the sidebar
  text and retention of loaded-data information. No workflow/database changes.

## MES API connection-refused recovery

- A CLI failure against 192.168.60.215:5001 was caused by MES being stopped;
  no Python MES web process was running. MRP on port 5002 remained available.
- Read-only schema preflight confirmed all four MES model tables and their columns
  already existed, the multiple-WO migration was applied, and the startup legacy
  seed had zero candidates. No table creation was needed or authorized.
- Started the existing MES run.py web process with its existing protected settings,
  ensuring the database connection host was 192.168.60.215. Startup log is outside
  the repository in the user's temporary directory; no credentials recorded here.
- Verified API HTTP 200/schema v2, then ran only MRP's real fetch_mes_quantities
  against the current imported SO identifiers: 158 identifiers, 283 quantity rows
  at that check. These counts are point-in-time verification, not fixed totals.
- Did not rerun ETL, replace imported/ledger tables, or bypass MES with zero WIP.

## WIP lookup diagnostic: intentionally excluded saved SO references

- The user clarified that a trailing apostrophe in a saved MES WO's `sales_order`
  intentionally excludes returned-to-warehouse stock while retaining the WO.
  Do not "repair" these references or normalize them back into active WIP.
- Exact raw-reference filtering happens before downstream normalization. A
  canonical request excludes the suffixed record. The disputed item total was
  correct; the apparent discrepancy came from comparing outstanding demand to
  actual MES WIP on a partially picked order, not from the returned WO.
- Operational IDs/quantities are omitted from this note. No WO records were
  changed during the investigation.

## MRP execution metrics aligned with MES

- `match_mes_quantities` keeps original Ordered Qty separate from source
  outstanding/inferred Shipped Qty and MES physical shipments. WIP is saved
  unshipped release quantity, without a source-outstanding cap. Awaiting pick is
  original ordered minus saved unshipped and physically shipped quantities.
- Over-execution is flagged for review, not silently truncated or auto-corrected.
  Source shipment reconciliation remains compatible; the existing source
  `Shipped Qty` field is not relabelled or treated as physical MES shipment.
- Shared `execution_metrics` yields one SO/item/site summary. Structured output
  retains both per-SO `SO WIP Qty` and item-wide `WIP_Qty`; legacy `Picked_Qty`
  now denotes per-SO active WIP. On Hand minus WIP uses the item-wide total and
  does not silently clamp negative stock. New numeric columns have output types.
- MES-derived inventory WIP replaces stale input WIP, including zero/empty
  lookup results. Both DB-read and downstream adapters preserve exact raw SO
  exclusions before normalization, including intentional apostrophes.
- Item-detail, SO drilldown, and inventory lookup tables display Ordered,
  Picked (WIP), Awaiting Pick, Physically Shipped, and Source Outstanding
  separately. They read existing MES WO facts on lookup (no HTTP dependency,
  writes, schema changes, or new tables); repeated source lines are grouped once.
  Full source site membership is consulted before inferring legacy missing WO
  sites, rather than guessing from the WH01S-only structured view.
  Full structured metrics will also persist on the next ordinary CLI run.
- Actual headless Edge verification confirmed the displayed partial-pick split,
  zero WIP for the intentionally excluded WO, and correct summed item WIP. The
  execution table uses horizontal scrolling to avoid deeply wrapped item names.
  Screenshots remain in the user's temporary directory, outside Git.
- Validation: 230 tests passed; four unrelated existing cases deselected
  (mixed-site Drop Ship, runtime path, shipping spelling, source-rename target).
  One legacy test module was ignored because its rename-sales-order-workflow
  module is absent. Existing shipping concat warnings remain unrelated.
  Read-only full-snapshot transformations and output contract validation passed;
  no current item WIP totals changed under the new uncapped calculation and no
  execution groups exceeded ordered quantities at that check. No full ETL,
  migration, live assignment, business-record update, or table creation ran.

## Production Planning full-page table toggle

- Added an Expand table button to the top-right of Needs Production Planning's
  card only. It fills the browser viewport; Exit full page or Escape restores
  the normal layout. This is an in-page expansion, not browser fullscreen.
- No form replacement or navigation occurs: unsaved dates/notes and table scroll
  stay intact. Background controls become inert and their prior states are
  restored on exit; save errors remain visible over the expanded card.
- CSS v11 / JS v7. Validation: 56 Planning/UI/JS tests passed. Actual headless
  Edge at 1440x1000 measured the expanded card at x=0,y=0,width=1440,height=1000.
  Escape restored the original card dimensions and background focus access;
  unsaved inputs retained the same nodes and values. Browser-only test values
  were restored; no forms submitted or business/database records changed.
- Preserved preexisting edits to Webpage/lt_planning.py and its tests.
- Follow-up: replaced visible button text with the user's supplied expand icon,
  saved as static/planning-expand.jpg. Retained hidden accessible text and dynamic
  action labels/tooltips without replacing the image on toggle. CSS v12 / JS v8;
  56 tests and actual Edge expand/Escape/input-retention checks passed again.
- Superseded the JPEG with the user's supplied inline Tabler arrows-maximize SVG.
  Removed the newly added unused JPEG asset. Accessible labels and toggle behavior
  remain unchanged; 56 Planning/UI/JS tests passed.
- Icon size subsequently reduced to 18px at the user's request.

## Production Planning single Save: notes-only or assignment

- Replaced the inline assignment button with Save and made its date optional.
  A blank date posts notes-only; an entered date uses the existing validated,
  transactional production assignment/MES handoff. Initial combined review and
  returned-revision behavior remain unchanged.
- `/lt_planning/notes` uses CSRF, shared advisory/row locks, ownership/source and
  exact review/signature/version checks. It does not load/reassess material data,
  publish to MES, change blockers, update schedules/FG/WOs, or alter review rows.
- Draft notes and an append-only note-event journal live inside the existing
  workflow metadata_fields (`planning_note`, `planning_note_history`). Events
  carry submission UUID, assessment link, versions and UTC timestamp. No new
  table/schema required. Empty notes can explicitly clear the current draft.
- Retries with an identical submission are idempotent; altered UUID reuse and
  stale notes versions are rejected. Assignment also checks the draft version
  and carries saved notes when omitted from an API request. Note versions do not
  reset content/source revision, ownership, assessment timestamps, or history.
- Lists render the current assessment-scoped draft. Notes-only success stays on
  the same page, updates the form's version/submission context, and shows
  "Notes saved · date not assigned". Edits typed during a request are retained
  and flagged unsaved. Full-page mode and other unsaved rows are retained.
- CSS v14 / JS v9. Validation: 62 targeted Planning/UI/JS tests and 239 broader
  tests passed, with the same four unrelated legacy cases deselected and one
  missing-module legacy test file ignored. Existing shipping concat warnings
  remain unrelated. Actual Edge verified native blank-date submission and inline
  confirmation using a mocked fetch response, followed by expand/Escape/input
  retention checks. Live GET verified the controls; no real POST, business-data
  update, ETL, migration, or database-table creation was performed.
- Follow-up: removed the persistent Date optional hint. Clicking Save with a
  blank date now asks for confirmation that only notes will save, with no date
  assignment or MES handoff. Cancel sends no request and retains typed notes;
  dated saves do not prompt. JS v10; 62 targeted Planning/UI/JS tests passed.

## Canonical Planning item display

- User requested canonical inventory spelling, not all-uppercase formatting.
  Planning renders saved blockers, allocation labels, readiness suggestions and
  existing review item labels using alias resolution plus case-insensitive lookup
  of the exact inventory_status.Part_Number spelling. Unknown items retain their
  existing spelling. No blockers, quantities, reasons, assessments, or history
  are rewritten or recalculated to resolve display names.
- The inventory-name lookup is read-only and cached for 60 seconds; explicit
  Reload invalidates it. New blocker option labels are canonical but their raw
  submitted values remain unchanged. Review inputs retain original values in
  data attributes so canonical display alone cannot amend source/manual item
  identifiers; explicit manual edits still submit the entered value.
- This is a user-authorized display-only exception to the earlier instruction
  to show saved blocker text verbatim; the saved assessments remain authoritative.
- Planning JS v11 / review JS v8. Validation: 77 targeted Planning/review/UI/JS
  tests passed, including exact casing, aliases, unknown items, cached refresh,
  unchanged saved blocker records, and original review payload preservation.
  Live GET verified the two CPU blocker labels use the inventory's lowercase i.
  No live POST, data correction, migration, or table creation was performed.
