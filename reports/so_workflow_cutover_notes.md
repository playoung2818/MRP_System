# SO workflow cutover — 2026-10-20

Historical note: on 2026-10-22, `approved_work_orders` was renamed in place to
`sales_order_workflow`. The original names below describe the cutover at that time.

The applications now share `public.approved_work_orders.workflow_status`.
No new database tables were created. Existing review/planning histories and WO
execution rows were not rewritten.

## Current-state verification

- Needs Material Review: 32 SOs.
- Needs Production Planning: 4 SOs.
- Released to MES: 108 SOs.
- Closed/excluded/previously inactive records: 129.
- Active MRP/MES owner overlap: **0**.
- Current state export: `so_workflow_current_state.csv`.

The initial migration preview included three EO-prefixed legacy aliases. The
existing CLI canonicalizes these to SO-prefixed identifiers. Those inactive
aliases (EO-20260002/3/4) were archived after verifying that they had no WOs;
the existing SO-20260002/3/4 records remain MES-owned. No WO identifiers were
renamed. The initial preview is retained as an audit artifact, not a current
queue count.

SO-20260750 had an old approval equal to the current outstanding quantity (13
systems) while the imported order totals 20 with 7 already shipped. Its comparison
baseline was normalized to total ordered quantities. Migration did not expand or
rewrite its published WO requirements. SO-20261324 similarly stays MES-owned:
its seven ordered systems, six shipped and one outstanding are not interpreted
as a customer amendment.

SO-20261389 remains MES-owned with a source-missing/disposition warning. Absence
from an open-order export was not treated as proof of cancellation.

## Safety and validation

- MRP live pages verified; no Planning Complete/history list, and searching for
  MES-owned orders does not return them to MRP.
- MES home, planning and WO pages rendered against the shared database using a
  read-only Flask test client. No MES server was listening locally on port 5001
  during verification; start/restart MES to serve the upgraded code.
- Actual PostgreSQL CLI staging and returned-revision publication were exercised
  inside outer transactions that were rolled back. Original SO/history state
  was checked afterward; no test approval/import was committed.
- The returned-revision test verified unchanged schedule, finished-goods marker,
  WO rows and original planning history; added finished-goods work was flagged.
- Source shipments overlapping WOs still marked unshipped produce MES shipment
  reconciliation warnings, not invented customer quantity reductions or MRP
  returns. Exact shipment attribution must be resolved operationally.
- This change does not automatically cancel WOs, return material, reverse
  production, or transfer excess finished goods to stock. Such dispositions
  require an explicit operational decision; unresolved conflicts block releases.

A complete pre-migration SO-row backup is under `private_backups/` (git-ignored).
Existing MRP history rows remain queryable; new observations include incident,
source-revision and review-kind fields for blocker analysis.

Validation: 156 MRP tests passed with the two previously failing daily-path and
NRU-description tests deselected. All 87 MES application tests passed; the four
unrelated PowerShell/database-backup environment checks were excluded (their
preexisting configuration/PowerShell failures remain unresolved).
