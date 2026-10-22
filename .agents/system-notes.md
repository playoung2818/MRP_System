# System implementation notes

Moved from the human-facing README at the user's request. The sections below
preserve the existing implementation notes; they are context, not permission to
run migrations or create database tables. Verify current behavior against code
before relying on a historical description.

### Manual LT planning workspace

Open **Planning** from the homepage card (`/planning`; `/lt_planning` remains a compatibility alias). The sidebar directly links to each planning stage. Its compact
white-sidebar / blue-accent presentation follows the design language of
[Lookup-Part-Name-by-Serial-Number](https://github.com/playoung2818/Lookup-Part-Name-by-Serial-Number).
The original highlighted component report remains at `/dashboard/unassigned_so`.

The Planning search spans only the two current MRP-owned stages. MES-owned and
closed SOs do not appear even when searched. There is no Planning Complete or
history view; previous approvals remain queryable in the database.

The shared `approved_work_orders.workflow_status` is authoritative in both apps:
`needs_material_review`, `needs_production_planning`, `released_to_mes`, `closed`.
The workspace has only two active MRP views:

1. **Needs Material Review:** select **Review SO & Materials**. Review the sales-order
   metadata, source diffs, configuration and item order in the same form as material
   readiness. Enter blockers/quantities/reasons, or explicitly select **No material
   blockers**. **Save SO & Material Review** saves SO content and the material
   assessment in one transaction. No reviewer name is required. Each successful CLI run stages new SO snapshots
   privately in MRP; they do not appear in MES yet.
2. **Needs Production Planning:** a table, not a second detail page. It shows
   the exact recorded blockers from the latest database review,
   and an inline production-date assignment column. Enter the date, then click
   **Assign Production Date**; the button itself confirms the decision, with no
   additional approval or blocker-override checkboxes. List/search views and date
   assignment do not recalculate or replace the saved assessment. Assignment links
   the existing review; source ownership, item/quantity and concurrency checks remain.
   Notes are optional even with blockers; assignment with blockers is flagged in
   planning history. No operator name is required. Ship date is displayed exactly as imported by CLI, never entered.
After assignment the SO leaves MRP and is **Released to MES**. Genuine changes to
items or **total ordered quantities** return it to Needs Material Review with a
**Returned from MES** badge, not a third tab. Approval republishes the revision
straight to MES without another production-date assignment, schedule/finished-goods
reset or WO rewrite. Existing WOs remain visible as records, with a warning;
new releases/picking are blocked while MRP owns the SO. Conflicting allocations
and added work on finished-goods SOs require explicit MES reconciliation after
revision approval. Ship-date, production-date, line-order/ID and fulfillment
quantity changes do not reopen approval.

CLI preserves `Ordered Qty`, `Shipped Qty` and outstanding `Qty` separately.
Snapshots use total ordered `quantity` plus `remaining_quantity` and
`shipped_quantity`. The normalized `approved_order_signature` ignores dates,
line IDs/order, non-production lines and manual extras. Fully shipped lines
omitted by an open-order export are retained when recorded shipment evidence
proves fulfillment; an unexplained disappearance still needs review. Missing
whole SOs are not assumed cancelled. Proven fully MES-shipped orders can close;
otherwise they keep their owner with a source-missing/disposition warning and
blocked new releases.

**Sales-order review is integrated into Needs Material Review.** There is no
separate Review Sales Orders menu or independent approval step. Old `/sales_orders`
bookmarks redirect to Planning; old independent approval/sync submissions return
HTTP 410. The combined form retains source diffs, stable item IDs, item ordering,
manual rows, revision checks and saved-WO quantity safeguards.
CLI runs stage new SOs in the existing `public.approved_work_orders` table as
`workflow_status='needs_material_review'`. SO-content review prepares the data;
initial production-plan confirmation sets `workflow_status='released_to_mes'`. A
returned revision is republished by its combined material/SO review instead.
No new tables or copies of business rows are created. Source staging is performed
by MRP CLI, not a separate web Google-sync step. Excluded SOs are closed, not deleted;
missing SOs with active work require disposition.

**Drop Ship exclusion:** source lines with Inventory Site `Drop Ship`
(case-insensitive, ignoring surrounding spaces) are outside MRP/MES production
scope. Warehouse lines on the same SO remain eligible. CLI filters only the
Drop Ship lines before ledger calculations/staging; review and publication
likewise retain only warehouse lines. Pure Drop Ship SOs remain excluded.
Previously closed mixed-site SOs re-enter material review after reimport; the
filter change does not silently approve or release them. Historical review/planning
records and saved WOs are preserved, not deleted.

**Notes carry through the workflow:** the single **Notes** value entered during
material review is prefilled in production assignment. It can be edited there;
confirmation stores that final value in planning history and publishes it as
`approved_work_orders.metadata_fields.notes`. MES displays it under **Notes** on
Production Planning and retains it when dates are adjusted. Prior review-history
entries preserve their original notes rather than being overwritten. No new
column or table is needed for this shared value.

**MES handoff:** initial assignment saves history, publishes content, sets the
shared owner and upserts `public.production_overrides` in one transaction. New
production dates must be today/future weekdays and not after a real source ship
date. Initial handoff clears finished-goods state, matching normal scheduling.
Returned-revision approval preserves the existing schedule and finished-goods
state; inconsistent allocations/added work are flagged for MES reconciliation.
MES releases use ordered requirements minus externally shipped quantities,
without subtracting MES-recorded shipments twice. No automatic WO release or
WO rewrite is performed. Historical execution timestamps remain unchanged.

**Source presence:** `public.approved_work_orders.source_present` indicates
presence in the latest successfully staged eligible Open Sales Order import—not
Google Sheets presence, approval, cancellation, or ownership. Existing installs
with the old column name must run this explicit rename before starting upgraded
MRP/MES (preview by default; apply creates a private backup and preserves all values):

```powershell
cd "MRP_System 3.0"
python -m mrp_system.cli.rename_source_present
python -m mrp_system.cli.rename_source_present --apply
```

**Current schema:** the one-time ownership backfill and independent legacy
approval/sync helpers have been removed. MES validates the current shared SO
schema without adding approval columns, renaming legacy tables, or inferring
ownership from old approval flags. Existing history and cutover backups remain
unchanged. Ownership comes only from `workflow_status`; source eligibility is
tracked separately by `source_present`.

For a fresh installation, provision the current shared SO model and run the
explicit LT planning schema setup below. Fresh history-table definitions include
incident, source-revision, review-kind and handoff fields. Do not recreate
existing business tables to upgrade a populated database.

The two approved PostgreSQL tables are `public.mrp_review_history` (one row per
manual approval, with an array of approved blocking components plus the system
suggestion snapshot) and `public.lt_planning_history` (one row per initial assignment,
linked to its exact approved review; ship date is a nullable snapshot
of the source at save time). The current displayed ship date always comes from
MRP source data, not the historical snapshot. `assigned_at` is the plan save time,
not the actual time LT was assigned in Excel. Empty blocker arrays explicitly
record a no-blockers review. ETL does not write, replace, or create these tables.
For a new deployment, create them explicitly after approval:

```powershell
cd "MRP_System 3.0"
python -m mrp_system.cli.setup_lt_planning
```

`run_id` identifies the manual assessment, not a scheduled ETL execution;
`source_fingerprint` identifies the source content and assessment day used.
Use `jsonb_array_elements(blockers)` to expand approved components for analysis.
New reviews store `incident_id`, `source_revision`, `review_kind` (initial,
returned_from_mes or readiness_refresh), and revision handoff `mes_sent_at`.
Group repeated assessments by SO + incident + item to avoid counting the same
shortage repeatedly; legacy incident IDs remain NULL rather than fabricated.
Observations and handoff times are not proof of the exact physical receipt time.
Both histories are append-only through the application. Duplicate form submissions
are protected by UUIDs; concurrent reviews/assignments require reloading rather
than silently overwriting another decision.

Material suggestions replay the ledger with the target SO's OUT demand removed,
retain competing demand, and exclude unknown-date inbound supply. Readiness is
checked at daily close against subsequent projected balances. Uncovered quantity
is the SO requirement not covered at its best feasible date before 2099-12-31,
clamped between zero and this SO's requirement. It is not a shortage against a
specific planned start date, and quantities across independent SO assessments
must **not** be summed as a purchase recommendation. POD reasons are advisory;
verify vendor shipping and effective ledger receipts during manual review.

Set a stable `FLASK_SECRET_KEY` for persistent sessions or multiple web workers.
Forms use CSRF protection, explicit confirmation and source-change checks.
Neither reviews nor assignments require an operator name; history uses neutral
`Manual review` and `Manual assignment` markers. The current app does not
authenticate operator identity.
Keep the workspace behind trusted-network access or an authenticated proxy.
History timestamps are stored as timezone-aware PostgreSQL timestamps.

### Part-number normalization

Direct part-number normalization uses rows from
`public.part_number_aliases`: `alias_part` maps to `canonical_part`. Many aliases
can share one canonical part. Matching ignores case and surrounding whitespace,
while the output retains the database's canonical spelling. Unknown names remain
unchanged unless an existing Jetson regex fallback applies. The table no longer
uses `alias_type`, `confidence`, or `active`; all rows are used. Ambiguous aliases
cause an error rather than an arbitrary mapping.
The mapping is cached and refreshed at ETL startup and web data reload, with no
per-item database queries. Database failures stop refresh instead of silently
using the removed hard-coded mappings.

### SO item ordering (PDF only)

Production ETL uses only the PDF item sequence, matched by canonical part number
and occurrence. Unmatched lines retain their relative source order after matched
lines. With no PDF match, source order is retained. No trained model or WO Details
sequence is used to order production SOs.

Repeated lines, quantities, and other row values are preserved by the PDF-only
reorder step. There is no learned item-order model or generated ordering snapshot.

### POD inventory-site snapshot

ETL derives non-default POD inventory sites from the current POD export and saves
an atomic JSON snapshot to `MRP_System 3.0/data/pod_site.json`. This generated
file is ignored by Git; normalization source code is never rewritten. Set the
`POD_SITE_PATH` environment variable to use a different writable location.
A missing snapshot starts empty and is populated by ETL. Refresh updates ledger
exclusions in the same process, so the current run uses the current POD sites.
Standalone ledger callers load the last saved snapshot when the module imports.

## Remark

``` text
- expand_sap_preinstalled() checks, in order:
1. fixed_group   — all rows, before Pre/Bare split
2. core_group    — Pre rows only, inside expand_preinstalled_row
3. nuvo_group    — all rows, final catch-all pass
```
