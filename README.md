
![Animation1](https://github.com/user-attachments/assets/d8266a1c-08dd-4b89-a996-ca9529e34241)
# QuickBooks MRP System

Rebuilt QuickBooks operational views into a unified pipeline, blending inventory, sales orders, purchase orders, picking signals, and shipping data to drive lead-time decisions and Inventory status visability.

## Script

- From `MRP_System 3.0`, run `python -m mrp_system.cli.etl` to run the ETL and update the system.


## Data inputs
- Inventory Status (warehouse snapshot)
- Open Sales Order
- Open Purchase Orders (POD)
- Shipping schedule
- Word pick API (`/api/word-files`) for picked QB Num / WIP
- PDF WO references from Supabase (`pdf_file_log`)

## Run the ETL
- Install deps from `requirements.txt`.
- Configure `DATABASE_DSN` in the environment or `MRP_System 3.0/.env`.
- Update input file paths in `MRP_System 3.0/mrp_system/runtime/paths.py`.
- From `MRP_System 3.0`, run `python -m mrp_system.cli.etl`.
- Unassigned LT orders are available on the webpage: click **Sales Order Not Assigned LT** on the homepage ( `/dashboard/unassigned_so` ). ETL no longer writes `Not_assigned_SO.xlsx`; existing workbooks are left untouched.
- The browser report uses the existing structured sales order data, supports SO/customer/item search, and links to SO details. It preserves SO group banding, red Shortage rows, and yellow Available + On PO cells when Sales/Week is higher. July 4 and December 31 ship dates are treated as unassigned, matching the former export.
- Outputs: inventory_status, structured sales orders, POD, shipping, ledger, item summary, and ATP; pushed to DB and Sheets when configured.

### Manual LT planning workspace

Open **Planning** from the homepage card (`/planning`; `/lt_planning` remains a compatibility alias). The sidebar directly links to each planning stage. Its compact
white-sidebar / blue-accent presentation follows the design language of
[Lookup-Part-Name-by-Serial-Number](https://github.com/playoung2818/Lookup-Part-Name-by-Serial-Number).
The original highlighted component report remains at `/dashboard/unassigned_so`.

The workspace has two active MRP views and one completion/history view:

1. **Needs Material Review:** select **Review SO & Materials**. Review the sales-order
   metadata, source diffs, configuration and item order in the same form as material
   readiness. Enter blockers/quantities/reasons, or explicitly select **No material
   blockers**. **Save SO & Material Review** saves SO content and the material
   assessment in one transaction. No reviewer name is required. Each successful CLI run stages new SO snapshots
   privately in MRP; they do not appear in MES yet.
2. **Needs Production Planning:** a table, not a second detail page. It shows
   recorded blockers, refreshed current blockers after shipment/inventory updates,
   and an inline production-date assignment column. Confirm current readiness and
   the date, then **Assign Production Date**. If data changed, that explicit inline
   confirmation also records a fresh material assessment; old blocker history is
   preserved. Remaining blockers require an override and notes. No operator name
   is required. Ship date is displayed exactly as imported by CLI, never entered.
3. **Planning Complete:** a confirmed production date moves the SO here and publishes
   it to MES atomically. This view is read-only. Later production-date changes are
   made in MES; MRP does not reopen completed orders for routine source updates.

**Sales-order review is integrated into Needs Material Review.** There is no
separate Review Sales Orders menu or independent approval step. Old `/sales_orders`
bookmarks redirect to Planning; old independent approval/sync submissions return
HTTP 410. The combined form retains source diffs, stable item IDs, item ordering,
manual rows, revision checks and saved-WO quantity safeguards.
CLI runs stage new SOs in the existing `public.approved_work_orders` table with
`is_active=FALSE`. SO-content review prepares the data but does not activate it.
Only final production-plan confirmation publishes/activates the SO for MES. No
new tables or copies of business rows are created. Source staging is performed by
MRP CLI, not a separate web Google-sync step. Removed SOs are disabled, not deleted.

**Drop Ship exclusion:** if any source line has Inventory Site `Drop Ship`
(case-insensitive, ignoring surrounding spaces), the entire SO is outside
MRP/MES production scope, including its warehouse lines. CLI excludes it before
ledger calculations/staging; previously staged orders are deactivated on the
next CLI run. Planning and MES also filter existing snapshots immediately, and
reject review/publication/scheduling of excluded SOs. Historical review/planning
records and saved WOs are preserved, not deleted.

**Notes carry through the workflow:** the single **Notes** value entered during
material review is prefilled in production assignment. It can be edited there;
confirmation stores that final value in planning history and publishes it as
`approved_work_orders.metadata_fields.notes`. MES displays it under **Notes** on
Production Planning and retains it when dates are adjusted. Prior review-history
entries preserve their original notes rather than being overwritten. No new
column or table is needed for this shared value.

**MES handoff:** `public.production_overrides` is the shared schedule destination,
keyed by SO number (`wo_number`). Confirming a plan atomically saves MRP history and
upserts the production date. At that same final confirmation, the latest staged
SO content is published/activated after checking saved-WO allocations. There is
no prior MES eligibility or approval step. MES refreshes the shared database and
can adjust production dates later on its Production Planning page. Dates must be
today or a future weekday. Missing CLI staging or invalid allocations roll back
publication, scheduling and planning history together. Existing MES dates are
replaced; finished-goods status is cleared, matching MES schedule-save semantics.
No WO release is created and `WO Details` is not modified. QuickBooks, source ship
dates, and actual production records are untouched. `mes_sent_at` records successful
shared-schedule writes. Historical plans without that marker are labelled as lacking
a recorded handoff; no legacy plans are automatically sent or rescheduled.

The two approved PostgreSQL tables are `public.mrp_review_history` (one row per
manual approval, with an array of approved blocking components plus the system
suggestion snapshot) and `public.lt_planning_history` (one row per assignment or
revision, linked to its exact approved review; ship date is a nullable snapshot
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

The experimental model is retained for evaluation only. To train from the ordered
`items` JSON arrays in `public."WO Details"` (read-only):

```powershell
cd "MRP_System 3.0"
python -m mrp_system.cli.train_item_order
```

The generated `data/wo_item_order.json` is ignored by Git. Training does not
activate it in ETL. Repeated lines, quantities, and other row values are preserved
by the production PDF-only reorder step.

Training counts at most one vote per SO per item pair, excludes conflicting
release votes, and defaults to at least three SOs with 80% agreement. Evaluation
holds out entire SOs, including their releases, and reports pairwise and full
unique-item sequence accuracy plus evidence coverage. The learned fallback is
not guaranteed to reproduce every WO. Cycles are resolved deterministically.
Retrain after WO data or normalization rules change; changed database aliases
invalidate the experimental snapshot. Re-enabling model-based production ordering
requires an explicit code change. No QuickBooks integration or approval is required.

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


## Potential Improvement
-   Dedicate each SO, POD, Shipping schedule is Pre-installed or Barebone is top priority. Idea output is each line in SO, POD, Shipping schedule can be difined as pre/bare. SO it can be acted as a locking item for deidcated SO function. a quick testing is a nice have
-   There's always lots of mismatch btw Quickbooks open purchase order and SAP Shipping schedule, in terms of Item Name and Qty. Data Cleaning is required frequently, a quick diff view is a nice have. (Latest Open PO vs Ledger Report Run window in Supbase)
-   The core idea is building a future ledger, then develop window views from that ledger to gain business idea. So how to make sure the accuracy of the ledger is a important topic.
-   Right now the program definately have lots of duplicate functions that can be combine, so modulating is a nice have. But require dedicated testing before migrate. This could be a fruitful learning journey.
-   Since the ledger is the core pillar, store real inventory status to run a backtesting is how to define success of this project

## Lead Time Assignment Workflow
```text
[Receiving WO]
    |
    v
[Check Inventory Status: available > 0 and ATP > 0?]
    |-- Yes -> [Check Labor Hour]
    |           |-- Yes -> [Assign LT]
    |           |-- No  -> [Wait until labor available]
    |
    |-- No  -> [Check if PO for short items exists]
               |-- Yes -> [Assign LT = Vendor Ship Date + 7]
               |-- No  -> [Ask Taipei to place order]
```

## Projected Inventory Shortages
<img width="1691" height="567" alt="image" src="https://github.com/user-attachments/assets/995b4df0-06fe-4c86-86a8-ba2ff2670364" />

### Logic
- find the rows for this item
- drop rows where:
  - `QB Num == target SO`
  - `Kind == OUT`
- sort remaining ledger rows by date
- rebuild running inventory from:
  - `opening + cumulative delta`
- build `FutureMin_NAV` from rebuilt `Projected_NAV`:
  - scan backward from the last row to the first row
  - at each row, store the minimum `Projected_NAV` from that row forward
- test earliest available date:
  - find the earliest date where `FutureMin_NAV >= required qty`
  - if true, inserting this SO on that date will not make future inventory negative
