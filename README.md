
![Animation1](https://github.com/user-attachments/assets/d8266a1c-08dd-4b89-a996-ca9529e34241)
# QuickBooks Inventory Analytics – Portfolio Project

I rebuilt QuickBooks operational views into a unified analytics pipeline, blending inventory, sales orders, purchase orders, picking signals, and shipping data to drive lead-time decisions and sales order visibility.

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
- The Not_assigned_SO export defaults to the repository root; set `NOT_ASSIGNED_SO_EXPORT_PATH` to override it.
- Outputs: inventory_status, structured sales orders, POD, shipping, ledger, item summary, ATP, and Not_assigned_SO exports; pushed to DB and Sheets when configured.

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
