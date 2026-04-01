# Name: erp-ledger-maintainer
description: Maintain and validate MRP ETL and ledger workflows. Use when tasks involve parsing or normalization changes in Open_Purchase_Orders, NT Shipping Schedule, and ledger_analytics; reconciling item/qty mismatches; updating Supabase QA report tests; and verifying parser changes with before-vs-after diffs.


# ERP Ledger Maintainer

Follow this workflow when changing parsing, normalization, or reconciliation behavior in Project Folder.

## Use the right scope

- Locate the source stream first: `Open_Purchase_Orders` (POD), `NT Shipping Schedule` (shipping), `Open_Sales_Order` (SO), or `ledger_analytics` (ledger events).
- Keep source-specific logic separate from shared normalization logic.
- Preserve existing business filters unless explicitly asked to change them.

## Key files

- `ERP_System 2.0/core.py`
- `ERP_System 2.0/ledger.py`
- `ERP_System 2.0/erp_normalize.py`
- `ERP_System 2.0/tests/test_open_po_vs_ledger_in_summary.py`
- `ERP_System 2.0/tests/test_pod_vs_ledger_in.py`
- `ERP_System 2.0/tests/test_shipping_reference_so_match.py`

## Parser change rules

- Change POD parsing in `transform_pod` only for POD-specific structure problems.
- Change shipping expansion behavior in `transform_shipping` or `expand_nav_preinstalled` only for shipping-specific behavior.
- Put reusable item alias/canonical rules in `erp_normalize.py`.
- Treat `ITEM_MAPPINGS` direct mappings as exact-match and case-sensitive unless intentionally changed.

## Required validation after parser/normalization edits

1. Run targeted pytest for the touched behavior.
2. Run a before-vs-after diff for the affected parser output.
3. Check at least one concrete example row (POD/QB/item) end-to-end through report output.
4. Confirm whether mismatch changes are expected or regressions.

## Diff protocol for `transform_pod`

- Compare old-vs-new parsed rows with stable keys and occurrence index.
- Compare item names row-by-row in order.
- Summarize transition pairs (`old_item -> new_item`) with counts.
- Highlight business-critical transitions (CPU, GPU, SSD, cable-kit components, service lines).

## Reconciliation expectations

- For `open_po_qty` checks, always state active filters used in the test.
- Do not use `Ship Date` / estimated date from `Open_Purchase_Orders` as a default matching key when comparing POD source rows to `ledger_analytics`. Treat it as planning metadata, not a reliable identity field, unless the task explicitly requires date-based analysis.
- If `open_po_qty` is zero unexpectedly, verify:
  - item key split/alias mismatch,
  - vendor exclusion filter,
  - POD scope filter,
  - stale table data from pre-change ETL runs.
- If ship date is `NULL`, verify whether ship date source is ledger or shipping table before concluding parse failure.

## QA logging expectations

- Keep report schema additive and stable where possible.
- Include `filter_used` text in each run summary.
- Include row-level context (`item_key`, qty fields, POD context) for mismatches.

## Output style for investigations

- Provide the exact reason for mismatch with one concrete row example.
- Provide source table evidence (`Open_Purchase_Orders`, `NT Shipping Schedule`, `ledger_analytics`).
- Provide the minimal code change needed, then rerun the relevant test.

## Safety checks

- Do not change multiple business rules in one edit unless explicitly requested.
- Do not silently change filter scope in tests.
- Do not assume mapping fixes old DB rows; re-run ETL or explain staleness explicitly.


## Business Logic
[IN] -- `Open_Purchase_Orders`, `NT Shipping Schedule`
[OUT] -- `Open_Sales_Order` (SO)

Generate `ledger_analytics` as core pillar as this MRP system

For each item in wo_structured table, it has status column, for Status == 'Waiting' or 'Shortage', I need build a reference table where i can assign POD to that item.
At the same time, a Supply ussage view is required. So i can know how many supply by item by pod i can assign. After assign POD to that item, the qty of pod is locked to that SO that item. 


Success defination: a workflow where i can assign supply to demand easily. a test that make sure every item in ledger would never have negative projected qty any time.


## Ddefination of "Ready to be assigned"
For a So that have not been assigned Lead Time (2099-07-04), if there's any date before 2099-07-04 can be assigned to this SO, without having negative "Projected_NAV" in the ledger (w or w/o including date == 2099-07-04) for all the items in this So, then it can be defined as "Ready to be assigned"

## Lead Time Assign Model
First run the model to assign lead time to unassigned SOs with restict constrain: Assign date before 2099-07-04, without having negative "Projected_NAV" in the ledger (including date == 2099-07-04) for all the items in this SO
Then run the model to assign lead time to unassigned SOs with more lose constrain: Assign date before 2099-07-04, without having negative "Projected_NAV" in the ledger (before date == 2099-07-04) for all the items in this SO
Then run a diff of these two runs, let Admin(Me) to decide whether diff SOs should be assigned Lead Time.
I would write constrains/note in a file, so every time run this model, read this file first. For the content of the constrains/note, for example, link SO-20260353 to POD-260255 and this SO/POD is pre-installed. Another example is like saying, SO-20260353 is rush order, fulfill this first. 

Discussion Topic:
1. Compare using script to run this model VS. using LLM api to run this model. (Choose script)
2. About constrains/note, compare using markdown VS.  using excel   Or if there's any other solutions please recommend. (Choose Markdown, Only used by myself)
3. Compare using LLM VS. Prescriptive Analysis [Linear Programming (LP),Integer Programming (IP),Dynamic Programming, Markov Decision Processes, Simulation / Monte Carlo, Reinforcement Learning] for this project as the infrusture (Choose Prescriptive Analysis)

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


Core:
```python
projected = scoped["Projected_NAV"].tolist()
future_min: list[float] = [0.0] * len(projected)
current_min = float("inf")
for idx in range(len(projected) - 1, -1, -1):
    value = projected[idx]
    if pd.notna(value):
        current_min = min(current_min, float(value))
    future_min[idx] = current_min
scoped["FutureMin_NAV"] = future_min
```
