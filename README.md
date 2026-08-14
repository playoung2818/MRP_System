
![Animation1](https://github.com/user-attachments/assets/d8266a1c-08dd-4b89-a996-ca9529e34241)
# QuickBooks Inventory Analytics – Portfolio Project

I rebuilt QuickBooks operational views into a unified analytics pipeline, blending inventory, sales orders, purchase orders, picking signals, and shipping data to drive lead-time decisions and sales order visibility.


## Data inputs
- Inventory Status (warehouse snapshot)
- Open Sales Order
- Open Purchase Orders (POD)
- Shipping schedule
- Word pick API (`/api/word-files`) for picked QB Num / WIP
- PDF WO references from Supabase (`pdf_file_log`)

## Run the ETL
- Install deps from `requirements.txt`.
- Configure DB DSN in `db_config.py` (or environment).
- Update file paths in `ERP_System 3.0/config.py`.
- Run `erp.bat` or `python "ERP_System 3.0/etl.py"`.
- Outputs: inventory_status, structured sales orders, POD, shipping, ledger, item summary, ATP, and Not_assigned_SO exports; pushed to DB and Sheets when configured.

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
