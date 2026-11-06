
![Animation1](https://github.com/user-attachments/assets/d8266a1c-08dd-4b89-a996-ca9529e34241)
# QuickBooks MRP System

Turning QuickBooks exports into a time-phased inventory ledger, extend functions based on this ledger table

## Script

- From `MRP_System 3.0`, run `python -m mrp_system.cli.etl` to run the ETL and update the system.


## Database table names

Operational tables use lowercase `snake_case`: `item_info`,
`hq_shipping_schedule`, `open_purchase_orders`, `wo_details`, and
`outgoing_records`. Column names are unchanged.

For an existing installation using the former table names, stop the applications
and ETL, then run from `MRP_System 3.0`:

```powershell
python -m mrp_system.cli.rename_operational_tables
python -m mrp_system.cli.rename_operational_tables --apply
```

The first command previews changes. `--apply` renames the existing tables in one
transaction and verifies table identity and row counts; it does not create or
copy tables. Both-name collisions abort the migration. Restart MRP, MES, and the
Receiving Log app with the updated code afterward.

## Data inputs
- Inventory Status (warehouse snapshot)
- Open Sales Order
- Open Purchase Orders (POD)
- Shipping schedule
- PDF WO references (`pdf_file_log`)


## Ledger timeline generation logic

| Date | Kind | Source | Reference | Delta | CumDelta | Projected_OnHand |
|---|---|---|---|---:|---:|---:|
| 2026-11-04 | OPEN | Snapshot | Opening stock | 0 | 0 | 10 |
| 2026-11-05 | IN | USA | PO-001 | +5 | +5 | 15 |
| 2026-11-05 | OUT | SO | SO-001 | -8 | -3 | 7 |
| 2026-11-06 | OUT | SO | SO-002 | -9 | -12 | **-2** |
| 2099-12-31 | IN | HQ | Unconfirmed supply | +20 | +8 | 18 |

1. **Establish opening stock**
   - Add an `OPEN` row dated today with `Source = Snapshot` and `Delta = 0`.
   - Normalize item names and take `On Hand` from the inventory snapshot, summed by item.

2. **Create inbound supply events**
   - HQ supply becomes `Kind = IN`, `Source = HQ`. Date = Shipping schedule sheet `ship date` + 5
   - Local supply becomes `Kind = IN`, `Source = USA`. Date = POD ship date
   - Use the transformed supply date and a positive `Delta = Qty(+)`.

3. **Create outbound demand events**
   - Each positive required quantity in the structured SO data becomes `Kind = OUT`, `Source = SO`.
   - Use the SO's `Ship Date` as the event date and `Delta = -Qty(-)`.

4. **Calculate the running balance**
   - `CumDelta = cumulative sum of Delta`, separately for each item.
   - `Projected_NAV = Opening + CumDelta`.
   - In Quotation Lookup, this balance is displayed as **Projected_OnHand**.


6. **Display the item's timeline**
   - Rows with `Date >= 2099-12-31` remain in the ledger calculation but appear in the gray **Unconfirmed dates** zone; these are not confirmed arrival dates.
   - The lowest Projected_OnHand across the entire ledger, including unconfirmed dates, is highlighted as green. 
   - A negative lowest Projected_OnHand indicates demand is not fully covered by scheduled inventory. (Mostly caused by unscheduled PODs)
   - `Waiting_Item` shows the latest saved Planning blocking items for the row's `QB Num`




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


