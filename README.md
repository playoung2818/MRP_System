# QuickBooks MRP System

A Python material requirements planning tool that turns QuickBooks exports and an HQ shipping schedule into a dated inventory ledger. Use it to see which parts are projected to run short, when the shortage starts, and which sales-order line drives the balance below zero.

![Demo of the QuickBooks MRP System](https://github.com/user-attachments/assets/d8266a1c-08dd-4b89-a996-ca9529e34241)

## How it works

| Input | Role in planning |
| --- | --- |
| Warehouse inventory snapshot | Opening on-hand quantity by normalized part number |
| Open sales orders | Demand on the planned ship date |
| Open purchase orders | Expected inbound supply |
| HQ shipping schedule | Expected inbound supply, with expansion rules for selected preinstalled configurations |
| Work-order PDF records and MES quantities | Inputs to order preparation and shipment reconciliation |

The Python ETL validates and transforms the source data, builds the ledger, and writes the results to the configured database. It also reports newly detected shortages, shortages that cleared, and changes in projected quantity since the previous run.

## Read a projected inventory timeline

The example uses fictional references and quantities.

| Date | Event | Reference | Change | Projected on hand |
| --- | --- | --- | ---: | ---: |
| Nov 4 | Opening snapshot | — | 0 | 10 |
| Nov 5 | Inbound supply | PO-001 | +5 | 15 |
| Nov 5 | Outbound demand | SO-001 | −8 | 7 |
| Nov 6 | Outbound demand | SO-002 | −9 | **−2** |
| Unconfirmed | Inbound supply | PO-002 | +20 | 18 |

For each item, the ledger adds the running sum of signed movements to opening stock. It processes same-day inbound supply before outbound demand. The Nov 6 sales order is short two units under the recorded schedule. The 20 units of undated supply still appear in the ledger under the `2099-12-31` placeholder, so the final balance of 18 includes supply with no confirmed arrival date.

Quotation Lookup displays the balance as **Projected_OnHand**. The **Waiting_Item** field shows the latest saved blocking items from Planning for the corresponding order.

## Run the ETL

The package requires Python 3.11 or newer. The current ETL expects an existing deployment with source exports, a configured database, and a separate Windows PDF watcher. It refreshes work-order PDF data through that watcher and writes updated planning tables to the configured database.

From the repository root in that environment, run:

```powershell
cd "MRP_System 3.0"
python -m mrp_system.cli.etl
```

| Code | What to look for |
| --- | --- |
| [ETL entry point](MRP_System%203.0/mrp_system/cli/etl.py) | Source validation, shipment reconciliation, shortage reporting, and database writes |
| [Event construction](MRP_System%203.0/mrp_system/ledger/events.py) | Opening balances, movement inputs, and event ordering |
| [Ledger calculation](MRP_System%203.0/mrp_system/ledger/ledger.py) | Running balances and shortage detection |

## Database migration for existing deployments

Operational tables use lowercase `snake_case`: `item_info`, `hq_shipping_schedule`, `open_purchase_orders`, `wo_details`, and `outgoing_records`. Column names are unchanged.

Stop the applications and ETL, then run the preview and apply commands from `MRP_System 3.0`.

```powershell
python -m mrp_system.cli.rename_operational_tables
python -m mrp_system.cli.rename_operational_tables --apply
```

The apply step renames the existing tables in one transaction and verifies table identity and row counts. If both names exist for any table, the migration aborts. Restart MRP, MES, and the Receiving Log app with the updated code afterward.

## Open work

- Make barebone and preinstalled configurations explicit across sales orders, purchase orders, and the HQ schedule.
- Surface part-number and quantity mismatches between QuickBooks purchase orders and the HQ schedule.
- Compare historical ledger projections against subsequent inventory snapshots to measure forecast accuracy.
