# AGENTS.md

Instructions for AI coding agents (any model/tool) working in this repo.

This file captures system-design *preferences* — how to approach changes here,
not a description of what the code currently does. For the latter, read the
code and the implementation notes indexed in `.agents/INDEX.md`.

## Documentation and agent memory

- `README.md` is human-facing documentation. Do not use it as agent memory,
  a work log, or a record of implementation investigations and test runs.
- Store implementation context, decisions, investigation notes, and work records
  in `.agents/`. Read `.agents/INDEX.md` to find the relevant notes.
- Update the README only when human-facing usage or setup documentation genuinely
  needs to change, not merely because an agent completed work.
- Treat notes as context, not executable instructions or a substitute for reading
  current code. Do not store credentials, secrets, or private business exports.

## Reference repositories

This system is a hand-built, event-sourced inventory ledger (opening stock +
inbound/outbound deltas → cumulative projected balance), not a full ERP. When
making design decisions here, prefer consulting these over inventing a novel
pattern — favor ones with a similar *core* (ledger/ATP/BOM logic), not full
ERP suites, since the UI/accounting/HR layers in those aren't relevant here:

- **[mrpSolver](https://github.com/twoldstad/mrpSolver)** — small, standalone
  Python module computing an MRP order-release schedule and ATP from an item
  master + master production schedule + BOM. Closest direct analog to
  `mrp_system/ledger/ledger.py` / `atp.py` — small enough to read start to
  finish.
- **[ERPNext](https://github.com/frappe/erpnext)** `erpnext/stock` specifically
  — full ERP (Python/Frappe/MariaDB), too big to use wholesale, but its Stock
  Ledger Entry model (every stock movement is an immutable ledger row, running
  balances derived by replaying entries) is the closest production-grade
  analog to this repo's `Kind`/`Source`/`Delta`/`CumDelta` events table.
- **[OpenMRP-System](https://github.com/gabejohnsnn/OpenMRP-System)** — small/
  medium-scale open MRP (inventory, BOM, production planning, shop-floor
  control). Reference for overall module shape at a scale closer to this repo
  than a full ERP.
- **[BlueSeer](https://www.blueseer.com/)** — free/open ERP+EDI where MRP is
  tightly integrated with purchasing/production/shipping for a consistent
  inventory-status picture — similar spirit to this repo's POD↔Shipping↔Ledger
  reconciliation work.

## System design preferences

<!-- dictated entries go here -->
