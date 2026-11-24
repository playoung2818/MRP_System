# Item price history (`public.item_cost`)

## Decision

User approved creating `public.item_cost` to collect item purchase-price history from saved daily `POD_*.CSV` exports, accepting missing data. Purpose: future homepage item price-trend chart.

## Sources and coverage

- `public.open_purchase_orders` only holds currently open PODs (replaced on each ETL); closed PODs are not retained. `receiving_log`/`hq_shipping_schedule` have no prices; `item_info` Cost/Price are item-level, not history.
- Archive: OneDrive `Share NTA Warehouse/Daily Update/POD_*.CSV` (QuickBooks open-POD report grouped by item). Snapshots `POD_01_05.CSV` .. `POD_11_23_2026.CSV`; early names have no year.
- `Amount` is the full line amount (`Open Balance` is the unreceived part; e.g. 9 x 179 = 1611, open 4 x 179 = 716). Unit cost = Amount / Qty.
- Partial by design: only PODs open on a saved snapshot day. PODs opened and closed between snapshots, earlier history and missing days are absent. A QuickBooks PO report including closed POs would be authoritative.

## Loader

- `MRP_System 3.0/mrp_system/cli/load_item_cost.py`; run from `MRP_System 3.0`: `python -m mrp_system.cli.load_item_cost [--dry-run] [--folder PATH]`.
- Reuses `transform_pod` (item section headers, Total-row handling, memo fallback, `normalize_item` canonical names), so items use the same keys as the ledger/aliases.
- Snapshot date from file name; year-less names take the earliest year not before the newest order date in the file. Unreadable/non-POD files are skipped and reported (`POD_12_05.CSV` is actually an inventory export; `POD_Watchlist_*`/`POD_joined_*` are ignored by the name pattern).
- Dedupe key `(pod_number, item, order_date, qty, amount)`; keeps vendor/source/memo/deliv/site from latest snapshot, `first_seen`, `last_seen`, `snapshot_count`. Distinct lines on one POD with different qty remain separate; an edited order date creates a second row. Zero-amount lines (75) kept; filter them in charts if needed.
- Idempotent: `CREATE TABLE IF NOT EXISTS` + upsert (first_seen LEAST, last_seen GREATEST). Index `(item, order_date)` for chart queries. Re-run after new daily exports to extend history; not yet wired into ETL.

## Homepage chart (search-for-an-item)

- User chose a search-for-an-item chart. Card “Item price history” below Needs attention on the live homepage (`INDEX_TPL`), script `Webpage/static/home-price.js`, styles appended to `home.css` (v21).
- Read-only endpoints in `Webpage/server.py`: `/api/item_cost/suggest?q=` (up to 15 items, prefix matches first, then most recent purchase; LIKE wildcards escaped with `!`) and `/api/item_cost?item=` (case-insensitive exact match, then `normalize_item` fallback; 404 when not found, 503 on DB error). Queries live in `Webpage/item_cost_history.py`.
- Multiple prices on one POD + item (125 pairs at load time): ~113 are revisions (old version disappears from later snapshots, sometimes with an edited order date); 12 are lines coexisting in the same snapshots (e.g. two service charges). User decision: chart shows only the latest version per POD + item, chosen by latest `last_seen`, then `first_seen`, then `id` (`ROW_NUMBER` in `HISTORY_SQL`). This also collapses coexisting lines to one point. All versions remain in `public.item_cost`.
- Points exclude qty <= 0 and zero-amount lines. Chart: inline SVG step line of unit cost by order date, hover tooltip per POD (date, POD, unit cost, qty, vendor), summary Latest/Lowest/Highest/Purchases, coverage note. Built with `textContent`/SVG DOM, no chart library.
- Tests: `tests/test_item_cost_history.py` (SQLite stand-in with attached `public` schema): case-insensitive resolve, ordering, zero-amount exclusion, normalisation fallback/failure, prefix ranking, literal wildcards. Live checks: suggest for 6218, history for the 40CM item (25 points), 404 for unknown, card and script present; browser screenshot of SSD item verified.

## Initial load (2026-11-24)

- 6,142 rows, 2,371 PODs, 976 items, order dates 2025-04-21 .. 2026-11-23; one file skipped. Second run left row count unchanged (idempotent).
- Spot check: `E-mPCIe-BTWifi-WT-6218_Mod_40CM` unit cost 44 across PODs.
- Tests: `MRP_System 3.0/tests/test_load_item_cost.py` (2 passed): file dating, section parsing, full-amount unit cost, snapshot dedupe, non-POD file skip. Note: patch `normalize_item` with `new=` (a MagicMock is treated as a mapping by `Series.map`).
- No other tables altered.
