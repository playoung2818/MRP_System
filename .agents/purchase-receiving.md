# Purchase-order search and receiving

## User scope / navigation

- Extend port-5002 `/purchase_orders` with POD# search and a POD detail/receiving
  page, similar to MES order lookup/detail and the port-5000 receiving flow.
- New route: `/purchase_orders/pod/<pod_number>` (GET/POST). Both prefixed POD
  numbers and six-to-ten-digit numeric IDs are accepted. POD# and QB Num are the
  SAME identifier: the webpage shows one POD# field and one leading POD# column;
  legacy qb_num URL filters map into pod_num. Raw export/schema columns remain
  compatible. Listing POD# cells open the detail page; vendor/date filters and Excel exports
  retain the POD filter. No homepage navigation changes.

## Existing schema and safety

- The actual existing table is `public.receiving_log` (not the user's spelling
  recevin_log), with serial/default-sequence id, serial_number, entry_date,
  invoice_number, box_number, pod_number, part_number, quantity, Reference.
- Receipt writes are INSERT ONLY. No table creation, migration, history updates,
  deletions, QuickBooks PO quantity updates, or inventory balance updates.
- Uses SQLAlchemy reflection, never imports port-5000 application startup/ORM.
- Receiving confirmation locks receiving_log in SHARE ROW EXCLUSIVE mode, the
  same lock used by port 5000, then takes a SHARE lock on Open_Purchase_Orders.
  Local lock timeout is 10 seconds; all validation/inserts commit atomically or
  roll back. Tests only use mocks/metadata definitions, never create databases.
- Source PO quantities and physical receipt logs are separate facts. Do not
  subtract receipts again from imported outstanding or silently infer QB updates.

## Input / preview behavior

- Manual date/invoice/box plus repeatable part/serial/QTY rows. QTY is the TOTAL
  expected quantity for that manual row, NOT a multiplier per serial. Each serial
  creates a one-unit receipt. Preview warns if nonblank serial-line count differs
  from QTY (non-blocking; actual previewed serial units are recorded). All-NA
  input skips this check and creates one batch with total QTY, even if NA is
  repeated across lines. Workbook QTY semantics remain one quantity per record.
  Manual entries survive preview/validation rendering.
- Non-Neousys UI expands QTY into individual serial controls (1–1000 per part
  row), except case-insensitive Dust-prefixed items, which keep the textarea.
  I/Core-prefixed items get separate serial and batch inputs, stored as
  serial_batch in the existing serial_number field. Both fields are validated
  before preview and again at confirmation; NA remains allowed. Neousys layout
  stays unchanged. Source/master names retain canonical case.
- User explicitly removed shared-batch/apply-to-all behavior. Enter in a CPU
  serial field focuses that row's batch; Enter in batch focuses the next serial.
  Enter is prevented from submitting. Ordinary serialized parts advance to the
  next serial. QTY edits preserve typed entries; multiline serial paste supported.
- Scanner follow-up: 54 receiving/search/export/JS tests passed. Actual Edge DOM
  verified 10 pairs, underscore combinations, Dust exception, both Enter focus
  transitions, and no shared-batch control. No screenshots were captured for this
  follow-up, and submission was prevented (no live receiving POST).
- Workbook upload is shown/enabled ONLY when all current POD Source Name values
  equal `Neousys Technology Incorp.` (trimmed). Other/blank/mixed vendors keep
  manual receiving only. Backend rejects upload attempts for other vendors too.
  For those other/blank/mixed vendors, Box # is always `1--1` and read-only;
  Invoice # is also read-only and equals Source Name (distinct vendor names
  joined with ' / ' for anomalous mixed-source PODs). Preview overrides posted
  box/invoice values and confirmation rechecks both. Neousys fields stay editable.
  Existing receiving history is never rewritten by these rules.
- Workbook format matches port 5000: first worksheet, row-one headers Date,
  Inv#, Box #, POD#, Part#, SN#, QTY. 10 MB request / 100 MB decompressed /
  20,000-row bounds. Formulas rejected, date/part required, quantities finite
  and positive, blank quantity defaults to 1, blank/nan serials skipped.
- Only the selected current POD's parts are accepted. New receipt part names
  resolve aliases and preserve exact inventory master spelling; saved history
  is canonicalized for display only, never rewritten.
- Preview is read-only. Signed/session-CSRF-bound/POD-bound tokens expire in
  30 minutes and retain raw source fingerprints and affected history snapshots.
- Exact receipts/repeated uploaded rows are skipped. Serial identity ignores
  case and whitespace. NA identity uses the full receipt event (not literal NA).
- Conflicts default to Skip. Explicit Add receipt event(s) preserves all history;
  no replacement/deletion options exist. Preview must be confirmed to insert.
- Confirm rechecks source, allowed parts and receipt snapshots under locks.
  Changed previews abort before any INSERT; exact retries skip already-inserted
  events without duplicating them. Other database fields, including Reference,
  are never overwritten. Recent POD receipts are shown separately (latest 500).

## Files / validation

- `mrp_system/purchase_order_lookup.py`: listing/filter/export integration and
  blueprint registration. `mrp_system/purchase_receiving.py`: validation,
  workbook reader, preview/append service, route and detail template.
- `tests/test_purchase_receiving.py`: pure/mocked input, preview, stale/retry,
  append-only/rollback, CSRF/tamper/expiry/POD-binding, workbook route tests.
- Full MRP regression: 283 passed, four known unrelated cases deselected and
  missing rename-sales-order-workflow test module ignored. Existing unrelated
  shipping concat warnings remain.
- Live checks were GET-only, plus read-only metadata/INSERT/sequence permission
  checks. Actual Edge tested add/remove rows, native form validation and browser
  layout. Its submit event was prevented; no live POST or receipt insertion.
- No real receiving data, database tables, or inventory quantities were changed
  during implementation/verification. Screenshots remain outside Git in Temp.
