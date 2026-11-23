# Quotation ledger presentation

## Confirmed-horizon minimum highlight

- Request: highlight the lowest `Projected_OnHand` before the `Unconfirmed dates` section in orange.
- `Webpage/quotation_ledger.py::confirmed_minimum_flags` computes the numeric minimum only for valid dates strictly before `2099-12-31`. Invalid dates/values and all sentinel/pending rows are excluded. All exact numeric ties highlight; comparison precedes display rounding. OPEN snapshot participates as a confirmed balance.
- `Webpage/server.py::quotation_lookup` applies the flags after sorting, before numeric formatting; no ledger calculations or database writes changed.
- Follow-up request: also mark the whole-timeline minimum green, and color **only Date and Projected_OnHand cells**, not the entire row.
- `overall_minimum_flags` includes every numeric balance, including pending dates; all ties participate. Green takes priority when a row is both minima.
- `Webpage/ui.py::QUOTE_TPL` uses orange confirmed-minimum cells and green overall-minimum cells, with legends explaining both. Pending rows cannot receive orange but may receive green. Removed whole-row minimum warning; existing neutral pending-zone shading remains.
- Regression coverage: `MRP_System 3.0/tests/test_quotation_ledger.py` covers pending lower values, ties, sentinel boundary, invalid/empty inputs, pre-rounding comparison, and template pending guard.
- Validation: combined quotation/UI run **20 passed, 2 failed**; failures are the already-known homepage literal-class and `production_overrides` absence assertions in `test_web_ui_assets.py`, not related to this change. One pandas date-inference warning in invalid-input test.
- Live read-only GET for the 40CM WiFi item confirmed orange style, one confirmed highlighted value, and no highlighted values below the unconfirmed divider. Do not treat the current item's value as fixed test data; input data/aliases can change independently.
- Follow-up validation: **12 passed** across quotation ledger/blocker/card tests, including overall ties, invalid values, green precedence and Date/Projected_OnHand-only coloring. Existing pandas invalid-input inference warning remains.
- `git diff --check` passed. No database/schema changes or ETL executed.
