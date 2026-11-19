# Safety-stock continuous improvement

## Project agreements

- This is an ongoing MRP improvement project, not a one-time feature.
- Keep the roadmap, decisions, implementation context, and work records in repository Markdown files under `.agents/`.
- Do not connect to the Notion app or read/write Notion for this project. The previously reviewed page is background only; Markdown is the ongoing source of truth.
- Primary outcome measure: the existing SO Planning KPI in the Planning module. Read `.agents/planning-kpi.md` and verify current code before defining evaluations or implementing changes.
- Supporting checks: inventory/excess-stock guardrails and automated correctness/runtime tests; these do not replace the SO Planning KPI.
- No performance baseline or improvement result has been established yet.

## Starting scope (background, not approved implementation)

- Local vendors: time-phased ledger planning, comparing minimum projected inventory over the horizon with safety-stock thresholds. Initial proposal: two weeks of sales minimum and six weeks maximum.
- NT purchasing: static inventory position (`Available + On PO`) and an order-up-to target covering lead time, review period, and safety buffer.
- Reliable PO shipment/arrival dates are a prerequisite for reliable time-phased planning.

## Open decisions

- Separate parameter-maintenance frequency from replenishment-review frequency; the background proposal mixes quarterly review with a weekly example.
- Define the six-week maximum: replenishment target, warning, or hard cap.
- Verify QB Available semantics to avoid double-counting committed demand.
- Define sales/week, demand history window, component-demand treatment, and planning horizon.
- Define treatment of late/undated POs and partial receipts.
- Verify how the existing SO Planning KPI can evaluate affected SOs and how long the evaluation window must be.

## Roadmap

All milestones below are proposed and not yet implemented or validated.

1. **Baseline and definitions** — inspect existing KPI and planning logic; agree on pilot scope, owners, policy definitions, and baseline window.
2. **Data reliability** — assess missing/stale arrival dates, lead times, and demand inputs; define validation checks.
3. **Shadow recommendations** — evaluate proposed policies without changing orders; review exceptions with planners.
4. **Pilot and evaluation** — apply approved changes to selected items; compare the existing SO Planning KPI with baseline, allowing for replenishment lead times; monitor inventory guardrails.
5. **Continuous tuning** — record evidence, retain/adjust/roll back changes, and maintain regression tests.

## Record format for each improvement

- ID and status
- Owner and affected items/vendors
- Hypothesis and proposed change
- Acceptance criteria and inventory guardrails
- Baseline and evaluation windows
- SO Planning KPI definition/version used
- Code revision, input snapshot reference (no private exports), and policy parameters
- Correctness/runtime test evidence
- KPI results and limitations/confounding factors
- Decision and next review date

## Work record

- Initial agreement: use the existing SO Planning KPI as the primary performance measure.
- Storage agreement: repository Markdown holds the roadmap and project memory; no further Notion connection.
- Initial roadmap recorded only; no application changes, database changes, or performance tests performed.
