# To Do

## React + API Training Rollout

- [ ] Phase 1: Read-only UI (1 week)
  - [ ] Expose `Demand Queue`, `Supply Pool`, and `Coverage` views in read-only mode.
  - [ ] Confirm users can filter and inspect item/SO/POD records.
  - [ ] Goal: users understand fields and workflow without editing risk.

- [ ] Phase 2: Sandbox allocation (1 week)
  - [ ] Enable allocation flow in test environment only.
  - [ ] Practice `stage -> validate -> commit` with sample data.
  - [ ] Run 3 scripted scenarios:
    - [ ] Normal fill
    - [ ] Partial fill
    - [ ] Conflict/stale data

- [ ] Phase 3: Controlled production (1-2 weeks)
  - [ ] Enable production access for a small user group.
  - [ ] Limit to selected item groups/SOs.
  - [ ] Review `allocation_history` and `qa_locking_validation` daily.

- [ ] Phase 4: Full adoption
  - [ ] Make React+API allocation the primary workflow.
  - [ ] Keep Excel as reporting/export only (not source of truth).

## Training Deliverables

- [ ] 30-minute walkthrough
  - [ ] Screen layout
  - [ ] Core allocation/locking rules

- [ ] 30-minute hands-on lab
  - [ ] Assign PODs to 5 shortage lines
  - [ ] Resolve one validation error

- [ ] 1-page cheat sheet
  - [ ] Column definitions
  - [ ] Commit/validation rules
  - [ ] Common errors and fixes
