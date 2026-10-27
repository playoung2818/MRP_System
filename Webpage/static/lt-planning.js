"use strict";
(() => {
  const csrfElement = document.getElementById("planning-csrf");
  if (!csrfElement) return;
  const csrf = JSON.parse(csrfElement.textContent);
  function showError(message) {
    const box = document.getElementById("save-message");
    box.className = "alert alert-danger"; box.textContent = message;
    box.scrollIntoView({behavior: "smooth", block: "center"});
  }
  async function save(form, action, payload) {
    const button = form.querySelector('button[type="submit"]');
    button.disabled = true;
    try {
      const response = await fetch(`/lt_planning/${action}`, {
        method: "POST", headers: {"Content-Type": "application/json", "X-CSRF-Token": csrf},
        body: JSON.stringify(payload)
      });
      if (!response.headers.get("content-type")?.includes("application/json")) {
        throw new Error("Planning data is unavailable or the session expired. Reload and try again.");
      }
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || "Unable to save.");
      const url = new URL(window.location.href);
      url.searchParams.delete("reload"); url.searchParams.delete("assigned"); url.searchParams.delete("so");
      url.searchParams.delete("q");
      url.searchParams.set("status", action === "review"
        ? "needs_material_review" : (result.next_status || "needs_material_review"));
      url.hash = ""; window.location.assign(url.toString());
    } catch (error) {
      showError(error.message || "Network error. Retry the same form; duplicate submissions are protected.");
      button.disabled = false;
    }
  }
  document.querySelectorAll(".inline-plan-form").forEach(form => {
    form.addEventListener("submit", event => {
      event.preventDefault();
      const fields = form.elements;
      save(form, "assign", {...JSON.parse(form.dataset.context),
        production_start: fields.production_start.value, notes: fields.notes.value});
    });
  });
  const contextElement = document.getElementById("planning-context");
  if (!contextElement) return;
  const context = JSON.parse(contextElement.textContent);
  const rows = document.getElementById("blocker-rows");
  const noBlockersInput = document.getElementById("no-blockers");
  const productionPanel = document.getElementById("review-production-panel");
  const productionInput = document.getElementById("review-production-start");
  const reviewSubmit = document.getElementById("review-submit");
  const reviewHint = document.getElementById("review-save-hint");
  const defaultLabel = reviewSubmit.textContent;
  const defaultHint = reviewHint.textContent;
  function updateImmediateAssignment() {
    const assignNow = noBlockersInput.checked && Boolean(productionPanel);
    if (productionPanel) {
      productionPanel.hidden = !assignNow;
      productionInput.disabled = !assignNow;
      productionInput.required = assignNow;
    }
    reviewSubmit.textContent = assignNow ? "Save Review & Assign Production Date" : defaultLabel;
    reviewHint.textContent = assignNow
      ? "Saves the SO review, material review and production date together, then releases this SO to MES."
      : defaultHint;
  }
  noBlockersInput.addEventListener("change", updateImmediateAssignment);
  updateImmediateAssignment();
  function option(value, label) {
    const el = document.createElement("option"); el.value = value; el.textContent = label; return el;
  }
  document.getElementById("add-blocker").addEventListener("click", () => {
    const tr = document.createElement("tr");
    const item = document.createElement("select");
    item.className = "form-select item-select"; item.required = true;
    item.setAttribute("aria-label", "Blocking component"); item.append(option("", "Select component"));
    context.suggestions.forEach(s => item.append(option(s.item, s.item)));
    const pod = document.createElement("input");
    pod.type = "text"; pod.className = "form-control"; pod.maxLength = 200;
    pod.placeholder = "POD# (optional)"; pod.setAttribute("aria-label", "POD number");
    const required = document.createElement("input");
    required.type = "number"; required.readOnly = true; required.className = "form-control";
    required.setAttribute("aria-label", "SO required quantity");
    const uncovered = document.createElement("input");
    uncovered.type = "number"; uncovered.step = "any"; uncovered.min = "0.000001";
    uncovered.required = true; uncovered.className = "form-control";
    uncovered.setAttribute("aria-label", "Approved uncovered quantity");
    item.addEventListener("change", () => {
      const suggestion = context.suggestions.find(s => s.item === item.value);
      required.value = suggestion ? suggestion.required_qty : ""; uncovered.max = required.value;
    });
    const remove = document.createElement("button"); remove.type = "button";
    remove.className = "btn btn-sm btn-outline-danger"; remove.textContent = "Remove";
    remove.addEventListener("click", () => tr.remove());
    [item, pod, required, uncovered, remove].forEach(el => {const td = document.createElement("td"); td.append(el); tr.append(td);});
    tr.controls = {item, pod, required, uncovered}; rows.append(tr); item.focus();
  });
  document.getElementById("review-form").addEventListener("submit", event => {
    event.preventDefault();
    const blockers = Array.from(rows.children, tr => ({item: tr.controls.item.value, pod_num: tr.controls.pod.value,
      required_qty: Number(tr.controls.required.value), uncovered_qty: Number(tr.controls.uncovered.value)}));
    const noBlockers = noBlockersInput.checked;
    if (noBlockers && blockers.length) return showError("Remove blocker rows before approving no material blockers.");
    if (!noBlockers && !blockers.length) return showError("Add blockers or explicitly approve no material blockers.");
    const assignNow = noBlockers && Boolean(productionInput);
    if (assignNow && !productionInput.value) return showError("Select a production start date before saving.");
    const itemRows = document.getElementById("approved-item-rows");
    save(event.target, "review", {so: context.so, fingerprint: context.fingerprint,
      review_id: context.review_id, submission_id: context.review_submission, blockers,
      no_blockers: noBlockers,
      ...(assignNow ? {production_start: productionInput.value} : {}),
      notes: document.getElementById("review-notes").value,
      order_content: {...context.order_versions, items: itemRows ? JSON.parse(itemRows.value || "[]") : []}});
  });
})();
