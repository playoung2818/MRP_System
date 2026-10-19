"use strict";
document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("source-review-form");
  if (!form) return;
  form.addEventListener("submit", async event => {
    event.preventDefault();
    const button = form.querySelector("button");
    const message = document.getElementById("save-message");
    button.disabled = true;
    try {
      const response = await fetch("/lt_planning/acknowledge_source", {
        method: "POST",
        headers: {"Content-Type": "application/json", "X-CSRF-Token": JSON.parse(document.getElementById("planning-csrf").textContent)},
        body: form.dataset.context
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || "Approval failed.");
      window.location.reload();
    } catch (error) {
      message.textContent = error.message || "Network error. Reload and review before retrying.";
      message.className = "alert alert-danger";
      button.disabled = false;
    }
  });
});
