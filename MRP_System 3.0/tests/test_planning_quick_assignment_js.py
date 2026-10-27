"""Exercise the browser checkbox/date/payload behavior without a browser or DB."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which('node') is None, reason='Node is needed for the DOM smoke test')
def test_no_blockers_toggles_immediate_assignment_and_preserves_revision_behavior():
    script = Path(__file__).resolve().parents[2] / 'Webpage' / 'static' / 'lt-planning.js'
    harness = r'''
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(process.argv[1], 'utf8');
function setup(returned = false) {
  const elements = {};
  function el(id, extra = {}) {
    return elements[id] = {textContent: '', value: '', children: [], handlers: {},
      addEventListener(name, callback) { this.handlers[name] = callback; },
      scrollIntoView() {}, ...extra};
  }
  el('planning-csrf', {textContent: JSON.stringify('token')});
  el('planning-context', {textContent: JSON.stringify({so: 'SO-1', suggestions: [],
    fingerprint: 'current', review_id: '', review_submission: 'submission', order_versions: {source_revision: 1}})});
  el('blocker-rows'); el('add-blocker'); el('save-message');
  el('no-blockers', {checked: false});
  el('review-notes', {value: 'Ready'});
  el('approved-item-rows', {value: '[]'});
  el('review-submit', {textContent: returned ? 'Approve Revision & Return to MES' : 'Save SO & Material Review'});
  el('review-save-hint', {textContent: 'Original hint'});
  el('review-form', {querySelector() { return elements['review-submit']; }});
  if (!returned) {
    el('review-production-panel', {hidden: true});
    el('review-production-start', {disabled: true, required: false});
  }
  const calls = [], redirects = [];
  const context = {URL, console, document: {getElementById(id) {return elements[id] || null;},
      querySelectorAll() {return [];}},
    window: {location: {href: 'http://192.168.60.215:5002/planning?so=SO-1&q=Customer&reload=1',
      assign(url) {redirects.push(url);}}},
    fetch: async (url, options) => {
      calls.push({url, options, payload: JSON.parse(options.body)});
      return {ok: true, headers: {get() {return 'application/json';}},
        async json() {return {next_status: 'needs_production_planning'};}};
    }};
  vm.runInNewContext(source, context);
  return {elements, calls, redirects, toggle(checked) {
      elements['no-blockers'].checked = checked;
      elements['no-blockers'].handlers.change();
    }, submit() {elements['review-form'].handlers.submit({preventDefault() {}, target: elements['review-form']});}};
}
const initial = setup();
const e = initial.elements;
assert.equal(e['review-production-panel'].hidden, true);
assert.equal(e['review-production-start'].disabled, true);
initial.toggle(true);
assert.equal(e['review-production-panel'].hidden, false);
assert.equal(e['review-production-start'].required, true);
assert.equal(e['review-production-start'].disabled, false);
assert.equal(e['review-submit'].textContent, 'Save Review & Assign Production Date');
initial.submit();
assert.equal(initial.calls.length, 0);
assert.match(e['save-message'].textContent, /Select a production start date/);
e['review-production-start'].value = '2026-07-10';
initial.submit();
assert.equal(initial.calls[0].url, '/lt_planning/review');
assert.equal(initial.calls[0].payload.production_start, '2026-07-10');
assert.equal(initial.calls[0].payload.no_blockers, true);
assert.equal(initial.calls[0].options.headers['X-CSRF-Token'], 'token');
initial.toggle(false);
assert.equal(e['review-production-panel'].hidden, true);
assert.equal(e['review-production-start'].required, false);
assert.equal(e['review-production-start'].disabled, true);
assert.equal(e['review-submit'].textContent, 'Save SO & Material Review');
e['blocker-rows'].children.push({controls: {item: {value: 'PART-A'}, pod: {value: ''},
  required: {value: '10'}, uncovered: {value: '8'}}});
initial.submit();
assert.equal(initial.calls.length, 2);
assert.equal('production_start' in initial.calls[1].payload, false);
assert.equal(initial.calls[1].payload.no_blockers, false);
const revision = setup(true);
revision.toggle(true);
assert.equal(revision.elements['review-submit'].textContent, 'Approve Revision & Return to MES');
revision.submit();
assert.equal(revision.calls.length, 1);
assert.equal('production_start' in revision.calls[0].payload, false);
(async () => {
  // Let fetch/json promises resolve before checking successful navigation.
  await new Promise(resolve => setImmediate(resolve));
  for (const page of [initial, revision]) {
    assert.ok(page.redirects.length > 0);
    for (const target of page.redirects) {
      const url = new URL(target);
      assert.equal(url.searchParams.get('status'), 'needs_material_review');
      assert.equal(url.searchParams.has('so'), false);
      assert.equal(url.searchParams.has('q'), false);
      assert.equal(url.searchParams.has('reload'), false);
    }
  }
})().catch(error => {console.error(error); process.exitCode = 1;});
'''
    result = subprocess.run([shutil.which('node'), '-e', harness, str(script)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
