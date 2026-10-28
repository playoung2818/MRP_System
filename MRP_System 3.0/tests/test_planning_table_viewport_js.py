"""Actual remaining-height calculation, including resize, reflow, and scroll."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which('node') is None, reason='Node required for layout calculation test')
def test_table_height_fills_remaining_viewport_without_a_fixed_header_offset():
    script = Path(__file__).resolve().parents[2] / 'Webpage' / 'static' / 'lt-planning.js'
    harness = r'''
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
let top = 260, footerHeight = 30, height, observerCallback;
const events = {}, observed = [];
const workspace = {};
const footer = {getBoundingClientRect: () => ({height: footerHeight})};
const wrap = {nextElementSibling: footer, getBoundingClientRect: () => ({top}),
  style: {setProperty(name, value) {assert.equal(name, '--planning-table-height'); height = value;}}};
const card = {querySelector: () => wrap};
const window = {innerHeight: 1000, scrollY: 0,
  getComputedStyle(el) {return el === workspace ? {paddingBottom: '20px'} : {marginBottom: '0px', borderBottomWidth: '1px'};},
  requestAnimationFrame(callback) {callback();},
  addEventListener(name, callback) {events[name] = callback;},
  ResizeObserver: class {constructor(callback) {observerCallback = callback;} observe(el) {observed.push(el);}}};
const document = {
  getElementById(id) {return id === 'planning-csrf' ? {textContent: '"token"'} : id === 'workspace' ? workspace : null;},
  querySelectorAll(selector) {return selector === '.planning-orders-card' ? [card]
    : selector.includes('.brand-header') ? [{}, {}, {}] : [];}};
vm.runInNewContext(fs.readFileSync(process.argv[1], 'utf8'), {window, document});
assert.equal(height, '689px');
assert.equal(observed.length, 4);
window.innerHeight = 1200;
events.resize();
assert.equal(height, '889px');
window.scrollY = 120; top = 140;
events.resize();
assert.equal(height, '889px', 'Scrolling must not change the document-space table allocation');
top = 180;
observerCallback();
assert.equal(height, '849px', 'Header or alert reflow must reduce available space');
footerHeight = 60;
observerCallback();
assert.equal(height, '819px', 'Wrapped footer must be included');
window.innerHeight = 400;
events.resize();
assert.equal(height, '240px', 'Keep a usable minimum on small screens');
'''
    result = subprocess.run([shutil.which('node'), '-e', harness, str(script)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
