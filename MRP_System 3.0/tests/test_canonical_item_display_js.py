"""Canonical labels must not silently change saved item identifiers on submission."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which('node') is None, reason='Node required for payload test')
def test_display_spelling_preserves_source_values_and_accepts_explicit_manual_edits():
    script = Path(__file__).resolve().parents[2] / 'Webpage' / 'static' / 'so-review.js'
    harness = r'''
const assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
function row(id) {
 const item={value:'i7-9700E',dataset:{originalItem:'I7-9700E',displayItem:'i7-9700E'}};
 const fields={'.review-item':item,'.review-quantity':{value:'1'},
  '.review-configuration':{value:''},'.review-site':{value:'WH01S-NTA'},'.approval-number':{}};
 return {dataset:{itemId:id},querySelector:selector=>fields[selector],item};
}
const source=row('source-line'),manual=row('manual-1');
const elements={'approved-items':{children:[source,manual],addEventListener(){}},
 'approved-item-order':{value:''},'approved-item-rows':{value:''},
 'restore-sheet-order':{addEventListener(){}},
 'review-form':{handlers:{},addEventListener(name,fn){this.handlers[name]=fn;}}};
const document={getElementById:id=>elements[id]||null};
const window={addEventListener(){}};
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'),{window,document});
let payload=JSON.parse(elements['approved-item-rows'].value);
assert.equal(source.item.value,'i7-9700E');
assert.equal(payload[0].item,'I7-9700E');
assert.equal(payload[1].item,'I7-9700E');
manual.item.value='Explicit-New-Item';
elements['review-form'].handlers.submit({});
payload=JSON.parse(elements['approved-item-rows'].value);
assert.equal(payload[0].item,'I7-9700E');
assert.equal(payload[1].item,'Explicit-New-Item');
'''
    result = subprocess.run([shutil.which('node'), '-e', harness, str(script)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
