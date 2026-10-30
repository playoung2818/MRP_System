"""Full-page table toggle, without replacing forms or navigating."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which('node') is None, reason='Node required for interaction test')
def test_expand_restore_escape_and_background_focus():
    script = Path(__file__).resolve().parents[2] / 'Webpage' / 'static' / 'lt-planning.js'
    harness = r'''
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
function classes() {
 const values=new Set();
 return {contains: value=>values.has(value), toggle(value,enabled) {
  if(enabled)values.add(value);else values.delete(value);
 }};
}
const buttonEvents={}, documentEvents={}, attributes={};
let focused=false;
const label={textContent:'Expand table'};
const button={get textContent(){return label.textContent;},querySelector:()=>label,
 setAttribute:(k,v)=>{attributes[k]=v;},
 addEventListener:(name,fn)=>{buttonEvents[name]=fn;},focus:()=>{focused=true;}};
const wrap={nextElementSibling:null,style:{setProperty(){}},getBoundingClientRect:()=>({top:200})};
const form={date:'2026-11-03',notes:'Keep my unsaved notes'};
const card={classList:classes(),querySelector:()=>wrap,form};
const backgrounds=[{inert:false},{inert:true}];
const document={body:{classList:classes()},
 getElementById(id){return id==='planning-csrf'?{textContent:'"token"'}:
  id==='planning-expand-table'?button:id==='workspace'?{}:null;},
 querySelectorAll(selector){return selector==='.planning-orders-card'?[card]:
  selector.startsWith('.app-sidebar,')?backgrounds:[];},
 addEventListener:(name,fn)=>{documentEvents[name]=fn;}};
const window={innerHeight:1000,scrollY:0,getComputedStyle:()=>({}),
 requestAnimationFrame:fn=>fn(),addEventListener(){}};
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'),{window,document});
buttonEvents.click();
assert.equal(card.classList.contains('is-expanded'),true);
assert.equal(document.body.classList.contains('planning-table-expanded'),true);
assert.equal(attributes['aria-expanded'],'true');
assert.equal(button.textContent,'Exit full page');
assert.equal(attributes['aria-label'],'Exit full page');
assert.equal(attributes.title,'Exit full page');
assert.equal(backgrounds[0].inert,true);
buttonEvents.click();
assert.equal(card.classList.contains('is-expanded'),false);
assert.equal(attributes['aria-expanded'],'false');
assert.equal(button.textContent,'Expand table');
assert.equal(attributes['aria-label'],'Expand table');
assert.equal(attributes.title,'Expand table');
assert.equal(backgrounds[0].inert,false);
assert.equal(backgrounds[1].inert,true,'Restore pre-existing inert state');
buttonEvents.click();
documentEvents.keydown({key:'Escape',isComposing:true});
assert.equal(card.classList.contains('is-expanded'),true);
documentEvents.keydown({key:'Escape',isComposing:false});
assert.equal(card.classList.contains('is-expanded'),false);
assert.equal(document.body.classList.contains('planning-table-expanded'),false);
assert.equal(focused,true);
assert.equal(card.form,form,'Do not replace forms');
assert.equal(form.notes,'Keep my unsaved notes');
assert.equal(form.date,'2026-11-03');
'''
    result = subprocess.run([shutil.which('node'), '-e', harness, str(script)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
