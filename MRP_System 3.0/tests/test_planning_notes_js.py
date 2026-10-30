"""Single Save button: blank date saves notes; entered date assigns production."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which('node') is None, reason='Node required for interaction test')
def test_inline_save_selects_action_and_preserves_in_progress_edits():
    script = Path(__file__).resolve().parents[2] / 'Webpage' / 'static' / 'lt-planning.js'
    harness = r'''
const assert=require('node:assert/strict'), vm=require('node:vm'), fs=require('node:fs');
const source=fs.readFileSync(process.argv[1],'utf8');
function setup({date='',fail=false,delayed=false,confirm=true}={}) {
 const events={},calls=[],redirects=[],warnings=[];
 const button={disabled:false},status={textContent:''};
 const error={textContent:'',scrollIntoView(){}};
 const form={dataset:{context:JSON.stringify({so:'SO-1',review_id:'review',submission_id:'first',
  source_revision:1,revision:0,notes_version:''})},
  elements:{production_start:{value:date},notes:{value:'Follow up'}},
  addEventListener:(name,fn)=>{events[name]=fn;},
  querySelector:selector=>selector==='button[type="submit"]'?button:status};
 let release;
 const context={URL,document:{
  getElementById:id=>id==='planning-csrf'?{textContent:'"token"'}:id==='save-message'?error:null,
  querySelectorAll:selector=>selector==='.inline-plan-form'?[form]:[]},
  window:{confirm:message=>{warnings.push(message);return confirm;},location:{href:'http://192.168.60.215:5002/planning?status=needs_production_planning',
   assign:url=>redirects.push(url)}},
  fetch:async(url,options)=>{
   const payload=JSON.parse(options.body);calls.push({url,payload,options});
   if(delayed)await new Promise(resolve=>{release=resolve;});
   return {ok:!fail,headers:{get:()=> 'application/json'},json:async()=>fail?
    {error:'Notes changed in another tab'}:
    {notes:payload.notes.trim(),notes_version:'saved-version',submission_id:'next-submission',next_status:'needs_material_review'}};
  }};
 vm.runInNewContext(source,context);
 return {form,button,status,error,calls,redirects,events,warnings,
  submit:()=>events.submit({preventDefault(){}}),release:()=>release()};
}
const flush=()=>new Promise(setImmediate);
(async()=>{
 const notes=setup();notes.submit();await flush();
 assert.equal(notes.calls[0].url,'/lt_planning/notes');
 assert.equal(notes.warnings.length,1);
 assert.ok(notes.warnings[0].includes('Save notes only?'));
 assert.equal(notes.calls[0].options.headers['X-CSRF-Token'],'token');
 assert.equal(notes.calls[0].payload.production_start,'');
 assert.equal(notes.redirects.length,0);
 assert.equal(notes.button.disabled,false);
 assert.equal(notes.status.textContent,'Notes saved · date not assigned');
 assert.equal(JSON.parse(notes.form.dataset.context).notes_version,'saved-version');
 notes.form.elements.notes.value='Updated';notes.events.input();
 assert.equal(notes.status.textContent,'Unsaved changes');
 notes.submit();await flush();
 assert.equal(notes.calls[1].payload.submission_id,'next-submission');
 assert.equal(notes.calls[1].payload.notes_version,'saved-version');
 const assign=setup({date:'2026-11-03'});assign.submit();await flush();
 assert.equal(assign.calls[0].url,'/lt_planning/assign');
 assert.equal(assign.warnings.length,0);
 assert.equal(assign.calls[0].payload.production_start,'2026-11-03');
 assert.equal(assign.redirects.length,1);
 const cancelled=setup({confirm:false});cancelled.submit();await flush();
 assert.equal(cancelled.warnings.length,1);
 assert.equal(cancelled.calls.length,0);
 assert.equal(cancelled.button.disabled,false);
 assert.equal(cancelled.form.elements.notes.value,'Follow up');
 const failed=setup({fail:true});failed.submit();await flush();
 assert.equal(failed.redirects.length,0);assert.equal(failed.button.disabled,false);
 assert.equal(failed.form.elements.notes.value,'Follow up');
 assert.equal(failed.status.textContent,'Not saved');
 assert.equal(failed.error.textContent,'Notes changed in another tab');
 assert.equal(JSON.parse(failed.form.dataset.context).submission_id,'first');
 const editing=setup({delayed:true});editing.submit();
 editing.form.elements.notes.value='New unsaved text';editing.release();await flush();
 assert.equal(editing.form.elements.notes.value,'New unsaved text');
 assert.equal(editing.status.textContent,'Earlier notes saved · unsaved changes');
 assert.equal(editing.redirects.length,0);
})().catch(e=>{console.error(e);process.exitCode=1;});
'''
    result = subprocess.run([shutil.which('node'), '-e', harness, str(script)],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stdout + result.stderr
