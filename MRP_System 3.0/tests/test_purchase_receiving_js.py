"""Pure scanner-mode/serialization regressions; browser DOM checked separately."""
from pathlib import Path
import shutil
import subprocess
import pytest


@pytest.mark.skipif(shutil.which('node') is None,reason='Node required')
def test_serial_batch_modes_and_database_values():
    script=Path(__file__).resolve().parents[2]/'Webpage'/'static'/'purchase-receiving.js'
    harness=r'''
const assert=require('node:assert/strict'),vm=require('node:vm'),fs=require('node:fs');
const context={window:{},document:{getElementById:()=>null}};
vm.runInNewContext(fs.readFileSync(process.argv[1],'utf8'),context);
const {mode,combine,nextScanTarget}=context.window.PurchaseReceiving;
assert.equal(mode('i7-14700').cpu,true);
assert.equal(mode(' CORE Ultra ').cpu,true);
assert.equal(mode('Dust filter').dust,true);
assert.equal(mode('DUST-i7').cpu,false);
assert.equal(mode('RAM').cpu,false);
assert.equal(combine(' SN0001 ',' BATCH100 ',true),'SN0001_BATCH100');
assert.equal(combine(' SN0001 ',' BATCH100 ',false),'SN0001');
assert.equal(combine('NA','BATCH100',true),'NA');
assert.equal(combine('','BATCH100',true),'');
assert.equal(JSON.stringify(nextScanTarget(true,true,0,10)),JSON.stringify({index:0,field:'batch'}));
assert.equal(JSON.stringify(nextScanTarget(true,false,0,10)),JSON.stringify({index:1,field:'serial'}));
assert.equal(JSON.stringify(nextScanTarget(true,true,9,10)),JSON.stringify({index:9,field:'batch'}));
assert.equal(nextScanTarget(true,false,9,10),null);
assert.equal(JSON.stringify(nextScanTarget(false,true,0,10)),JSON.stringify({index:1,field:'serial'}));
assert.equal(fs.readFileSync(process.argv[1],'utf8').includes('Apply batch to all'),false);
'''
    subprocess.run(['node','-e',harness,str(script)],check=True,capture_output=True,text=True)
