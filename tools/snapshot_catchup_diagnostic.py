import json, subprocess, time
from pathlib import Path
root=Path('/tmp/cellar-snapshot-diagnostic')
root.mkdir()
source=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
(root/'source.json').write_text(json.dumps({'source':source,'parent':subprocess.check_output(['git','rev-parse','HEAD^'],text=True).strip()},indent=2))
with (root/'build.jsonl').open('w') as out, (root/'build.log').open('w') as err:
    build=subprocess.run(['cargo','test','--locked','-p','cellar','--all-features','--test','native_raft_snapshot','--no-run','--message-format=json'],stdout=out,stderr=err)
if build.returncode:raise SystemExit(build.returncode)
binaries=[]
for line in (root/'build.jsonl').read_text().splitlines():
    item=json.loads(line)
    if item.get('reason')=='compiler-artifact' and item.get('executable') and item['target']['name']=='native_raft_snapshot':binaries.append(item['executable'])
assert len(binaries)==1,binaries
binary=binaries[0]
results=[]
# Retain the serial full-suite context of the original CI failure first.
for i in range(201):
    test=[] if i==0 else ['native_snapshot_catchup_resumes_follower_reads','--exact']
    if i==1:
        listing=subprocess.check_output([binary,*test,'--list'],text=True)
        assert '1 test, 0 benchmarks' in listing,listing
    start=time.monotonic()
    with (root/f'run-{i:03}.log').open('w') as log:
        result=subprocess.run([binary,*test,'--nocapture','--test-threads=1'],stdout=log,stderr=subprocess.STDOUT)
    entry={'run':i,'full_suite':i==0,'code':result.returncode,'seconds':time.monotonic()-start}
    results.append(entry)
    (root/'results.json').write_text(json.dumps(results,indent=2))
    print(entry,flush=True)
    if result.returncode:raise SystemExit(result.returncode)
