from pathlib import Path
import sys,os,subprocess,json,hashlib,shutil
root=Path(sys.argv[1]).resolve();root.mkdir(parents=True,exist_ok=False)
assert not subprocess.check_output(['git','status','--porcelain'],text=True).strip(),'checkout must be clean'
head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()
assert head==os.environ['CELLAR_LOAD_SHA'],'unexpected diagnostic commit'
paths=['Cargo.lock','Cargo.toml','tools/python/baseline.rs','tools/python/baseline_native.rs','tools/python/baseline_host.rs','tools/perf/deployment.rs','tools/python/baseline.py','tools/python/offered.py','tools/python/residency.py','tools/python/affinity.py','tools/python/offered_load.rs','tools/python/continuation_build.py','tools/python/continuation_measure.py','tools/python/continuation_verify.py','.github/workflows/native-sdk.yml']
sources={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in paths}
command=['cargo','build','--locked','-p','cellar-python','--example','python-baseline','--example','python-offered-load','--no-default-features','--profile','lab']
with (root/'build.log').open('w') as log:result=subprocess.run(command,stdout=log,stderr=subprocess.STDOUT)
assert result.returncode==0,result.returncode
for src,dst in [('python-baseline','host'),('python-offered-load','client')]:shutil.copy2(Path('target/lab/examples')/src,root/dst)
(root/'build.json').write_text(json.dumps(dict(head=head,sources=sources,command=command,sha256=hashlib.sha256((root/'host').read_bytes()).hexdigest(),feature_mode='native without embed',source_mode='committed'),indent=2)+'\n')
(root/'client-build.json').write_text(json.dumps(dict(sha256=hashlib.sha256((root/'client').read_bytes()).hexdigest(),source_sha256=sources['tools/python/offered_load.rs']),indent=2)+'\n')
shutil.copy2('tools/python/baseline_native.rs',root/'baseline_native.rs')
print('Built native control and fixed client at',head,flush=True)
