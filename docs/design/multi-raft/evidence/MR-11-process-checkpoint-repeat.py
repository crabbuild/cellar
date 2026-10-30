import pathlib,subprocess,os,sys,time
exe=pathlib.Path.home()/'Workspace/crabbuild-target/cellar-23c7-multi-raft/debug/deps/native_raft_snapshot-b7cf6f33b7a4b9fe'
args=[str(exe),'checkpoint_completion_loss_still_retains_durable_image','checkpoint_failed_sync_preserves_prospective_recovery_image','--exact','--nocapture','--test-threads=1']
for attempt in range(1,1001):
 out=pathlib.Path(f'/tmp/cellar-mr11-checkpoint-repeat-{attempt}.log')
 try:
  with out.open('wb') as log:r=subprocess.run(args,stdout=log,stderr=subprocess.STDOUT,timeout=10)
 except subprocess.TimeoutExpired:
  print('TIMEOUT',attempt,str(out),flush=True);sys.exit(124)
 if r.returncode:print('FAILED',attempt,r.returncode,str(out),flush=True);sys.exit(r.returncode)
 if attempt%10==0:print('PASSED',attempt,flush=True)
