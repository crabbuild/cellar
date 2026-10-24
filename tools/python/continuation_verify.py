from pathlib import Path
import json,sys,hashlib,subprocess,tempfile,re,statistics
root=Path(sys.argv[1]).resolve()
sys.path.insert(0,str(Path.cwd()/'tools/python'))
from baseline import require,digest,save
from offered import summarize,read_targets
r=json.loads((root/'measurement.json').read_text());require(r['status']=='complete' and len(r['cases'])==2,'incomplete experiment')
for p,h in r['helpers'].items():require(digest(Path('tools/python')/p)==h,'helper changed')
require(digest(root/'host')==r['build']['sha256'],'binary identity')
for path,expected in r['build']['sources'].items():
 data=subprocess.check_output(['git','show',r['build']['head']+':'+path])
 require(hashlib.sha256(data).hexdigest()==expected,'committed source: '+path)
require(digest(root/'baseline_native.rs')==r['build']['sources']['tools/python/baseline_native.rs'],'fixture source')
require(digest(root/'client')==r['client_sha256'],'client binary')
placement=r['cpu_placement']
require(not set(placement['host']) & set(placement['client']) and set(placement['host']+placement['client'])<=set(placement['available']),'disjoint CPU placement')
arrivals=0;comparisons=[];populations=set()
def counts(before,after,variant,n):
 require(before['fixture']==after['fixture']=='CELLAR_NATIVE_SINGLE_SUSPENSION_V1','fixture marker')
 require(before['variant']==after['variant']==variant,'selected variant')
 require(after['reads']-before['reads']==n,'read count')
 amount=n if variant=='suspended' else 0
 require(after['yielded']-before['yielded']==after['resumed']-before['resumed']==amount,'suspension count')
for c in r['cases']:
 population=c['population'];folder=root/str(population);
 require(c['affinity']['requested']==c['affinity']['observed']==placement['host'],'host affinity')
 for side in ['threads_before','threads_after']:require(bool(c[side]) and all(mask==placement['host'] for mask in c[side].values()),'host thread affinity')
 require(population not in populations,'duplicate population');populations.add(population)
 require(c['primed']==[{'cell':i,'acknowledged':{'value':1}} for i in range(population)],'acknowledged population')
 require(c['status']=='complete' and c['exit']['returncode']==0 and not c['exit']['killed_by_cleanup'],'unjoined host')
 require(c['host']['mode']=='native-resident' and c['host']['durability']=='local-only' and not c['host']['debug_assertions'] and not c['host']['embed_compiled'],'native mode')
 stop=c['shutdown'];native=stop['native']
 require(stop['complete'] and stop['python'] is None and native['complete'] and all(native[k] for k in ['ingress_drained','application_work_drained','cells_drained','log_quiesced','local_durability_closed']) and not native['deadline_elapsed'] and not native['drain_timed_out'],'native drain')
 require([p['variant'] for p in c['preflight']]==['immediate','suspended','immediate'],'preflight order')
 for p in c['preflight']:counts(p['before'],p['after'],p['variant'],1)
 require(len(c['phases'])==18,'phase count');metrics={}
 reads=read_targets(population)
 for target in reads:target['expected_body']=list(b'{"value":1}')
 require(json.loads((folder/'reads.json').read_text())==reads,'read body')
 for p in c['phases']:
  key=(p['repetition'],p['rate'],p['variant']);require(key not in metrics,'duplicate phase');prefix=f"{p['variant']}-{p['repetition']}-{p['rate']}"
  for label,suffix,duration in [('warmup','-warmup',500),('measurement','',5000)]:
   raw=json.loads((folder/(prefix+suffix+'.json')).read_text())
   proof=json.loads((folder/(prefix+suffix+'.affinity.json')).read_text())
   require(proof['requested']==proof['observed']==placement['client'] and set(proof['observed'])<=set(proof['available_before']),'client affinity')
   require(raw['workload']==dict(url=c['host']['url'].rstrip('/')+'/',rate=p['rate'],duration_ms=duration,max_inflight=64,body_bytes=0,timeout_ms=1000,max_lag_ms=5,reads=reads),'raw workload')
   result=summarize(raw);require(result==p[label],'raw summary')
   require(set(result['outcomes'])<={'success','client_capacity','client_late','http_status'} and all(row['status']==503 for row in raw['samples'] if row['outcome']=='http_status'),'unexpected outcome');arrivals+=result['arrivals']
  for side in ['before','after']:require(p[side]['runtime']==dict(kind='native',bridge=None,interpreter=None),'native-only runtime')
  counts(p['selection'],p['counts_before'],p['variant'],p['warmup']['outcomes']['success'])
  counts(p['counts_before'],p['counts_after'],p['variant'],p['measurement']['outcomes']['success'])
  expected=dict(rps=p['measurement']['completed_in_window_per_second'],p99_ns=p['measurement']['successful_request_latency_ns']['p99'])
  require(expected==p['metrics'],'derived metrics');metrics[key]=expected
 require(set(metrics)=={(rep,rate,variant) for rep in range(3) for rate in [500,30000,60000] for variant in ['immediate','suspended']},'phase matrix')
 for rate in [500,30000,60000]:
  changes=[]
  for rep in range(3):
   a=metrics[(rep,rate,'immediate')];b=metrics[(rep,rate,'suspended')];changes.append({k:(b[k]/a[k]-1)*100 for k in a})
  row=dict(population=population,rate=rate,paired_changes=changes,median_paired_change={k:statistics.median(c[k] for c in changes) for k in changes[0]},medians={variant:{k:statistics.median(metrics[(rep,rate,variant)][k] for rep in range(3)) for k in ['rps','p99_ns']} for variant in ['immediate','suspended']})
  comparisons.append(row);print(row,flush=True)
require(populations=={8,64} and arrivals==5973000,'matrix/arrival count')
save(root/'independent-verification.json',dict(status='verified',arrivals=arrivals,joined_hosts=2,comparisons=comparisons))
print('Verified',arrivals,'arrivals, two joined native-only hosts, and exactly one suspension/resumption per suspended read.')
