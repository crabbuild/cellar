from pathlib import Path
from types import SimpleNamespace
import json,sys,os
root=Path(sys.argv[1]).resolve()
sys.path.insert(0,str(Path.cwd()/'tools/python'))
from baseline import Host,Connection,require,save,digest
from offered import measure,observed_idle,read_targets
from residency import populate
import affinity
allowed=affinity.available()
require(len(allowed)>=2,'two eligible CPUs required')
placement=affinity.placement(str(allowed[0]),','.join(map(str,allowed[1:])))
build=json.loads((root/'build.json').read_text());client=root/'client'
cb=json.loads((client.parent/'client-build.json').read_text())
require(digest(client)==cb['sha256'] and digest(Path('tools/python/offered_load.rs'))==cb['source_sha256'],'client identity')
require(digest(root/'host')==build['sha256'],'host identity')
report=dict(cpu_placement=placement,status='running',scope='Same native binary and live host per population; alternate immediate and exactly one Pending/wake/Ready cell read. Disjoint Linux host/client CPU sets, local-only cells, no Python embedding or production capacity claim.',build=build,client_sha256=digest(client),helpers={p:digest(Path('tools/python')/p) for p in ['baseline.py','offered.py','residency.py']},cases=[])
args=SimpleNamespace(cpu_placement=placement,cell_echo=False,max_inflight=64,body_bytes=0,timeout_ms=1000,max_lag_ms=5,build_profile='lab')
def control(host,path):
 connection=Connection(host.origin)
 try:
  row=json.loads(connection.request('GET',path));require(row['fixture']=='CELLAR_NATIVE_SINGLE_SUSPENSION_V1','fixture marker');return row
 finally:connection.close()
try:
 for population in [8,64]:
  folder=root/str(population);folder.mkdir();host=Host(root/'host','native-resident',folder,'primary',command_prefix=affinity.prefix(placement['host'],folder/'host-affinity.json'))
  case=dict(population=population,status='running',phases=[]);report['cases'].append(case)
  try:
   case['startup_ns']=host.readiness();case['host']=host.ready
   case['affinity']=affinity.verify(folder/'host-affinity.json',placement['host'],host.process.pid)
   case['threads_before']=affinity.verify_threads(host.process.pid,placement['host'])
   require(case['host']['durability']=='local-only' and not case['host']['debug_assertions'] and not case['host']['embed_compiled'],'host policy')
   populate(host,0,population);con=Connection(host.origin);case['primed']=[]
   try:
    for index in range(population):
     answer=json.loads(con.request('POST',f'/cells/resident-{index}'))
     require(answer=={'value':1},'populated value acknowledgement');case['primed'].append({'cell':index,'acknowledged':answer})
   finally:con.close()
   reads=read_targets(population)
   for target in reads:target['expected_body']=list(b'{"value":1}')
   save(folder/'reads.json',reads)
   case['preflight']=[]
   for variant in ['immediate','suspended','immediate']:
    observed_idle(host,population);before=control(host,'/continuation-control/'+variant)
    require(before['variant']==variant,'variant selection');con=Connection(host.origin)
    try:require(json.loads(con.request('GET','/cells/resident-0'))=={'value':1},'control read')
    finally:con.close()
    observed_idle(host,population);after=control(host,'/continuation-observations')
    require(after['reads']-before['reads']==1 and after['yielded']-before['yielded']==int(variant=='suspended') and after['resumed']-before['resumed']==int(variant=='suspended'),'one suspension preflight')
    case['preflight'].append(dict(variant=variant,before=before,after=after))
   for rep in range(3):
    for rate in [500,30000,60000]:
     for variant in (['immediate','suspended'] if rep%2==0 else ['suspended','immediate']):
      observed_idle(host,population);selected=control(host,'/continuation-control/'+variant)
      require(selected['variant']==variant,'variant selection')
      phase=dict(repetition=rep,rate=rate,variant=variant,selection=selected,load_before=list(os.getloadavg()));case['phases'].append(phase)
      prefix=f'{variant}-{rep}-{rate}'
      phase['warmup']=measure(client,host,args,rate,500,folder/(prefix+'-warmup.json'),reads)
      phase['before']=observed_idle(host,population);phase['counts_before']=control(host,'/continuation-observations')
      phase['measurement']=measure(client,host,args,rate,5000,folder/(prefix+'.json'),reads)
      phase['after']=observed_idle(host,population);phase['counts_after']=control(host,'/continuation-observations')
      before=phase['counts_before'];after=phase['counts_after'];result=phase['measurement'];n=result['outcomes']['success']
      require(before['variant']==after['variant']==variant and after['reads']-before['reads']==n,'read accounting')
      count=n if variant=='suspended' else 0
      require(after['yielded']-before['yielded']==after['resumed']-before['resumed']==count,'exactly one suspension per success')
      require(set(result['outcomes'])<={'success','client_capacity','client_late','http_status'},'unexpected failures')
      raw=json.loads((folder/(prefix+'.json')).read_text());require(all(row['status']==503 for row in raw['samples'] if row['outcome']=='http_status'),'unexpected HTTP status')
      phase['metrics']=dict(rps=result['completed_in_window_per_second'],p99_ns=result['successful_request_latency_ns']['p99'])
      print(population,rep,rate,variant,phase['metrics'],flush=True);save(root/'measurement.json',report)
   case['threads_after']=affinity.verify_threads(host.process.pid,placement['host']);case['shutdown']=host.stop();case['status']='complete'
  finally:
   host.cleanup();case['exit']=json.loads((folder/'primary-exit.json').read_text());save(root/'measurement.json',report)
  require(case['exit']['returncode']==0 and not case['exit']['killed_by_cleanup'],'incomplete join')
 report['status']='complete'
except BaseException as error:
 report.update(status='failed',failure=f'{type(error).__name__}: {error}');raise
finally:save(root/'measurement.json',report)
