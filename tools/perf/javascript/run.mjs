// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Closed-loop local HTTP capacity probe. Uses no credentials or live fleet data.
import {spawn, execFileSync} from 'node:child_process';
import {createHash, randomUUID} from 'node:crypto';
import {createReadStream, createWriteStream} from 'node:fs';
import {mkdir, readFile, writeFile} from 'node:fs/promises';
import {request, Agent} from 'node:http';
import {resolve, dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {cpus, totalmem, platform, arch, release, loadavg} from 'node:os';
import {setTimeout as delay} from 'node:timers/promises';
import assert from 'node:assert/strict';

const root = fileURLToPath(new URL('../../../', import.meta.url));
const host = resolve(process.env.CELLAR_PERF_HOST ?? `${process.env.CARGO_TARGET_DIR}/release/cellar-javascript-perf`);
const application = resolve(process.env.CELLAR_PERF_APP ?? `${root}/.cellar/javascript-perf-app`);
const output = resolve(process.env.CELLAR_PERF_OUTPUT ?? `${root}/.cellar/javascript-perf-${Date.now()}`);
const seconds = Number(process.env.CELLAR_PERF_SECONDS ?? 5);
const warmup = Number(process.env.CELLAR_PERF_WARMUP ?? 1);
const repeats = Number(process.env.CELLAR_PERF_REPEATS ?? 3);
const concurrency = (process.env.CELLAR_PERF_CONCURRENCY ?? '1,8,32').split(',').map(Number);
const backends = (process.env.CELLAR_PERF_BACKENDS ?? 'v8,node').split(',');
assert(seconds > 0 && seconds <= 120 && warmup >= 0 && warmup <= 30);
assert(Number.isInteger(repeats) && repeats > 0 && repeats <= 10);
assert(concurrency.every(c => Number.isInteger(c) && c > 0 && c <= 64));
assert(backends.every(b => ['v8','node'].includes(b)));
await mkdir(output, {recursive:false});
const hash = async path => {
  const h = createHash('sha256');
  for await (const chunk of createReadStream(path)) h.update(chunk);
  return h.digest('hex');
};
const source_hashes = {};
for (const file of ['host.rs','app.ts','build.mjs','run.mjs']) source_hashes[file] = await hash(`${root}/tools/perf/javascript/${file}`);
const report = {
  source_hashes,
  format:1, started:new Date().toISOString(), source:execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim(),
  dirty:execFileSync('git',['status','--porcelain'],{cwd:root,encoding:'utf8'}),
  host_sha256:await hash(host), application_sha256:{v8:await hash(`${application}/v8.js`),node:await hash(`${application}/node.mjs`)},
  machine:{cpu:cpus()[0].model, logical_cpus:cpus().length, memory_bytes:totalmem(), platform:platform(), arch:arch(), os:release()},
  node:process.version, rust:execFileSync('rustc',['--version'],{encoding:'utf8'}).trim(),
  config:{seconds,warmup,repeats,concurrency,backends,rust_profile:'release',tokio_threads:2,bridge_options:'defaults',
    durability:'bucket',store:'private local SQLite development bucket',arrival:'closed-loop; one outstanding request per client',
    latency:'nearest-rank percentiles; successful responses; includes response body validation; no coordinated-omission correction',
    throughput:'successful completed requests / wall time including final drain; MiB/s counts request and response body bytes only'},
  trials:[], recovery:[],
};
await writeFile(`${output}/report.json`, JSON.stringify(report,null,2));
const log = createWriteStream(`${output}/trials.jsonl`);
const agent = new Agent({keepAlive:true,maxSockets:64});
let active;
async function http(url, method='GET', body=Buffer.alloc(0)) {
  return new Promise((resolve,reject) => {
    const req = request(url,{method,agent,headers:{'content-length':body.length}},res=>{
      const chunks=[];
      res.on('data', chunk=>chunks.push(chunk));
      res.on('error',reject);
      res.on('end',()=>resolve({status:res.statusCode,body:Buffer.concat(chunks)}));
    });
    req.setTimeout(15000,()=>req.destroy(new Error('request timed out')));
    req.on('error',reject);
    req.end(body);
  });
}
async function start(backend, generation) {
  const data = resolve(output,backend,generation);
  await mkdir(data,{recursive:true});
  const stdout = createWriteStream(`${data}/stdout.log`);
  const stderr = createWriteStream(`${data}/stderr.log`);
  // Deliberately do not inherit credentials or fleet configuration.
  const child = spawn(host,['--listen','127.0.0.1:0','--internal-listen','127.0.0.1:0'],{
    env:{PATH:process.env.PATH,HOME:process.env.HOME,
      CELLAR_PERF_BACKEND:backend,CELLAR_PERF_ENTRY:`${application}/${backend === 'v8' ? 'v8.js' : 'node.mjs'}`,
      CELLAR_NODE_EXECUTABLE:process.execPath,CELLAR_NODE:`perf-${randomUUID()}`,
      CELLAR_BUCKET:'cellar-dev',CELLAR_INTERNAL_DEV_STORE:resolve(output,backend,'bucket.sqlite'),
      CELLAR_TEST_DATA_DIR:resolve(data,'node'),CELLAR_DURABILITY:'bucket',CELLAR_TOKIO_THREADS:'2',
      CELLAR_REBALANCE_INTERVAL_MS:'0',CELLAR_PLACEMENT_WEIGHT:'1',CELLAR_SHUTDOWN_TOTAL_MS:'8000',RUST_LOG:'warn'},
    stdio:['ignore','pipe','pipe'],
  });
  let text='',failure;
  child.stdout.pipe(stdout);child.stderr.pipe(stderr);
  child.stdout.on('data',b=>{text=(text+b).slice(-32000);});
  child.on('error',e=>{failure=e;});
  const closed = new Promise(resolve=>child.once('close',code=>{stdout.end();stderr.end();resolve(code);}));
  active = {child,closed};
  for (let n=0;n<300;n++) {
    if(failure) throw failure;
    if(child.exitCode !== null) throw new Error(`Host exited: ${data}`);
    const address=/cellar listening on (\S+)/.exec(text)?.[1];
    const internal=/cellar internal listening on (\S+)/.exec(text)?.[1];
    if(address && internal) {
      try { if((await http(`http://${address}/.well-known/cellar/health`)).status === 200) return {...active,url:`http://${address}`,internal:`http://${internal}`}; } catch {}
    }
    await delay(100);
  }
  throw new Error(`Host not ready: ${data}`);
}
async function stop(server) {
  assert.equal((await http(`${server.internal}/shutdown`,'POST')).status,200);
  const code=await Promise.race([server.closed,delay(15000,Symbol.for('timeout'),{ref:false})]);
  assert.equal(code,0,'host must drain and exit successfully');
  active=undefined;
}
function resources(pid) {
  const rows=execFileSync('ps',['-axo','pid=,ppid=,rss=,time='],{encoding:'utf8'}).trim().split('\n').map(line=>{
    const [p,pp,rss,time]=line.trim().split(/\s+/);
    const parts=time.split(':').map(Number);
    const cpu=parts.reduce((n,v)=>n*60+v,0);
    return {pid:+p,ppid:+pp,rss:+rss,cpu};
  });
  const selected=new Set([pid]);
  for(let i=0;i<4;i++) for(const row of rows) if(selected.has(row.ppid)) selected.add(row.pid);
  const owned=rows.filter(row=>selected.has(row.pid));
  return {rss_kib:owned.reduce((n,r)=>n+r.rss,0),cpu_s:owned.reduce((n,r)=>n+r.cpu,0),pids:owned.map(r=>r.pid)};
}
const specs = [
  {name:'echo-1k',size:1024,method:'POST'},
  {name:'echo-64k',size:65536,method:'POST'},
  {name:'read',size:0,method:'GET'},
  {name:'write-sharded',size:1,method:'POST'},
  {name:'write-hot',size:1,method:'POST',hot:true},
];
const requested=(process.env.CELLAR_PERF_WORKLOADS ?? specs.map(s=>s.name).join(',')).split(',');
assert(requested.every(name=>specs.some(s=>s.name===name)));
report.config.workloads=requested;
async function phase(server,spec,c,duration,ledger) {
  const payload=Buffer.alloc(spec.size,120), latencies=[],errors={},error_samples=[];
  let count=0,bytes=0,attempted=0,maxRSS=0;
  const before=resources(server.child.pid), clientCPU=process.cpuUsage(), systemLoad=loadavg();
  const start=performance.now(), deadline=start+duration*1000;
  const monitor=setInterval(()=>{try{maxRSS=Math.max(maxRSS,resources(server.child.pid).rss_kib);}catch{}},1000);
  const workers=Array.from({length:c},async(_,i)=>{
    const cell=spec.hot ? 'hot' : `cell-${i}`;
    const url=`${server.url}/${spec.name.startsWith('echo') ? 'echo' : cell}`;
    while(performance.now()<deadline && server.child.exitCode === null && server.child.signalCode === null) {
      attempted++;
      const sent=performance.now();
      try {
        const reply=await http(url,spec.method,payload);
        if(reply.status !== 200) throw new Error(`HTTP ${reply.status}: ${reply.body.toString().slice(0,240)}`);
        if(spec.name.startsWith('echo')) assert(reply.body.equals(payload),'echo payload mismatch');
        else {
          const value=JSON.parse(reply.body).count;
          assert(Number.isSafeInteger(value) && value>=0,'invalid counter');
          if(spec.method === 'POST') ledger.set(cell,(ledger.get(cell) ?? 0)+1);
        }
        latencies.push(performance.now()-sent);count++;bytes+=payload.length+reply.body.length;
      } catch(error) {
        const message=String(error.message);
        errors[message]=(errors[message] ?? 0)+1;
        if(error_samples.length<3) error_samples.push(message);
      }
    }
  });
  try {await Promise.all(workers);} finally {clearInterval(monitor);}
  const elapsed=(performance.now()-start)/1000;
  const after=resources(server.child.pid),cpu=process.cpuUsage(clientCPU);
  latencies.sort((a,b)=>a-b);
  const percentile=q=>latencies.length ? latencies[Math.max(0,Math.ceil(q*latencies.length)-1)] : null;
  return {host_exited:server.child.exitCode !== null || server.child.signalCode !== null,system_load_start:systemLoad,system_load_end:loadavg(),attempted,completed:count,failed:attempted-count,errors,error_samples,elapsed_s:elapsed,rps:count/elapsed,
    body_mib_s:bytes/elapsed/1048576,latency_ms:{p50:percentile(.5),p95:percentile(.95),p99:percentile(.99),max:latencies.at(-1)??null},
    server_cpu_cores:after.pids.length ? Math.max(0,after.cpu_s-before.cpu_s)/elapsed : null,client_cpu_cores:(cpu.user+cpu.system)/1e6/elapsed,
    server_peak_rss_mib:Math.max(maxRSS,before.rss_kib,after.rss_kib)/1024,
    latency_samples_ms:latencies};
}
async function reconcile(server,ledger) {
  const counts={};
  for(const [cell,expected] of ledger) {
    const reply=await http(`${server.url}/${cell}`);
    assert.equal(reply.status,200);
    const actual=JSON.parse(reply.body).count;
    counts[cell]={expected,actual};
    assert.equal(actual,expected,`counter mismatch ${cell}: acknowledged writes vs stored effects`);
  }
  return counts;
}
try {
  for(const backend of backends) {
    const ledger=new Map(Array.from({length:Math.max(...concurrency)},(_,i)=>[`cell-${i}`,0]));ledger.set('hot',0);
    let server=await start(backend,'initial');
    await reconcile(server,ledger); // Activate cells before timed windows.
    for(let repeat=1;repeat<=repeats;repeat++) for(const spec of specs.filter(s=>requested.includes(s.name))) for(const c of concurrency) {
      if(warmup) {
        const warm=await phase(server,spec,c,warmup,ledger);
        const {latency_samples_ms,...summary}=warm;
        log.write(JSON.stringify({backend,workload:spec.name,concurrency:c,repeat,phase:'warmup',...summary})+'\n');
        report.warmup_failures=(report.warmup_failures??0)+warm.failed;
        if(warm.host_exited) throw new Error(`Host exited during warmup: ${backend} ${spec.name} c=${c}`);
      }
      const trial=await phase(server,spec,c,seconds,ledger);
      const {latency_samples_ms,...summary}=trial;
      const row={backend,workload:spec.name,concurrency:c,repeat,...summary};
      const sample=`${backend}-${spec.name}-c${c}-r${repeat}.json`;
      await writeFile(`${output}/${sample}`,JSON.stringify(latency_samples_ms));
      row.latency_samples_file=sample;
      report.trials.push(row);log.write(JSON.stringify(row)+'\n');
      await writeFile(`${output}/report.json`,JSON.stringify(report,null,2));
      if(trial.host_exited) throw new Error(`Host exited during trial: ${backend} ${spec.name} c=${c}`);
      if(spec.name.startsWith('write')) row.reconciled=await reconcile(server,ledger);
      await writeFile(`${output}/report.json`,JSON.stringify(report,null,2));
      console.log(`${backend} ${spec.name} c=${c} r=${repeat} ${row.rps.toFixed(1)} req/s p99=${row.latency_ms.p99?.toFixed(2)}ms errors=${row.failed}`);
    }
    const before=await reconcile(server,ledger);
    await stop(server);
    server=await start(backend,'cold-recovery');
    const after=await reconcile(server,ledger);
    assert.deepEqual(after,before);
    report.recovery.push({backend,passed:true,shutdown:'graceful',cold_local_storage:true,counts:after});
    await stop(server);
    await writeFile(`${output}/report.json`,JSON.stringify(report,null,2));
  }
  report.finished=new Date().toISOString();
  report.total_failed=report.trials.reduce((n,t)=>n+t.failed,0);
  await writeFile(`${output}/report.json`,JSON.stringify(report,null,2));
  console.log(`Report: ${output}/report.json`);
} catch(error) {
  report.fatal_error=String(error.stack);
  await writeFile(`${output}/report.json`,JSON.stringify(report,null,2));
  throw error;
} finally {
  if(active) {active.child.kill('SIGTERM');await Promise.race([active.closed,delay(10000,undefined,{ref:false})]);
    if(active.child.exitCode===null && active.child.signalCode===null){active.child.kill('SIGKILL');await active.closed;}}
  agent.destroy();log.end();
}
