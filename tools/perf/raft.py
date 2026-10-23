#!/usr/bin/env python3
"""Three-container native multi-Raft/RustFS setup and verified write measurements.

Resources are retained for inspection. Use the printed Docker context and project
prefix to manage them. Secrets live only in the mode-0700 run config directory.
"""
import argparse
import concurrent.futures as futures
import collections
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import random
import secrets
import shutil
import subprocess
import threading
import time
import urllib.error
import urllib.request
from run import create_bucket


def port_base(value):
    port = int(value)
    if not 1024 <= port <= 65532:
        raise argparse.ArgumentTypeError("port base must leave four ports in 1024..65535")
    return port


def captured_node_limits(config):
    # Docker omits an unused optional map. Actual CPU/memory quotas remain
    # mandatory; a missing quota must never become a successful zero/default.
    captured = {key: config[key] for key in ('NanoCpus', 'CpusetCpus', 'Memory', 'MemorySwap')}
    tmpfs = config.get('Tmpfs')
    if tmpfs is None:
        tmpfs = {}
    if not isinstance(tmpfs, dict):
        raise ValueError('invalid Docker tmpfs metadata')
    captured['Tmpfs'] = tmpfs
    return captured


def command(*args):
    p = subprocess.run(args, capture_output=True, text=True)
    if p.returncode:
        # Do not echo arguments: Docker environment arguments contain credentials.
        raise RuntimeError(f'{args[0]} failed: {p.stderr[-3000:]}')
    return p.stdout.strip()


def request(base, path, token, method='GET', headers=None, timeout=15):
    req = urllib.request.Request(base + path, method=method,
        headers={'Authorization': 'Bearer ' + token, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def parallel(fn, items, workers=16):
    with futures.ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(fn, items))


def wait_for_placement(total, balance, deadline, log, scan=parallel,
                       sleep=time.sleep, now=time.monotonic):
    pending=list(range(total))
    while True:
        ready=scan(balance,pending)
        pending=[c for c,ok in zip(pending,ready) if not ok]
        log(f'Awaiting {len(pending)}/{total} groups; a final full topology scan is required')
        if not pending:
            ready=scan(balance,range(total))
            pending=[c for c,ok in enumerate(ready) if not ok]
            if not pending:
                log(f'Full topology scan verified {total}/{total} assigned leaders')
                break
        if now()>deadline:raise RuntimeError('leader placement deadline')
        sleep(2)


def save_measurement(root, result, ordinal):
    """Retain each window even when one sweep repeats a load setting."""
    rate = result['offered_rate']
    suffix = f'-q{rate:g}' if rate else ''
    result['evidence_file'] = f"write-w{ordinal}-r{result['repeat']}-c{result['concurrency']}{suffix}.json"
    with (root / result['evidence_file']).open('x') as stream:
        stream.write(json.dumps(result) + '\n')


def load(config, seconds, concurrency, rate=0):
    """Closed-loop HTTP/1.1 keepalive clients, no write retries."""
    stop = time.monotonic() + seconds
    start = time.monotonic()
    lock = threading.Lock()
    samples, errors, receipts = [], [], []
    issue_lock = threading.Lock()
    next_issue = start
    def worker(w):
        nonlocal next_issue
        rng = random.Random(w)
        connections = [http.client.HTTPConnection(f'172.29.81.{11+n}',8082,timeout=15) for n in range(3)]
        local_samples, local_errors, local_receipts = [], [], []
        while time.monotonic() < stop:
            if rate:
                with issue_lock:
                    due = max(time.monotonic(), next_issue)
                    next_issue = due + 1 / rate
                if due >= stop:
                    break
                time.sleep(max(0, due - time.monotonic()))
                if time.monotonic() >= stop:
                    break
            # One in-flight command per cell is the current SDK contract.
            c = rng.choice(range(w, config['cells'], concurrency))
            node = 0 if config.get('primary_only') else c % 3
            headers = {'Authorization': 'Bearer ' + config['token'],
                'x-command-id': secrets.token_hex(16) + ':1',
                'x-retry-until-ms': str(int(time.time()*1000)+300000)}
            identity = {'cell': c, 'command_id': headers['x-command-id'],
                        'retry_until_ms': headers['x-retry-until-ms']}
            began = time.monotonic()
            try:
                conn = connections[node]
                conn.request('POST', f'/counters/{c}?delta=1', headers=headers)
                response = conn.getresponse()
                body = response.read().decode()
                elapsed = (time.monotonic()-began)*1000
                if response.status != 200:
                    local_errors.append({**identity, 'status': response.status, 'error': body, 'ms':elapsed})
                else:
                    receipt = json.loads(body)
                    local_samples.append(elapsed)
                    local_receipts.append({**identity, **receipt})
            except Exception as e:
                local_errors.append({**identity, 'error':str(e), 'ms':(time.monotonic()-began)*1000})
                connections[node].close()
        for conn in connections:
            conn.close()
        with lock:
            samples.extend(local_samples); errors.extend(local_errors); receipts.extend(local_receipts)
    with futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(worker, range(concurrency)))
    time.sleep(max(0, stop - time.monotonic()))
    elapsed = time.monotonic()-start
    samples.sort()
    def percentile(p):
        return samples[min(len(samples)-1, math.ceil(p*len(samples))-1)] if samples else None
    result = {'load_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), 'concurrency':concurrency, 'offered_rate':rate, 'offered_seconds':seconds, 'elapsed_seconds':elapsed,
        'successful_writes':len(samples), 'errors':errors, 'tps':len(samples)/elapsed,
        'latency_ms':{'p50':percentile(.5),'p95':percentile(.95),'p99':percentile(.99),
            'max':max(samples) if samples else None}, 'receipts':receipts, 'samples_ms':samples}
    print(json.dumps(result), flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--context',default='colima-cellar-raft-perf')
    p.add_argument('--port-base',type=port_base,default=19800,help='four loopback ports for RustFS and the three nodes')
    p.add_argument('--image',default='cellar-raft-perf:ios-7da4ddb5')
    p.add_argument('--cells-per-primary',type=int,default=500)
    p.add_argument('--primary-only',action='store_true',help='one primary leading the configured cells, two voting followers')
    p.add_argument('--seconds',type=int,default=20)
    p.add_argument('--tick-ms',type=int,default=100)
    p.add_argument('--queue-items',type=int,default=256)
    p.add_argument('--archive-producers',type=int,default=2,choices=range(1,5),help='bounded publications across distinct cells; physical replay shares one workspace')
    p.add_argument('--sql-workers',type=int,default=2,choices=range(1,17),help='fixed SQL owners per node')
    p.add_argument('--sql-worker-queue',type=int,default=32,choices=range(1,257),help='work reservations per SQL owner; total capacity is workers times this value')
    p.add_argument('--archive-tmpfs-mib',type=int,default=0,choices=[0,512],help='bounded disposable archive verification workspace; Raft images/logs stay on disk')
    p.add_argument('--concurrency',type=int,nargs='+',default=[1,8,32,64])
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--rates',type=float,nargs='+',default=[0],help='global requests/sec; zero is unpaced closed-loop load')
    p.add_argument('--output')
    p.add_argument('--continue-seed',type=Path,help='resume only journaled premeasurement seed commands using their original identities')
    p.add_argument('--continue-setup',type=Path,help='continue an existing live run only before any seed or measured writes')
    p.add_argument('--placement-seconds',type=int,default=600)
    p.add_argument('--load-config')
    args=p.parse_args()
    assert all(math.isfinite(rate) and rate >= 0 for rate in args.rates)
    if args.load_config:
        load(json.loads(Path(args.load_config).read_text()),args.seconds,args.concurrency[0],args.rates[0]);return
    assert 1 <= args.cells_per_primary <= 1365 and args.seconds > 0 and args.repeats > 0
    total=args.cells_per_primary if args.primary_only else args.cells_per_primary*3
    assert all(1 <= c <= total for c in args.concurrency)
    assert all(math.isfinite(rate) and rate >= 0 for rate in args.rates)
    owner=lambda c:0 if args.primary_only else c%3
    continuing = args.continue_setup is not None or args.continue_seed is not None
    if continuing:
        root=(args.continue_seed or args.continue_setup).resolve()
        report=json.loads((root/'report.json').read_text())
        if report['measurements'] or report['completed'] or (report.get('placement') and not args.continue_seed):
            raise ValueError('continuation requires a live setup failure before measured writes')
        if args.continue_seed and not (root/'seed-journal.jsonl').exists():
            raise ValueError('seed continuation requires its original command journal')
        if report['cells'] != total or report['context'] != args.context or report['tick_ms'] != args.tick_ms or report.get('archive_tmpfs_mib',0) != args.archive_tmpfs_mib or report.get('primary_only',False)!=args.primary_only or report.get('port_base',19800)!=args.port_base:
            raise ValueError('continuation must preserve cell count, context, port base and tick interval')
        if report.get('archive_producers',1) != args.archive_producers:
            raise ValueError('continuation must preserve archive producer concurrency')
        if report['sql_workers'] != args.sql_workers or report['sql_worker_queue'] != args.sql_worker_queue:
            raise ValueError('continuation must preserve SQL workers and work capacity')
        shutil.copyfile(root/'report.json',root/f"report-before-continuation-{int(time.time())}.json")
        report.setdefault('setup_failures',[]).append(report.pop('failure','interrupted setup'))
        report.setdefault('continuations',[]).append({'unix':time.time(),
            'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'placement_seconds':args.placement_seconds,'phase':'seed' if args.continue_seed else 'placement'})
        config=root/'config';token=(config/'token').read_text();tag=report['prefix']
        docker=lambda *a:command('docker','--context',args.context,*a)
    else:
        stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d-%H%M%S')
        root=Path(args.output or f'.cellar/raft-perf/{stamp}').resolve();root.mkdir(parents=True,exist_ok=False)
        config=root/'config';config.mkdir(mode=0o700)
        tag='cellar-raft-'+stamp
        token=secrets.token_hex(32);access=secrets.token_hex(12);secret=secrets.token_hex(32)
        (config/'token').write_text(token)
        credentials=config/'store-credentials.json'
        credentials.write_text(json.dumps({'access_key_id':access,'secret_access_key':secret}))
        credentials.chmod(0o600)
        for n in range(1,4): (config/f'process{n}').write_bytes(secrets.token_bytes(16))
        for name in ('ca.der','server.der','key.der'):
            shutil.copyfile(Path('crates/cellar/transport/tcp/fixtures')/name,config/name)
        (config/'load.json').write_text(json.dumps({'token':token,'cells':total,'primary_only':args.primary_only}))
        docker=lambda *a:command('docker','--context',args.context,*a)
        report={'source':command('git','rev-parse','HEAD'),'working_diff_sha256':hashlib.sha256(command('git','diff').encode()).hexdigest(),
            'source_files':{str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path('Cargo.toml'), Path('Cargo.lock'), Path('tools/perf/raft_node.rs'),Path('tools/perf/raft.py'),Path('tools/perf/raft.Dockerfile'),Path('examples/raft-counter/counter.rs'),Path('crates/cellar/Cargo.toml'),Path('crates/cellar/storage/sqlite/command.rs'),Path('crates/cellar-ltx/src/db.rs'),Path('crates/cellar-ltx/src/apply.rs'),Path('crates/cellar-ltx/src/generation.rs'), *sorted(Path('crates/cellar/durability/raft').rglob('*.rs'))]},
            'image':docker('image','inspect',args.image,'--format','{{.Id}}'),
            'binary_sha256':docker('run','--rm','--entrypoint','sha256sum',args.image,'/usr/local/bin/cellar-perf-raft').split()[0],
            'context':args.context,'port_base':args.port_base,'prefix':tag,'cells':total,'voters':3,
            'vm_cpu_count':int(docker('info','--format','{{.NCPU}}')),
            'host_cpu_count':os.cpu_count(),
            'archive_tmpfs_mib':args.archive_tmpfs_mib, 'archive_producers':args.archive_producers,
            'sql_workers':args.sql_workers, 'sql_worker_queue':args.sql_worker_queue,
            'queue_items':args.queue_items,'primary_only':args.primary_only,'tick_ms':args.tick_ms,'write_durability':'Quorum','archive':'RustFS','measurements':[], 'completed':False}
    def save(): (root/'report.json').write_text(json.dumps(report,indent=2)+'\n')
    save()
    def log(s): print(s,flush=True)
    log(f'Run directory: {root}\nDocker context: {args.context}\nResource prefix: {tag}')
    nodes=[tag+f'-node{n}' for n in range(1,4)]
    bases=[f'http://127.0.0.1:{args.port_base+1+n}' for n in range(3)]
    def sql_resources():
        observed=[]
        for base in bases:
            code,usage=request(base,'/resources',token)
            if code!=200 or usage.get('sql') is None:
                raise RuntimeError('SQL resource observation unavailable')
            sql=usage['sql']
            if sql['running_workers']!=args.sql_workers or sql['worker_capacity']!=args.sql_workers or sql['work_capacity']!=args.sql_workers*args.sql_worker_queue or sql['queue_closed']:
                raise RuntimeError('physical SQL workers/capacity differ from requested configuration')
            observed.append(usage)
        return observed
    try:
        if not continuing:
            docker('network','create','--subnet','172.29.81.0/24',tag)
            docker('volume','create',tag+'-rustfs')
            docker('run','--rm','-v',tag+'-rustfs:/data','alpine:3.22','chown','10001:10001','/data')
            docker('run','-d','--name',tag+'-rustfs','--network',tag,'--network-alias','rustfs',
                '--cpus','2','--cpuset-cpus','4,5','--memory','4g','--memory-swap','4g',
                '--ulimit','nofile=65536:65536','-p',f'127.0.0.1:{args.port_base}:9000',
                '-v',tag+'-rustfs:/data','-e','RUSTFS_ACCESS_KEY='+access,'-e','RUSTFS_SECRET_KEY='+secret,
                '-e','RUSTFS_CONSOLE_ENABLE=false','-e','RUSTFS_OBS_LOGGER_LEVEL=error',
                'ghcr.io/rustfs/rustfs@sha256:bffcab0c9d647aab0055d1c69d340b202d0909966b385932d4ead1aeb7602858','/data')
            deadline=time.monotonic()+120
            while True:
                try:
                    create_bucket(f'http://127.0.0.1:{args.port_base}','raft-perf',access,secret);break
                except Exception:
                    if time.monotonic()>deadline:raise
                    time.sleep(1)
            for n,name in enumerate(nodes,1):
                docker('volume','create',name)
                docker('run','-d','--name',name,'--network',tag,'--ip',f'172.29.81.{n+10}',
                    '--cpus','1','--cpuset-cpus',str(n-1),'--memory','3g','--memory-swap','3g',
                    '--ulimit','nofile=65536:65536','-p',f'127.0.0.1:{args.port_base+n}:8082',
                    '-v',name+':/data','-v',str(config)+':/config:ro',
                    *(['--tmpfs',f'/data/node/raft-cells/archive-verifier:rw,noexec,nosuid,nodev,size={args.archive_tmpfs_mib}m'] if args.archive_tmpfs_mib else []),
                    '-e',f'RAFT_NODE={n}','-e',f'RAFT_CELLS={total}','-e',f'RAFT_TICK_MS={args.tick_ms}','-e',f'RAFT_PRIMARY_ONLY={int(args.primary_only)}','-e',f'RAFT_QUEUE_ITEMS={args.queue_items}','-e',f'RAFT_ARCHIVE_PRODUCERS={args.archive_producers}',
                    '-e',f'RAFT_SQL_WORKERS={args.sql_workers}','-e',f'RAFT_SQL_WORKER_QUEUE={args.sql_worker_queue}',
                    '-e','AWS_ACCESS_KEY_ID='+access,'-e','AWS_SECRET_ACCESS_KEY='+secret,args.image)
        log('Waiting for native multi-Raft registration and listeners...')
        deadline=time.monotonic()+900
        for base in bases:
            while True:
                try:
                    status,_=request(base,'/admin/0',token)
                    if status==200:break
                except Exception: pass
                if time.monotonic()>deadline:raise RuntimeError('multi-Raft startup deadline')
                for name in nodes:
                    state=json.loads(docker('inspect','--format','{{json .State}}',name))
                    if not state['Running']:raise RuntimeError(f'{name} exited: {state}')
                time.sleep(2)
        report['rustfs_image']=docker('inspect','--format','{{.Image}}',tag+'-rustfs')
        report['vm_memory_bytes']=int(docker('info','--format','{{.MemTotal}}'))
        report['limits']=[json.loads(docker('inspect','--format','{{json .HostConfig}}',name)) for name in nodes]
        # Only record resource-related fields: full inspect includes credentials.
        report['limits']=[captured_node_limits(raw) for raw in report['limits']]
        assert all(r['NanoCpus']==1000000000 and r['Memory']==3*1024**3 for r in report['limits'])
        docker('pull','python:3.12-slim')
        log('Balancing real leaders to the requested primary placement...')
        deadline=time.monotonic()+args.placement_seconds
        def balance(c):
            status,s=request(bases[owner(c)],f'/admin/{c}',token)
            if status!=200: return False
            if s['leader']!=owner(c)+1 and s['leader']:
                request(bases[s['leader']-1],f'/admin/{c}',token,'POST')
                return False
            return s['ready']
        wait_for_placement(total,balance,deadline,log)
        report['placement']={'leaders_per_node':([total,0,0] if args.primary_only else [args.cells_per_primary]*3),'followers_per_node':([0,total,total] if args.primary_only else [args.cells_per_primary*2]*3)}
        save()  # Persist the no-continuation boundary before the first seed write.
        expected=collections.Counter(); latest={}
        seed_lock=threading.Lock()
        plans={};seeded={}
        journal=root/'seed-journal.jsonl'
        if journal.exists():
            for line in journal.read_text().splitlines():
                item=json.loads(line);c=item['cell']
                if item['event']=='planned':
                    if c in plans and plans[c]!=item:
                        raise ValueError('multiple seed plans for one cell')
                    plans[c]=item
                elif item['event']=='outcome' and item['status']==200:
                    seeded[c]=item
        def seed_record(value):
            with seed_lock, journal.open('a') as stream:
                stream.write(json.dumps({'unix':time.time(),**value})+'\n')
                stream.flush();os.fsync(stream.fileno())
        def seed(c):
            if c in seeded:
                assert seeded[c]['command_id']==plans[c]['command_id'] and seeded[c]['response']['value']==1
                return c,seeded[c]['response']
            if c in plans:
                command_id=plans[c]['command_id'];retry_until=plans[c]['retry_until_ms']
            else:
                command_id=secrets.token_hex(16)+':1';retry_until=str(int(time.time()*1000)+300000)
                seed_record({'event':'planned','cell':c,'command_id':command_id,'retry_until_ms':retry_until,'delta':1})
            while int(time.time()*1000)<int(retry_until):
                code,r=request(bases[owner(c)],f'/counters/{c}?delta=1',token,'POST',
                    {'x-command-id':command_id,'x-retry-until-ms':retry_until})
                seed_record({'event':'outcome','cell':c,'command_id':command_id,'status':code,'response':r})
                if code==200:
                    if r['value']!=1:raise RuntimeError(f'seed {c}: expected exactly one increment')
                    return c,r
                if code==503 and (r=='Overloaded' or r.startswith('Ambiguous')):
                    time.sleep(.1);continue
                raise RuntimeError(f'seed {c}: {code} {r}')
            raise RuntimeError(f'seed {c}: original retry deadline expired; no new command identity issued')
        for c,r in parallel(seed,range(total),2):
            expected[c]+=1;latest[c]=r
        log(f'Committed initial writes to all {total} groups.')
        def wait_archive(label):
            log(label)
            began=time.monotonic()
            deadline=began+600
            while True:
                states=parallel(lambda c:request(bases[owner(c)],f'/admin/{c}',token),range(total))
                covered=sum(code==200 and s['ready'] and s['leader']==owner(c)+1 and s['archived']>=latest[c]['index'] for c,(code,s) in enumerate(states))
                log(f'Archive covers {covered}/{total} acknowledged versions')
                if covered==total:
                    return states,time.monotonic()-began
                if time.monotonic()>deadline:raise RuntimeError('archive coverage deadline')
                time.sleep(5)
        initial_states,initial_wait=wait_archive('Draining the initial verified RustFS archive backlog before measurement...')
        (root/'initial-archive-verification.json').write_text(json.dumps(initial_states)+'\n')
        report['initial_archive_wait_seconds']=initial_wait
        report['initial_resources']=sql_resources()
        save()
        stop=threading.Event()
        def sample():
            with (root/'resources.jsonl').open('w') as f:
                while not stop.is_set():
                    stats=docker('stats','--no-stream','--format','{{json .}}',*nodes,tag+'-rustfs')
                    for row in stats.splitlines(): f.write(json.dumps({'unix':time.time(),'stats':json.loads(row)})+'\n')
                    f.flush();stop.wait(2)
        sampler=threading.Thread(target=sample,daemon=True);sampler.start()
        try:
            for repeat in range(args.repeats):
                settings=[(c,rate) for rate in args.rates for c in args.concurrency]
                for concurrency,rate in (settings if repeat%2==0 else list(reversed(settings))):
                    log(f'Measuring repeat={repeat+1} concurrency={concurrency} rate={rate:g} for {args.seconds}s')
                    raw=docker('run','--rm','--network',tag,'--cpus','2','--cpuset-cpus','6,7','--memory','1g',
                        '-v',str(Path('tools/perf').resolve())+':/work:ro','-v',str(config)+':/config:ro',
                        'python:3.12-slim','python','/work/raft.py','--load-config','/config/load.json',
                        '--seconds',str(args.seconds),'--concurrency',str(concurrency),'--rates',str(rate))
                    result=json.loads(raw);result['repeat']=repeat+1
                    save_measurement(root, result, len(report['measurements']) + 1)
                    for r in result.pop('receipts'):
                        c=r.pop('cell');expected[c]+=1
                        if r['index']>latest[c]['index']: latest[c]=r
                    result.pop('samples_ms');result['error_free']=not result['errors']
                    result['error_rate']=len(result['errors'])/(len(result['errors'])+result['successful_writes'])
                    report['measurements'].append(result);save()
                    log(json.dumps({k:result[k] for k in ('concurrency','offered_rate','successful_writes','tps','latency_ms')}))
                    if any(e.get('status')!=503 or e.get('error')!='Overloaded' for e in result['errors']):
                        raise RuntimeError('non-admission write failure or ambiguity; see raw outcomes')
        finally:
            stop.set();sampler.join(timeout=10)
        log('Verifying every cell on all three replicas with minimum-version proofs...')
        def verify(item):
            n,c=item
            code,r=request(bases[n],f'/counters/{c}',token,headers={
                'x-read-consistency':'at-least','x-min-version':latest[c]['version_token']})
            if code!=200 or r['value']!=expected[c] or r['served_by']!=n+1:
                raise RuntimeError(f'replica verification node={n+1} cell={c}: {code} {r}')
            return {'node':n+1,'cell':c,'value':r['value'],'index':r['index']}
        verification=parallel(verify,((n,c) for n in range(3) for c in range(total)),8)
        (root/'replica-verification.json').write_text(json.dumps(verification)+'\n')
        report['verified_replica_values']=len(verification)
        states,archive_wait=wait_archive('Waiting for verified RustFS archive coverage...')
        report['final_archive_wait_seconds']=archive_wait
        report['final_resources']=sql_resources()
        (root/'archive-verification.json').write_text(json.dumps(states)+'\n')
        report['cgroups']=[{
            'node':name,
            'memory_peak_bytes':int(docker('exec',name,'cat','/sys/fs/cgroup/memory.peak')),
            'memory_events':docker('exec',name,'cat','/sys/fs/cgroup/memory.events'),
            'cpu_stat':docker('exec',name,'cat','/sys/fs/cgroup/cpu.stat'),
            'filesystem':docker('exec',name,'df','-T','/data','/data/node/raft-cells/archive-verifier'),
            'image':docker('inspect','--format','{{.Image}}',name),
        } for name in nodes]
        assert all(c['image']==report['image'] for c in report['cgroups'])
        report['verified_archive_groups']=total
        report['all_windows_error_free']=all(m['error_free'] for m in report['measurements'])
        report['completed']=True
        log('Verification passed. Cluster and volumes remain running for inspection.')
    except Exception as e:
        report['failure']=str(e);log('FAILED: '+str(e));raise
    finally:
        for name in nodes:
            p=subprocess.run(['docker','--context',args.context,'logs',name],text=True,capture_output=True)
            (root/(name+'.log')).write_text(p.stdout+p.stderr)
        save()


if __name__=='__main__':main()
