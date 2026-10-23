#!/usr/bin/env python3
"""Diagnostic-only, matched sequential/batched stored-field reads with full arrival accounting."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
from types import SimpleNamespace

import affinity
from baseline import Host, Connection, require, save, digest
from build_identity import inspect_binary
from offered import measure, observed_idle, summarize
from residency import populate

SOURCES = ['baseline.rs', 'baseline_host.rs', 'baseline_native.rs', 'baseline_app.py',
           'offered_load.rs', 'offered.py', 'baseline.py', 'residency.py', 'affinity.py', 'batch_reads.py']


def cpu(pid):
    if not Path('/proc').exists():
        return None
    fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
    ticks = os.sysconf('SC_CLK_TCK')
    return dict(user_ticks=int(fields[11]), system_ticks=int(fields[12]), ticks_per_second=ticks)


def run(args):
    root = args.root.resolve()
    root.mkdir(parents=True, exist_ok=False)
    builds = json.loads((root.parent / 'builds.json').read_text())
    placement = None if args.smoke else affinity.placement(
        os.environ['CELLAR_LOAD_HOST_CPUS'], os.environ['CELLAR_LOAD_CLIENT_CPUS'])
    expected_free = os.environ.get('CELLAR_LOAD_PYTHON', '3.14.5').endswith('t')
    repetitions = 1 if args.smoke else int(os.environ['CELLAR_LOAD_REPETITIONS'])
    require(1 <= repetitions <= 5, 'invalid repetition count')
    profile = 'dev' if args.smoke else 'lab'
    rates, duration, warmup = ([100], 40, 20) if args.smoke else ([500, 15000, 30000], 3000, 500)
    diff = subprocess.check_output(['git', 'diff', '--binary', 'HEAD'])
    require(args.smoke or not diff, 'measurement checkout has source edits')
    report = dict(status='running', smoke=args.smoke, repetitions=repetitions, rates=rates,
                  duration_ms=duration, warmup_ms=warmup, expected_free=expected_free, profile=profile,
                  source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                  source_diff_sha256=hashlib.sha256(diff).hexdigest(), placement=placement,
                  measurement_sources={name:digest(Path('tools/python') / name) for name in SOURCES},
                  client_sha256=digest(args.client), cases=[])
    labels = [('native', 0), ('selected', 0), ('native', 1), ('selected', 1)]
    try:
        for repetition in range(repetitions):
            for fields in ([2, 8] if repetition % 2 == 0 else [8, 2]):
                order = labels[repetition % 4:] + labels[:repetition % 4]
                for runtime, batch in order:
                    executable = (args.hosts / ('load-native' if runtime == 'native' else 'load-python')).resolve()
                    identity = inspect_binary(executable, Path('.'), embedded=runtime != 'native')
                    require(identity['sha256'] == builds['executables'][runtime]['sha256'], 'frozen host changed')
                    source = Path('tools/python/baseline_app.py').read_bytes()
                    require((executable.read_bytes().find(source) >= 0) == (runtime != 'native'), 'wrong application source')
                    folder = root / f'{runtime}-{fields}-{batch}-r{repetition}'
                    folder.mkdir()
                    host_cpus = placement['host'] if placement else None
                    host = Host(executable, ('native' if runtime == 'native' else 'python') + '-resident',
                                folder, 'primary', command_prefix=affinity.prefix(host_cpus, folder / 'host-affinity.json'))
                    case = dict(runtime=runtime, fields=fields, batch=batch, repetition=repetition,
                                host_sha256=identity['sha256'], status='running', phases=[])
                    report['cases'].append(case)
                    try:
                        case['startup_ns'] = host.readiness()
                        case['host'] = host.ready
                        require(host.ready['embed_compiled'] == (runtime != 'native') and
                                host.ready['debug_assertions'] == args.smoke and host.ready['durability'] == 'local-only', 'host policy')
                        if placement:
                            case['affinity'] = affinity.verify(folder / 'host-affinity.json', host_cpus, host.process.pid)
                        case['python_runtime'] = host.python_runtime() if runtime != 'native' else None
                        if runtime != 'native':
                            require(case['python_runtime']['version'].startswith('3.14.5 ') and
                                    case['python_runtime']['free_threaded'] == expected_free and
                                    case['python_runtime']['gil_enabled'] != expected_free, 'wrong embedded ABI/GIL')
                        populate(host, 0, 8)
                        connection = Connection(host.origin)
                        try:
                            for index in range(8):
                                require(json.loads(connection.request('POST', f'/cells/resident-{index}', b'')) == {'value':1}, 'seed failed')
                        finally:
                            connection.close()
                        reads = [dict(path=f'/cells/resident-{i}?fields={fields}&batch={batch}',
                                      expected_body=list(json.dumps({'value':fields}, separators=(',', ':')).encode())) for i in range(8)]
                        save(folder / 'reads.json', reads)
                        options = SimpleNamespace(cpu_placement=placement, cell_echo=False, max_inflight=64,
                                                  body_bytes=0, timeout_ms=1000, max_lag_ms=5, build_profile=profile)
                        for rate in rates:
                            phase = dict(rate=rate)
                            case['phases'].append(phase)
                            phase['warmup'] = measure(args.client, host, options, rate, warmup, folder / f'warmup-{rate}.json', reads)
                            phase['before'] = observed_idle(host, 8)
                            phase['affinity_before'] = affinity.verify_threads(host.process.pid, host_cpus) if placement else None
                            phase['cpu_before'] = cpu(host.process.pid)
                            phase['measurement'] = measure(args.client, host, options, rate, duration, folder / f'measured-{rate}.json', reads)
                            phase['cpu_after'] = cpu(host.process.pid)
                            phase['affinity_after'] = affinity.verify_threads(host.process.pid, host_cpus) if placement else None
                            phase['after'] = observed_idle(host, 8)
                            print(runtime, fields, batch, repetition, rate, phase['measurement']['outcomes'], flush=True)
                        if runtime != 'native':
                            require(host.python_runtime() == case['python_runtime'], 'ABI changed')
                        case['shutdown'] = host.stop()
                        case['status'] = 'complete'
                    except BaseException as error:
                        case.update(status='failed', error=repr(error))
                        raise
                    finally:
                        host.cleanup()
                        save(root / 'measurements.json', report)
        require(report['measurement_sources'] == {name:digest(Path('tools/python') / name) for name in SOURCES}, 'sources changed during measurements')
        require(digest(args.client) == report['client_sha256'], 'client changed')
        report['status'] = 'complete'
    finally:
        save(root / 'measurements.json', report)
    verify(root)


def verify(root):
    report = json.loads((root / 'measurements.json').read_text())
    builds = json.loads((root.parent / 'builds.json').read_text())
    require(report['status'] == 'complete' and len(report['cases']) == 8 * report['repetitions'], 'incomplete experiment')
    smoke = report['smoke']
    require(report['rates'] == ([100] if smoke else [500,15000,30000]), 'rates changed')
    require(report['duration_ms'] == (40 if smoke else 3000) and report['warmup_ms'] == (20 if smoke else 500), 'window changed')
    if not smoke:
        require(report['source_diff_sha256'] == hashlib.sha256(b'').hexdigest(), 'source edits in comparison')
        placement = report['placement']
        require(placement['host'] and placement['client'] and not set(placement['host']) & set(placement['client']) and
                set(placement['host'] + placement['client']) <= set(placement['available']), 'invalid CPU partition')
    identities, outcomes, rows = set(), Counter(), []
    arrivals = 0
    for case in report['cases']:
        identity = (case['runtime'], case['fields'], case['batch'], case['repetition'])
        require(identity not in identities and case['runtime'] in ('native','selected') and
                case['fields'] in (2,8) and case['batch'] in (0,1) and 0 <= case['repetition'] < report['repetitions'], 'invalid case')
        identities.add(identity)
        folder = root / f"{case['runtime']}-{case['fields']}-{case['batch']}-r{case['repetition']}"
        require(case['status'] == 'complete' and case['host_sha256'] == builds['executables'][case['runtime']]['sha256'], 'host identity')
        require(case['host']['embed_compiled'] == (case['runtime'] != 'native') and case['host']['debug_assertions'] == smoke and
                case['host']['durability'] == 'local-only', 'host policy')
        if not smoke:
            require(case['host']['os'] == 'linux' and affinity.verify(folder / 'host-affinity.json', placement['host']) == case['affinity'], 'host affinity')
        stopped = json.loads((folder / 'complete.json').read_text())
        exit_status = json.loads((folder / 'primary-exit.json').read_text())
        require(stopped == case['shutdown'] and stopped['complete'] and stopped['native']['complete'] and
                exit_status['returncode'] == 0 and not exit_status['killed_by_cleanup'], 'incomplete host shutdown')
        if case['runtime'] != 'native':
            runtime = case['python_runtime']
            require(runtime['version'].startswith('3.14.5 ') and runtime['free_threaded'] == report['expected_free'] and
                    runtime['gil_enabled'] != report['expected_free'], 'ABI mismatch')
            py = stopped['python']
            require(not py['host_failed'] and not py['runtime_failed'] and
                    all(py[key] == 0 for key in ['pending_tasks','pending_objects','pending_threads']) and
                    py['objects_created'] == py['objects_retired'] == 8 and
                    py['bridge']['pending_reservations'] == py['bridge']['queued_bytes'] == 0 and
                    all(py['bridge']['operations'][key] == 0 for key in ['pending','queued','running','replies_pending']), 'Python work not joined')
        expected_reads = [dict(path=f"/cells/resident-{i}?fields={case['fields']}&batch={case['batch']}",
                               expected_body=list(json.dumps({'value':case['fields']},separators=(',',':')).encode())) for i in range(8)]
        require(json.loads((folder / 'reads.json').read_text()) == expected_reads, 'read specification changed')
        require([p['rate'] for p in case['phases']] == report['rates'], 'missing phase')
        for phase in case['phases']:
            for name,prefix,duration in [('warmup','warmup',report['warmup_ms']),('measurement','measured',report['duration_ms'])]:
                path = folder / f"{prefix}-{phase['rate']}.json"
                raw = json.loads(path.read_text())
                expected = dict(url=case['host']['url'].rstrip('/')+'/', rate=phase['rate'],duration_ms=duration,
                                max_inflight=64, body_bytes=0, timeout_ms=1000, max_lag_ms=5, reads=expected_reads)
                require(raw['workload'] == expected and raw['debug_assertions'] == smoke, 'raw workload changed')
                summary = summarize(raw)
                require(summary == phase[name], 'raw arrivals differ from summary')
                for sample in raw['samples']:
                    require(sample['outcome'] in ('success','client_late','client_capacity','http_status') and
                            (sample['outcome'] != 'http_status' or sample['status'] == 503), 'unexpected transport/application failure')
                if not smoke:
                    affinity.verify(path.with_suffix('.affinity.json'), placement['client'])
                arrivals += summary['arrivals'];outcomes.update(summary['outcomes'])
            if not smoke:
                for boundary in ('before','after'):
                    masks = phase['affinity_'+boundary]
                    require(masks and all(mask == placement['host'] for mask in masks.values()), 'host thread escaped CPU partition')
            for boundary in ('before','after'):
                snapshot = phase[boundary]
                require(snapshot['native_resident_cells'] == 8 and 0 <= snapshot['memory_sample_age_ns'] < 1_000_000_000, 'stale/wrong population observation')
                if case['runtime'] != 'native':
                    current = snapshot['runtime']['interpreter']
                    require(0 <= current['age_ns'] < 1_000_000_000 and current['work']['resident_objects'] == 8 and
                            all(current['work'][key] == 0 for key in ['active_invocations','active_tasks','pending_retirements']), 'unsettled interpreter observation')
                    bridge = snapshot['runtime']['bridge']
                    require(bridge['pending_reservations'] == bridge['queued_bytes'] == 0 and
                            all(bridge['operations'][key] == 0 for key in ['pending','queued','running','replies_pending']), 'unsettled bridge observation')
            row = dict(runtime=case['runtime'], fields=case['fields'], batch=case['batch'], repetition=case['repetition'], rate=phase['rate'])
            measured = phase['measurement'];success = measured['outcomes'].get('success',0)
            require(success > 0, 'no successful reads')
            row.update(rps=measured['completed_in_window_per_second'],p99_ns=measured['successful_request_latency_ns']['p99'])
            if case['runtime'] != 'native':
                before,after = phase['before']['runtime'],phase['after']['runtime']
                row['interpreter_cpu_ns_per_success'] = (after['interpreter']['interpreter']['thread_cpu_ns']-before['interpreter']['interpreter']['thread_cpu_ns'])/success
                if phase['rate'] in (100,500):
                    require(not measured['outcomes'].get('http_status',0), 'low-load refusal')
                    require(after['bridge']['operations']['admitted']-before['bridge']['operations']['admitted'] == success*(2 if case['batch'] else case['fields']+1), 'wrong number of bridge operations')
            if not smoke:
                before,after = phase['cpu_before'],phase['cpu_after']
                require(before['ticks_per_second'] == after['ticks_per_second'] > 0, 'invalid CPU clock')
                row['process_cpu_ns_per_success'] = ((after['user_ticks']+after['system_ticks'])-(before['user_ticks']+before['system_ticks']))*1e9/before['ticks_per_second']/success
            rows.append(row)
    require(arrivals == sum(report['rates'])*(report['duration_ms']+report['warmup_ms'])//1000*len(report['cases']), 'missing arrivals')
    comparisons = []
    for runtime in ('native','selected'):
        for fields in (2,8):
            for rate in report['rates']:
                groups = [[row for row in rows if row['runtime']==runtime and row['fields']==fields and row['rate']==rate and row['batch']==batch] for batch in (0,1)]
                require(all(len(group)==report['repetitions'] for group in groups), 'incomplete pairs')
                metrics = ['rps','p99_ns'] + ([] if smoke else ['process_cpu_ns_per_success']) + (['interpreter_cpu_ns_per_success'] if runtime!='native' else [])
                medians = [{key:statistics.median(row[key] for row in group) for key in metrics} for group in groups]
                comparisons.append(dict(runtime=runtime, fields=fields, rate=rate,sequential=medians[0],batch=medians[1]))
    save(root / 'verified.json', dict(arrivals=arrivals,joined_hosts=len(report['cases']),outcomes=dict(outcomes),rows=rows,comparisons=comparisons,smoke=smoke))
    print('Verified',arrivals,'arrivals and',len(report['cases']),'joined hosts',flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--hosts',type=Path)
    parser.add_argument('--client',type=Path)
    parser.add_argument('--smoke',action='store_true')
    parser.add_argument('--verify',action='store_true')
    args = parser.parse_args()
    if args.verify: verify(args.root)
    else:
        if not args.hosts or not args.client: parser.error('--hosts and --client are required')
        run(args)
