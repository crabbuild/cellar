# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Frozen, serial occupancy campaigns over the existing real RF3 workload owner.

Verification recomputes ledgers, physical topology and kernel samples. A missing
metric is unmeasured, never zero. Functional campaigns and release qualification
are separate results; unfinished lifecycle adapters fail before provisioning.
"""
import copy
import hashlib
import math
from pathlib import Path
import re
import subprocess
from types import SimpleNamespace
import uuid

from cell_group_resources import Engine, OwnedRun, save, stop
from raft_analyze import analyze
from raft_run import (ROOT, RUSTFS, input_digest, read_json, require, run as run_workload,
                      source_manifest, validate_profile, verify_image)
from raft_topology import local_placement, vm_requirements
from raft_faults import preflight_pause, validate_scenario
from raft_docker import BoundContainer, discover

SUPPORTED = {'idle_density', 'mixed_quorum', 'mixed_s3'}
FIELDS = {'format', 'run_id', 'source_revision', 'context', 'engine_id', 'image',
          'rustfs_image', 'data_root', 'port_base', 'profiles_file', 'profiles_sha256', 'workload'}


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            value.update(block)
    return value.hexdigest()


def revision():
    return subprocess.check_output(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], text=True).strip()


def config(path):
    value = read_json(path)
    require(isinstance(value, dict) and FIELDS <= set(value) <= FIELDS | {'fault_schedule'},
            'missing or unknown private configuration fields')
    require(type(value['format']) is int and value['format'] == 1, 'unsupported configuration format')
    require(str(uuid.UUID(value['run_id'])) == value['run_id'], 'run_id must be a canonical UUID')
    require(re.fullmatch(r'[0-9a-f]{40}', value['source_revision']) is not None, 'invalid source revision')
    require(value['source_revision'] == revision(), 'configured source revision differs from checkout')
    require(value['rustfs_image'] == RUSTFS, 'RustFS digest differs from the qualified adapter')
    require(re.fullmatch(r'(sha256:[0-9a-f]{64}|[^\s]+@sha256:[0-9a-f]{64})', value['image']) is not None,
            'Cellar image must use an immutable digest')
    require(type(value['port_base']) is int and 1024 <= value['port_base'] <= 65530, 'invalid explicit port allocation')
    for name in ('data_root', 'profiles_file'):
        target = Path(value[name])
        require(target.is_absolute() and str(target.resolve()) == str(target), 'paths must be absolute without symlinks')
    data = Path(value['data_root'])
    require(data.is_dir() and data != Path('/') and ROOT.resolve() not in (data, *data.parents),
            'data_root must be an existing external dedicated directory')
    require(re.fullmatch(r'[0-9a-f]{64}', value['profiles_sha256']) is not None
            and digest(value['profiles_file']) == value['profiles_sha256'], 'changed frozen profile catalog')
    validate_profile(value['workload'])
    Engine(value['context'], value['engine_id'])  # Validate without invoking Docker.
    return value


def selected_profile(settings, identifier):
    catalog = read_json(settings['profiles_file'])
    require(type(catalog.get('format')) is int and catalog['format'] == 1, 'unsupported profile catalog')
    matches = [row for row in catalog['profiles'] if row['id'] == identifier]
    require(len(matches) == 1, 'unknown or duplicate profile')
    profile = matches[0]
    require(identifier in SUPPORTED, 'profile needs an unfinished lifecycle or independent-host adapter')
    require(profile['topology'] == 'local_containers' and type(profile['nodes']) is int
            and 3 <= profile['nodes'] <= 5 and profile['voters'] == 3,
            'local adapter cannot claim independent failure domains')
    for name, low, high in [('cells', 1, 1000000), ('duration_seconds', 1, 86400), ('repetitions', 1, 10)]:
        require(type(profile[name]) is int and low <= profile[name] <= high, 'invalid campaign ' + name)
    require(profile['occupancies'] and len(set(profile['occupancies'])) == len(profile['occupancies'])
            and all(type(n) is int and 1 <= n <= 128 for n in profile['occupancies']), 'invalid occupancy matrix')
    require(profile['image_modes'] and len(set(profile['image_modes'])) == len(profile['image_modes'])
            and set(profile['image_modes']) <= {'materialized', 'extents'}, 'invalid image mode matrix')
    rate = profile['offered_requests_per_second']
    require(type(rate) in (int, float) and math.isfinite(rate) and 0 <= rate <= 100000,
            'invalid frozen arrival rate')
    require((rate == 0) is (identifier == 'idle_density'), 'idle and mixed traffic profiles differ')
    require(rate * profile['duration_seconds'] <= 1000000, 'offline ledger bound is one million arrivals')
    require(set(profile['gate_occupancies']) <= set(profile['occupancies']), 'gated cohort is absent')
    require(profile['gate_occupancies'] and profile['checks'], 'profile must retain nonempty acceptance gates')
    numeric_gates(profile, {})  # Validate numeric contracts before provisioning.
    require(set(profile['ratios']) <= set(profile['checks']), 'ratio must name a required metric')
    for spec in profile['ratios'].values():
        require(isinstance(spec, dict) and {'numerator', 'denominator'} <= set(spec)
                <= {'numerator', 'denominator', 'control_occupancy'}
                and all(isinstance(spec[key], str) and spec[key] for key in ('numerator', 'denominator')),
                'invalid matched-control ratio')
        require('control_occupancy' not in spec or spec['control_occupancy'] in profile['occupancies'],
                'ratio control occupancy is missing')
    return profile


def cohorts(settings, profile):
    result = []
    # Repeat ordering is fixed and recorded. Every cohort gets fresh state, equal
    # logical population and the same phase/seed trace regardless of occupancy.
    for repetition in range(profile['repetitions']):
        for image in profile['image_modes']:
            for occupancy in profile['occupancies']:
                workload = copy.deepcopy(settings['workload'])
                workload.update(image_mode=image, durability='s3' if profile['id'] == 'mixed_s3' else 'quorum',
                                read_fraction=.2, read_consistency='at-least',
                                skew={'hot_fraction': 1, 'hot_probability': 0})
                workload['seed'] += repetition
                workload['topology'] = {'nodes': profile['nodes'], 'voters': 3, 'failure_domains': 1}
                workload['cells'] = {'total': profile['cells'], 'active': profile['cells'], 'per_group': occupancy}
                workload['arrival']['rate_per_second'] = profile['offered_requests_per_second']
                workload['timing']['measurement_seconds'] = profile['duration_seconds']
                workload['timing']['soak_seconds'] = 0
                validate_profile(workload)
                local_placement(workload)
                owner = hashlib.sha256(f"{settings['run_id']}:{len(result)}".encode()).hexdigest()[:24]
                result.append({'ordinal': len(result), 'repetition': repetition, 'image_mode': image,
                               'occupancy': occupancy, 'owner': owner, 'workload': workload})
    return result


def preflight(settings, identifier, engine=None):
    profile = selected_profile(settings, identifier)
    planned = cohorts(settings, profile)  # Refuse unsupported capability before any external call.
    schedule = settings.get('fault_schedule', {'scenario': 'healthy', 'config': None})
    require(isinstance(schedule, dict) and set(schedule) == {'scenario', 'config'}, 'invalid fault schedule')
    require(schedule['scenario'] in ('healthy', 'slow-follower', 's3-outage', 's3-recovery'),
            'fault capability is unavailable; no resources created')
    for row in planned:
        validate_scenario(schedule['scenario'], schedule['config'], row['workload'])
    engine = engine or Engine(settings['context'], settings['engine_id'])
    engine.bind()
    info = read_json_text(engine.invoke('info', '--format',
        '{"os":{{json .OSType}},"cpus":{{.NCPU}},"memory":{{.MemTotal}},'
        '"memory_limit":{{json .MemoryLimit}},"cpu_quota":{{json .CpuCfsQuota}}}'))
    minimum = vm_requirements(profile['nodes'])
    require(info['os'] == 'linux' and info['memory_limit'] is True and info['cpu_quota'] is True,
            'CPU and memory limits must be enforced by Linux')
    require(info['cpus'] >= minimum['cpus'] and info['memory'] >= minimum['memory_bytes'],
            'dedicated VM has insufficient CPU or memory; resource bounds are unchanged')
    require(not engine.invoke('ps', '-q', '--filter', 'label=io.cellar.mr14.run'),
            'another workload is active; stop only its recorded owner')
    build = verify_image(settings['image'], engine.invoke)
    build['source_revision'] = settings['source_revision']
    build['compiler'] = engine.invoke('run', '--rm', '--network', 'none', '--entrypoint', 'cat',
                                      settings['image'], '/usr/local/bin/raft-rustc.txt')
    require(build['compiler'].startswith('rustc 1.98.1 '), 'benchmark compiler differs from the frozen toolchain')
    build['command'] = engine.invoke('run', '--rm', '--network', 'none', '--entrypoint', 'cat',
                                     settings['image'], '/usr/local/bin/raft-build-command.txt')
    fault_engine = discover(engine.invoke, settings['context']) if schedule['config'] is not None else None
    capability = preflight_pause(engine.invoke, settings['image'],
        lambda node, owner: BoundContainer(**fault_engine, node=node, owner=owner)) if fault_engine else None
    return {'profile': profile, 'cohorts': planned, 'build': build, 'vm': info,
            'source_manifest': source_manifest(), 'profiles_sha256': settings['profiles_sha256'],
            'fault_schedule': schedule, 'fault_engine': fault_engine, 'fault_capability': capability}


def read_json_text(text):
    from raft_run import decode_json
    return decode_json(text)


def artifacts(root):
    result = {}
    for path in sorted(root.rglob('*')):
        relative = path.relative_to(root)
        # Credential-bearing runtime configuration is private and never hashed
        # into public evidence. It is not consumed by the offline verifier.
        if 'config' in relative.parts or relative.as_posix() == 'receipt.json':
            continue
        require(not path.is_symlink(), 'evidence cannot contain symbolic links')
        if path.is_file():
            result[relative.as_posix()] = digest(path)
    return result


def numeric_gates(profile, values):
    results = {}
    for name, limits in profile['checks'].items():
        require(isinstance(limits, dict) and limits and set(limits) <= {'min', 'max', 'eq'}, 'invalid numeric gate')
        for bound in limits.values():
            require(type(bound) in (int, float) and math.isfinite(bound), 'gate bound must be finite numeric data')
        value = values.get(name)
        if value is None:
            results[name] = {'status': 'unmeasured'}
            continue
        require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
                'gate metric must be finite nonnegative numeric data')
        passed = all(value >= n if op == 'min' else value <= n if op == 'max' else value == n
                     for op, n in limits.items())
        results[name] = {'status': 'passed' if passed else 'failed', 'value': value, 'limits': limits}
    return results


def measured_values(analysis):
    """Only values established by existing independent raw-evidence validators."""
    counts = analysis['summary']['counts']
    samples = counts.get('completed', 0)
    kernel = analysis['kernel']['measurement']
    # The lifetime cgroup peak conservatively includes build-independent setup.
    # No measured zero is inferred for double-writer or owner-join safety gates.
    values = {'refused_requests': counts.get('rejected', 0) + counts.get('generator_rejected', 0),
              'ambiguous_requests': counts.get('ambiguous', 0),
              'unexpected_errors': counts.get('failed', 0), 'latency_samples': samples,
              'actual_cells': analysis['replicas'] // 3,
              'peak_rss_limit_ratio': max(row['lifetime_peak_memory_bytes'] / (3 * 1024**3)
                  for row in kernel if not row['node'].endswith('-store'))}
    voters = [row for row in kernel if not row['node'].endswith('-store')]
    values['cpu_cores'] = sum(row['average_cpu_cores']['maximum'] for row in voters)
    values['process_rss_bytes'] = sum(max(row['process_rss_bytes'].values()) for row in voters)
    values['process_fds'] = sum(max(row['process_fds'].values()) for row in voters)
    observed = analysis['performance']['measurement']
    values['control_bytes_per_second'] = sum(row['counters']['transport_control_write_bytes']
        * 1000000 / row['owner_sample_interval_us'] for row in observed)
    for name, counter in [('engine_write_bytes', 'engine_file_write_completed_bytes'),
                          ('prepared_ltx_bytes', 'sql_prepared_ltx_bytes'),
                          ('s3_get_bytes', 'object_get_body_bytes'),
                          ('s3_put_bytes', 'object_put_confirmed_bytes')]:
        values[name] = sum(row['counters'][counter] for row in observed)
    for name, operations in [('engine_fsyncs', ('engine_file_sync', 'engine_directory_sync')),
                              ('s3_requests', ('object_get', 'object_put_create', 'object_put_update', 'object_put_overwrite'))]:
        values[name] = sum(row['operations'][operation]['started'] for row in observed for operation in operations)
    values['physical_groups'] = sum(row['leader_groups'] for row in analysis['placement'])
    if samples:
        values['p99_ms'] = analysis['summary']['latency_ms']['p99']
    return values


def ratios(profile, row, rows):
    values = dict(row['metrics'])
    for name, spec in profile['ratios'].items():
        if 'control_occupancy' in spec:
            controls = [other for other in rows if other['repetition'] == row['repetition']
                        and other['image_mode'] == row['image_mode'] and other['occupancy'] == spec['control_occupancy']]
            require(len(controls) == 1, 'missing matched occupancy control')
            denominator = controls[0]['metrics'].get(spec['denominator'])
        else:
            denominator = values.get(spec['denominator'])
        numerator = values.get(spec['numerator'])
        if numerator is not None and denominator is not None:
            require(type(denominator) in (int, float) and math.isfinite(denominator) and denominator > 0,
                    'ratio requires a positive measured baseline')
            require(type(numerator) in (int, float) and math.isfinite(numerator), 'ratio numerator is invalid')
            values[name] = numerator / denominator
    controls = [other for other in rows if other['repetition'] == row['repetition']
                and other['image_mode'] == row['image_mode'] and other['occupancy'] == 1]
    if len(controls) == 1:
        for name, source in [('baseline_cpu_cores', 'cpu_cores'), ('baseline_control_bytes', 'control_bytes_per_second')]:
            if source in controls[0]['metrics']:
                values[name] = controls[0]['metrics'][source]
    return values


def verify(root):
    root = Path(root).resolve()
    receipt = read_json(root / 'receipt.json')
    record = read_json(root / 'campaign.json')
    require(receipt['format'] == 1 and receipt['artifacts'] == artifacts(root), 'missing or drifted evidence artifact')
    require(record['completed'] is True, 'unfinished campaign; failed attempts are retained')
    settings = record['settings']
    require(settings['source_revision'] == revision() and record['preflight']['source_manifest'] == source_manifest(),
            'checkout source drifted from frozen executable inputs')
    require(digest(settings['profiles_file']) == settings['profiles_sha256'], 'profile catalog drifted')
    profile = selected_profile(settings, record['profile'])
    require(profile == record['preflight']['profile'], 'selected profile drifted')
    planned = cohorts(settings, profile)
    require(len(record['cohorts']) == len(planned), 'incomplete cohort matrix')
    results = []
    for row, expected in zip(record['cohorts'], planned):
        require(row['plan'] == expected and row['completed'] is True, 'cohort identity or completion differs')
        directory = root / f"cohort-{expected['ordinal']:03}"
        report = read_json(directory / 'workload/report.json')
        require(report['profile'] == expected['workload'] and report['run_id'] == expected['owner']
                and report['context'] == settings['context'] and report['build'] == record['preflight']['build'],
                'workload source, binary, identity or profile differs')
        schedule = record['preflight'].get('fault_schedule', {'scenario': 'healthy', 'config': None})
        require(report['scenario'] == schedule['scenario'] and report['fault_profile'] == schedule['config'],
                'replayed fault schedule differs from the frozen campaign')
        ownership = read_json(directory / 'resources.json')
        require(ownership['campaign'] == settings['run_id'] and ownership['owner'] == expected['owner']
                and ownership['context'] == settings['context'] and ownership['engine_id'] == settings['engine_id']
                and ownership['directory'] == str(directory) and ownership['stops'], 'missing owned joined stop')
        stopped = {item['after']['id'] for item in ownership['stops']
                   if item['after']['running'] is False and item['after']['paused'] is False}
        require(len(stopped) == profile['nodes'] + 2 and stopped == {item['id'] for item in ownership['containers'].values()},
                'incomplete physical process join')
        for artifact in ('operations.jsonl', 'replicas.json', 'topology.json', 'measurements.json',
                         'node-samples.jsonl', 'kernel-measurements.json', 'resources.jsonl', 'probe-timings.jsonl'):
            require((directory / 'workload' / artifact).is_file(), 'missing raw workload artifact: ' + artifact)
        analysis = analyze(directory / 'workload')
        metrics = measured_values(analysis)
        if expected['occupancy'] == 64:
            metrics['measured_groups_occupancy64'] = metrics['physical_groups']
        results.append({**{key: expected[key] for key in ('repetition', 'image_mode', 'occupancy')},
                        'metrics': metrics})
    gated = []
    for row in results:
        if row['occupancy'] in profile['gate_occupancies']:
            gated.append({**row, 'gates': numeric_gates(profile, ratios(profile, row, results))})
    qualified = bool(gated) and all(gate['status'] == 'passed' for row in gated for gate in row['gates'].values())
    # Replacement occupancy-one is a control; it does not stand in for the
    # historical per-cell implementation at a different pinned revision.
    release_profiles = ROOT / 'docs/design/multi-raft/cell-groups/profiles.json'
    release_catalog = settings['profiles_sha256'] == digest(release_profiles)
    if profile['historical_control'] or not release_catalog:
        qualified = False
    return {'verified': True, 'profile': profile['id'], 'cohorts': gated, 'qualified': qualified,
            'all_cohorts': results,
            'historical_control_measured': False, 'release_profile_catalog': release_catalog,
            'scope': 'local serialized SQL workload, RF3 physical inventory and kernel observations'}


def run(settings, identifier, output):
    output = Path(output).absolute()
    require(output.parent == Path(settings['data_root']) and not output.exists()
            and output.name == settings['run_id'], 'output must be the fresh data_root/run_id directory')
    checked = preflight(settings, identifier)
    output.mkdir(mode=0o700)
    record = {'format': 1, 'settings': settings, 'profile': identifier,
              'preflight': checked, 'completed': False, 'cohorts': []}
    save(output / 'campaign.json', record)
    try:
        for plan in checked['cohorts']:
            require(checked['source_manifest'] == source_manifest(), 'source changed between cohorts')
            directory = output / f"cohort-{plan['ordinal']:03}"
            directory.mkdir(mode=0o700)
            owned = OwnedRun(Engine(settings['context'], settings['engine_id']), directory,
                             plan['owner'], settings['run_id'])
            row = {'plan': plan, 'completed': False}
            record['cohorts'].append(row)
            save(output / 'campaign.json', record)
            arguments = SimpleNamespace(output=directory / 'workload', image=settings['image'],
                context=settings['context'], port_base=settings['port_base'],
                scenario=checked['fault_schedule']['scenario'], fault_config=checked['fault_schedule']['config'],
                fault_capability=checked['fault_capability'], fault_engine=checked['fault_engine'],
                observe_log_reclamation=False, run_id=plan['owner'])
            try:
                run_workload(arguments, plan['workload'], checked['build'], owned,
                             checked['vm']['cpus'], checked['vm']['memory'])
                analyze(directory / 'workload')
                row['completed'] = True
            finally:
                row['stop'] = stop(directory, settings['run_id'])
                save(output / 'campaign.json', record)
        record['completed'] = True
    except BaseException as error:
        record['failure_type'] = type(error).__name__
        raise
    finally:
        save(output / 'campaign.json', record)
        save(output / 'receipt.json', {'format': 1, 'artifacts': artifacts(output)})
    return verify(output)
