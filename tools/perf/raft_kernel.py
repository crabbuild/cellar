"""Phase-boundary cgroup v2/process observations; never a whole-device estimate."""
import json
import math
import re
import time


FILES = {
    'cpu': '/sys/fs/cgroup/cpu.stat', 'quota': '/sys/fs/cgroup/cpu.max',
    'memory': '/sys/fs/cgroup/memory.current', 'peak': '/sys/fs/cgroup/memory.peak',
    'limit': '/sys/fs/cgroup/memory.max', 'events': '/sys/fs/cgroup/memory.events',
    'swap': '/sys/fs/cgroup/memory.swap.current', 'swap_limit': '/sys/fs/cgroup/memory.swap.max',
    'io': '/sys/fs/cgroup/io.stat', 'status': '/proc/1/status', 'process': '/proc/1/stat',
}
SCRIPT = 'set -eu\n' + ''.join(f"printf 'FILE {key}\\n'\ncat {path}\n" for key, path in FILES.items()) + "set -- /proc/1/fd/*\nprintf 'FILE fds\\n%s\\n' \"$#\"\n"
INSPECT = '{"id":{{json .Id}},"owner":{{json (index .Config.Labels "io.cellar.mr14.run")}},"started":{{json .State.StartedAt}},"running":{{json .State.Running}},"paused":{{json .State.Paused}}}'
CPU = ('usage_usec', 'user_usec', 'system_usec', 'nr_periods', 'nr_throttled', 'throttled_usec')
EVENTS = ('low', 'high', 'max', 'oom', 'oom_kill', 'oom_group_kill')
IO = ('rbytes', 'wbytes', 'rios', 'wios', 'dbytes', 'dios')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def integer(value):
    require(isinstance(value, str) and re.fullmatch(r'0|[1-9][0-9]*', value) is not None, 'invalid kernel integer')
    result = int(value)
    require(result < 2**64, 'kernel counter overflow')
    return result


def mapping(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        require(len(fields) == 2 and fields[0] not in result, 'invalid kernel counter fields')
        result[fields[0]] = integer(fields[1])
    return result


def parse(text):
    require(isinstance(text, str) and len(text) <= 65536, 'kernel snapshot exceeds bound')
    sections, key = {}, None
    for line in text.splitlines():
        if line.startswith('FILE '):
            key = line[5:]
            require(key in {*FILES, 'fds'} and key not in sections, 'unexpected/duplicate kernel section')
            sections[key] = []
        else:
            require(key is not None, 'kernel data without section')
            sections[key].append(line)
    require(set(sections) == {*FILES, 'fds'}, 'missing kernel section')
    raw = {key: '\n'.join(lines) for key, lines in sections.items()}
    cpu, events = mapping(raw['cpu']), mapping(raw['events'])
    require(set(CPU) <= cpu.keys() and set(EVENTS) <= events.keys(), 'missing kernel counters')
    io = {}
    for line in raw['io'].splitlines():
        device, *values = line.split()
        require(re.fullmatch(r'[0-9]+:[0-9]+', device) is not None and device not in io and len(io) < 32, 'invalid block device identity')
        counters = mapping('\n'.join(value.replace('=', ' ') for value in values))
        require(set(IO) <= counters.keys(), 'missing cgroup I/O counters')
        io[device] = {key: counters[key] for key in IO}
    status = {}
    for line in raw['status'].splitlines():
        name, _, value = line.partition(':')
        if name in ('VmRSS', 'VmHWM', 'Threads'):
            require(name not in status, 'duplicate process status')
            fields = value.split()
            require(len(fields) == (1 if name == 'Threads' else 2) and (name == 'Threads' or fields[1] == 'kB'), 'invalid process units')
            status[name] = integer(fields[0]) * (1 if name == 'Threads' else 1024)
    require(set(status) == {'VmRSS', 'VmHWM', 'Threads'}, 'missing process status')
    # The comm field can contain spaces and parentheses. Field 22 is starttime.
    process = re.fullmatch(r'1 \(.*\) ([^\n]+)', raw['process'])
    require(process is not None and len(process[1].split()) >= 20, 'invalid process identity')
    started = integer(process[1].split()[19])
    quota = raw['quota'].split()
    require(len(quota) == 2, 'invalid CPU quota')
    result = {key: integer(raw[key]) for key in ('memory', 'peak', 'limit', 'swap', 'swap_limit', 'fds')}
    require(result['peak'] >= result['memory'] and status['VmHWM'] >= status['VmRSS'] and status['Threads'] > 0 and result['fds'] > 0, 'invalid kernel gauges')
    return {**result, 'cpu': {key: cpu[key] for key in CPU}, 'events': {key: events[key] for key in EVENTS},
            'quota': [integer(value) for value in quota], 'io': io, 'process': status, 'process_start_ticks': started}


def capture(docker, nodes, owner):
    result = []
    for node in nodes:
        started = time.monotonic_ns()
        before = json.loads(docker('inspect', '--format', INSPECT, node))
        require(before['owner'] == owner and before['running'] is True and before['paused'] is False, 'kernel target is not the running owned container')
        raw = docker('exec', node, '/bin/sh', '-c', SCRIPT)
        after = json.loads(docker('inspect', '--format', INSPECT, node))
        require(before == after, 'container changed during kernel observation')
        parse(raw)
        result.append({'node': node, 'started_ns': started, 'finished_ns': time.monotonic_ns(), 'identity': before, 'raw': raw})
    return result


def snapshots(rows, report):
    nodes = report['profile']['topology']['nodes']
    expected = {f"{report['prefix']}-node{n}": (1, 3 * 1024**3) for n in range(1, nodes + 1)}
    expected[report['prefix'] + '-store'] = (2, 4 * 1024**3)
    require(isinstance(rows, list) and len(rows) == len(expected), 'missing kernel node/store snapshot')
    result = {}
    for row in rows:
        node = row['node']
        require(node in expected and node not in result, 'invalid kernel node identity')
        require(all(type(row[k]) is int for k in ('started_ns', 'finished_ns')) and 0 <= row['started_ns'] < row['finished_ns'], 'invalid kernel clock')
        identity = row['identity']
        require(re.fullmatch(r'[0-9a-f]{64}', identity['id']) is not None and identity['owner'] == report['run_id']
                and identity['running'] is True and identity['paused'] is False
                and isinstance(identity['started'], str) and bool(identity['started']), 'invalid kernel owner/process identity')
        value = parse(row['raw'])
        cpus, memory = expected[node]
        require(value['quota'] == [cpus * 100000, 100000] and value['limit'] == memory and value['swap_limit'] == 0 and value['swap'] == 0, 'kernel resource limits differ')
        result[node] = {**row, 'value': value}
    require(len({r['identity']['id'] for r in result.values()}) == len(expected), 'duplicate physical kernel source')
    return result


def difference(left, right):
    require(right >= left, 'kernel counter reset')
    return right - left


def analyze(raw, report):
    phases = ['warmup', 'measurement'] if report['profile']['timing']['warmup_seconds'] else ['measurement']
    require(isinstance(raw, dict) and set(raw) == set(phases), 'missing kernel phase')
    result, previous = {}, None
    for phase in phases:
        require(set(raw[phase]) == {'before', 'after'}, 'missing kernel boundary')
        before, after = (snapshots(raw[phase][edge], report) for edge in ('before', 'after'))
        require(max(r['finished_ns'] for r in before.values()) < min(r['started_ns'] for r in after.values()), 'overlapping kernel boundaries')
        measured = report[phase]
        start, elapsed = measured['started_monotonic_seconds'], measured['elapsed_seconds']
        require(all(type(v) in (int, float) and math.isfinite(v) and v > 0 for v in (start, elapsed)), 'invalid kernel workload clock')
        require(max(r['finished_ns'] for r in before.values()) <= start * 10**9
                and min(r['started_ns'] for r in after.values()) >= (start + elapsed) * 10**9, 'kernel observations do not enclose workload')
        result[phase] = []
        for node, first in before.items():
            last = after[node]
            sequence = [first, last] if previous is None else [previous[node], first, last]
            for left, right in zip(sequence, sequence[1:]):
                require(left['finished_ns'] < right['started_ns'] and left['identity'] == right['identity']
                        and left['value']['process_start_ticks'] == right['value']['process_start_ticks'], 'kernel process changed or clock overlaps')
                for field in ('cpu', 'events'):
                    for key in left['value'][field]:
                        difference(left['value'][field][key], right['value'][field][key])
                difference(left['value']['peak'], right['value']['peak'])
                require(left['value']['io'].keys() <= right['value']['io'].keys(), 'cgroup I/O device disappeared')
                for device, counters in left['value']['io'].items():
                    for key, value in counters.items():
                        difference(value, right['value']['io'][device][key])
            a, b = first['value'], last['value']
            lower, upper = last['started_ns'] - first['finished_ns'], last['finished_ns'] - first['started_ns']
            cpu = {key: b['cpu'][key] - a['cpu'][key] for key in CPU}
            io = {device: {key: value - a['io'].get(device, {}).get(key, 0) for key, value in counters.items()} for device, counters in b['io'].items()}
            peak = b['peak']
            headroom = 1 - peak / b['limit']
            result[phase].append({'node': node, 'interval_ns': {'minimum': lower, 'maximum': upper},
                'observer_window_ns': {'before': first['finished_ns'] - first['started_ns'], 'after': last['finished_ns'] - last['started_ns']},
                'cpu_delta': cpu, 'average_cpu_cores': {'minimum': cpu['usage_usec'] * 1000 / upper, 'maximum': cpu['usage_usec'] * 1000 / lower},
                'cgroup_io_delta': io, 'lifetime_peak_memory_bytes': peak, 'lifetime_memory_headroom_fraction': headroom,
                'process_rss_bytes': {'before': a['process']['VmRSS'], 'after': b['process']['VmRSS']},
                'process_threads': {'before': a['process']['Threads'], 'after': b['process']['Threads']},
                'process_fds': {'before': a['fds'], 'after': b['fds']}, 'memory_events': b['events'],
                'memory_objective_met': headroom >= report['profile']['slo']['memory_headroom_fraction'] and b['events']['oom'] == b['events']['oom_kill'] == b['events']['oom_group_kill'] == 0})
        previous = after
    return result
