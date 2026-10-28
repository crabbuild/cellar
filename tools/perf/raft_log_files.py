# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Owned, non-atomic append/rewrite file censuses for reclamation experiments.

Logical lengths and allocated 512-byte blocks describe retained files. Removed
file observations are lower bounds, not a measure of device writes or freed
space: live records can move to rewrite files and active files can grow.
"""
import json
import re
import time

IDENTITY = '{"id":{{json .Id}},"owner":{{json (index .Config.Labels "io.cellar.mr14.run")}},"started":{{json .State.StartedAt}},"running":{{json .State.Running}},"paused":{{json .State.Paused}}}'
SCRIPT = "find /data/node/raft/log -ignore_readdir_race -maxdepth 1 -type f \\( -name '*.raftlog' -o -name '*.rewrite' \\) -printf '%f\\t%i\\t%s\\t%b\\n'"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def parse(text):
    files = {}
    require(isinstance(text, str) and len(text) <= 4 * 1024**2, 'log census exceeds bound')
    for line in text.splitlines():
        fields = line.split('\t')
        require(len(fields) == 4 and re.fullmatch(r'[0-9]{16}\.(raftlog|rewrite)', fields[0]), 'invalid log filename')
        require(fields[0] not in files and len(files) < 32768, 'duplicate or excessive log inventory')
        require(all(re.fullmatch(r'0|[1-9][0-9]*', v) and int(v) < 2**64 for v in fields[1:]), 'invalid log file stat')
        inode, length, blocks = map(int, fields[1:])
        require(inode > 0, 'invalid log inode')
        files[fields[0]] = {'inode': inode, 'bytes': length, 'allocated_bytes': blocks * 512}
    require(bool(files), 'empty engine inventory')
    return files


def capture(docker, nodes, owner):
    rows = []
    for node in nodes:
        began = time.monotonic_ns()
        before = json.loads(docker('inspect', '--format', IDENTITY, node))
        require(before['owner'] == owner and before['running'] is True and before['paused'] is False, 'log census requires running owned node')
        raw = docker('exec', node, '/bin/sh', '-c', SCRIPT)
        after = json.loads(docker('inspect', '--format', IDENTITY, node))
        require(before == after, 'log census process changed')
        parse(raw)
        rows.append({'node': node, 'started_ns': began, 'finished_ns': time.monotonic_ns(), 'identity': before, 'raw': raw})
    return rows


def validate(rows, report):
    expected = {report['prefix'] + '-node' + str(n) for n in range(1, report['profile']['topology']['nodes'] + 1)}
    require(isinstance(rows, list) and len(rows) == len(expected), 'missing log census nodes')
    result = {}
    for row in rows:
        require(set(row) == {'node', 'started_ns', 'finished_ns', 'identity', 'raw'}, 'invalid log census fields')
        node, identity = row['node'], row['identity']
        require(node in expected and node not in result, 'foreign or duplicate log node')
        require(type(row['started_ns']) is int and type(row['finished_ns']) is int
                and 0 < row['started_ns'] < row['finished_ns'], 'invalid log census clock')
        require(set(identity) == {'id', 'owner', 'started', 'running', 'paused'}
                and re.fullmatch('[0-9a-f]{64}', identity['id']) and identity['owner'] == report['run_id']
                and identity['running'] is True and identity['paused'] is False
                and isinstance(identity['started'], str) and bool(identity['started']), 'invalid log census identity')
        files = parse(row['raw'])
        result[node] = {**row, 'files': files, 'file_count': len(files),
                        'bytes': sum(f['bytes'] for f in files.values()),
                        'allocated_bytes': sum(f['allocated_bytes'] for f in files.values())}
    require(len({row['identity']['id'] for row in result.values()}) == len(expected), 'duplicate log census container')
    return result


def analyze(raw, periodic, report):
    phases = ['warmup', 'measurement'] if report['profile']['timing']['warmup_seconds'] else ['measurement']
    require(set(raw) == {*phases, 'settled'}, 'missing log census boundary')
    result = {'phases': {}, 'settled': [], 'sampling': 'non-atomic retained file census; deleted-file bytes are lower bounds'}
    previous = None
    for phase in phases:
        require(set(raw[phase]) == {'before', 'after'}, 'invalid log census phase')
        before, after = (validate(raw[phase][edge], report) for edge in ('before', 'after'))
        workload = report[phase]
        require(max(r['finished_ns'] for r in before.values()) <= workload['started_monotonic_seconds'] * 10**9
                and min(r['started_ns'] for r in after.values()) >= (workload['started_monotonic_seconds'] + workload['elapsed_seconds']) * 10**9,
                'log census does not enclose workload')
        pairs = [(before, after)] + ([(previous, before)] if previous is not None else [])
        for left, right in pairs:
            for node in left:
                require(left[node]['identity'] == right[node]['identity'] and left[node]['finished_ns'] < right[node]['started_ns'], 'log source changed or census overlaps')
        result['phases'][phase] = [{'node': node, 'before': {k: before[node][k] for k in ('bytes', 'allocated_bytes', 'file_count')},
                                    'after': {k: after[node][k] for k in ('bytes', 'allocated_bytes', 'file_count')}} for node in before]
        previous = after
    settled = validate(raw['settled'], report)
    samples = [validate(rows, report) for rows in periodic]
    require(bool(samples), 'missing periodic log census')
    require(all(sample[node]['identity'] == settled[node]['identity'] for sample in samples for node in settled), 'periodic log source differs from settled source')
    # Phase boundaries and periodic observations are serialized independently;
    # use only the ordered periodic stream to infer disappearing physical files.
    vanished = {node: {} for node in settled}
    for left, right in zip(samples, samples[1:]):
        for node in left:
            require(left[node]['identity'] == right[node]['identity'] == settled[node]['identity']
                    and left[node]['finished_ns'] < right[node]['started_ns'], 'periodic log source changed or clock overlaps')
            new = {(name, info['inode']) for name, info in right[node]['files'].items()}
            for name, info in left[node]['files'].items():
                if (name, info['inode']) not in new:
                    vanished[node][(name, info['inode'])] = max(vanished[node].get((name, info['inode']), 0), info['bytes'])
    for node, row in settled.items():
        require(previous[node]['identity'] == row['identity'] and previous[node]['finished_ns'] < row['started_ns'], 'settled log source changed')
        result['settled'].append({'node': node, **{k: row[k] for k in ('bytes', 'allocated_bytes', 'file_count')},
            'observed_removed_files': len(vanished[node]), 'observed_removed_bytes_lower_bound': sum(vanished[node].values()),
            'maximum_periodic_bytes': max(s[node]['bytes'] for s in samples)})
    return result
