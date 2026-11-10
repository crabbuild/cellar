# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Archive latency upper bounds and joined native-owner witnesses for CG-14.

A command's send time precedes its acknowledgement. The first subsequently
observed archive covering it therefore gives a conservative latency upper bound.
Unavailable polls retain debt; a final archive snapshot alone is not a lag sample.
"""
import math
from pathlib import Path
import threading
import time

from raft_workload import Ledger
from raft_run import require, decode_json

CLOSE = 'CELLAR_CG14_CLOSE '


class ArchiveObserver:
    def __init__(self, path, layout):
        self.ledger = Ledger(path)
        self.layout = layout
        self.lock = threading.Lock()
        self.pending = {}
        self.phase = 'seed'

    def acknowledge(self, cell, reply, started_ns):
        group = self.layout.group(cell)['ordinal']
        with self.lock:
            self.ledger.append({'event': 'ack', 'cell': cell, 'group': group,
                'index': reply['index'], 'started_ns': started_ns, 'finished_ns': time.monotonic_ns(),
                'phase': self.phase})
            if group not in self.pending or reply['index'] > self.pending[group][1]:
                self.pending[group] = (cell, reply['index'])

    def capture(self, probe):
        with self.lock:
            pending = dict(self.pending)
        for group, (cell, index) in pending.items():
            began = time.monotonic_ns()
            try:
                status, view = probe(cell)
                row = {'status': status, 'response': view}
                if status == 200 and view.get('archived') is not None and view['archived'] >= index:
                    with self.lock:
                        if self.pending.get(group) == (cell, index):
                            del self.pending[group]
            except Exception as error:
                row = {'error_type': type(error).__name__}
            self.ledger.append({'event': 'archive', 'group': group, 'cell': cell,
                'started_ns': began, 'finished_ns': time.monotonic_ns(), **row})

    def close(self):
        self.ledger.close()


def archive_lag(path, operations, layout):
    """Join every successful command to its raw HTTP and archive observations."""
    expected = []
    for line in Path(operations).read_text().splitlines():
        row = decode_json(line)
        if row['event'] in ('seed_complete', 'complete') and row.get('status') == 200 \
                and (row['event'] == 'seed_complete' or row['operation'] == 'command'):
            expected.append((row['cell'], row['response']['index'], row.get('phase', 'seed')))
    acks, observations, previous = [], {}, 0
    for line in Path(path).read_text().splitlines():
        row = decode_json(line)
        require(row['order'] == previous + 1, 'archive observation sequence is incomplete')
        previous += 1
        require(type(row['started_ns']) is int and type(row['finished_ns']) is int
                and 0 < row['started_ns'] <= row['finished_ns'], 'invalid archive observation interval')
        cell, group = row['cell'], row['group']
        require(type(group) is int and type(cell) is int and group == layout.group(cell)['ordinal'], 'archive observation history differs')
        if row['event'] == 'ack':
            require(type(row['index']) is int and row['index'] > 0, 'invalid acknowledged archive position')
            acks.append(row)
        else:
            require(row['event'] == 'archive', 'unknown archive observation')
            if row.get('status') == 200:
                view = row['response']
                target = layout.group(cell)
                require(view['cell'] == cell and view['registered'] is True
                        and view['group'] == target['group'] and view['group_incarnation'] == target['incarnation'],
                        'archive poll returned another history')
                if view['archived'] is not None:
                    require(type(view['archived']) is int and view['archived'] >= 0, 'invalid archive frontier')
                    observations.setdefault(group, []).append(row)
    actual = [(row['cell'], row['index'], row['phase']) for row in acks]
    # Threads can durably append replies in a different order from the workload
    # ledger. Compare the complete multiset, preserving duplicates.
    require(sorted(actual) == sorted(expected), 'archive acknowledgements differ from the operation ledger')
    lags = []
    for ack in acks:
        covered = next((row for row in observations.get(ack['group'], [])
                        if row['finished_ns'] >= ack['finished_ns']
                        and row['response']['archived'] >= ack['index']), None)
        require(covered is not None, 'archive observation never covers an acknowledged command')
        lags.append((covered['finished_ns'] - ack['started_ns']) / 1e9)
    require(bool(lags) and all(math.isfinite(n) and n >= 0 for n in lags), 'no archive latency observations')
    return {'max_archive_lag_seconds': max(lags), 'acknowledged_commands': len(acks),
            'scope': 'send-to-observed-archive upper bound including seed, warmup and measurement'}


def capture_joins(directory, ownership, engine, planned):
    """Called after physical stop. Logs come from the exact retained container IDs."""
    result = []
    for placement in planned:
        number = placement['node']
        name = 'cellar-mr14-' + ownership['owner'] + f'-node{number}'
        identity = ownership['containers'][name]['id']
        engine.bind()
        state = engine.inspect(name, ownership['owner'], identity)
        text = engine.invoke('logs', identity)
        (Path(directory) / f'joined-node{number}.log').write_text(text + '\n')
        reports = [decode_json(line[len(CLOSE):]) for line in text.splitlines() if line.startswith(CLOSE)]
        result.append({'node': number, 'id': identity, 'state': state, 'reports': reports})
    return result


def joined_owners(rows, planned, ownership):
    require(len(rows) == len(planned), 'missing native join witnesses')
    failures = 0
    for row, placement in zip(rows, planned):
        node = placement['node']
        require(row['node'] == node and ownership['containers'][f"cellar-mr14-{ownership['owner']}-node{node}"]['id'] == row['id'],
                'join witness differs from physical owner')
        state = row['state']
        require(state['id'] == row['id'] and state['running'] is False and state['paused'] is False,
                'native join witness predates physical stop')
        healthy = state['exit_code'] == 0 and len(row['reports']) == 1
        if healthy:
            report = row['reports'][0]
            healthy = (all(type(report[key]) is int and report[key] >= 0 for key in (
                    'node', 'groups_total', 'reported_groups', 'omitted_groups', 'failed_groups', 'retained_calls_at_shutdown')) and report['node'] == node and report['groups_total'] == placement['replica_groups']
                and report['reported_groups'] + report['omitted_groups'] == report['groups_total']
                and report['failed_groups'] == 0 and report['retained_calls_at_shutdown'] == 0
                and report['mutation_outcomes'] == 'no_unresolved_local_producers')
        failures += int(not healthy)
    return failures
