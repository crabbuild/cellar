# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Shared, owned RustFS pauses and acknowledgement-window evidence.

A process freeze is a bounded backend outage, not packet loss or an S3 service
failure. Histories and final state must also pass the adapter's own checker.
"""
import bisect
import math
from pathlib import Path
import re
import threading
import time

from raft_faults import owned_state, require
from raft_workload import Ledger, input_digest


def is_store_fault(config):
    return isinstance(config, dict) and config.get('target') == 'object-store'


def validate_store_fault(config, profile, *, recovery=False):
    require(isinstance(config, dict) and set(config) == {
        'format', 'target', 'pause_ms', 'max_pause_ms', 'interval_ms',
        'start_after_seconds', 'stop_before_seconds', 'max_start_lateness_ms'}, 'invalid store fault fields')
    require(is_store_fault(config) and config['format'] == 1, 'unsupported store fault profile')
    for key, value in config.items():
        if key != 'target':
            require(type(value) is int and 0 <= value <= 3600000, 'invalid store fault integer: ' + key)
    require(10 <= config['pause_ms'] < config['max_pause_ms'] < config['interval_ms'], 'invalid store pause bounds')
    if recovery:
        require(profile['durability'] in ('s3', 'bucket'), 'recovery drill requires object-store acknowledgements')
        require(profile['timing']['request_timeout_ms'] + 1000 < config['pause_ms'] < config['max_pause_ms'] <= 60000,
                'recovery pause must exceed both native and HTTP request budgets')
        required = (profile['timing']['measurement_seconds'] + profile['slo']['max_recovery_seconds']) * 1000
        require(profile['timing']['retry_window_ms'] > required + profile['timing']['request_timeout_ms'] + 1000,
                'original retry window cannot cover the recovery drill')
    else:
        require(config['max_pause_ms'] < profile['timing']['request_timeout_ms'], 'store pause exceeds request budget')
    require(profile['read_fraction'] < 1, 'store fault requires write traffic')
    start = config['start_after_seconds']
    end = profile['timing']['measurement_seconds'] - config['stop_before_seconds']
    count = math.floor((end - start - config['max_pause_ms'] / 1000) / (config['interval_ms'] / 1000)) + 1
    require(1 <= count <= 1000, 'store fault schedule is empty or exceeds its bound')
    return [start + n * config['interval_ms'] / 1000 for n in range(count)]


def validate_store_scenario(scenario, config, profile):
    require(scenario in ('healthy', 's3-outage', 's3-recovery', 's3-fenced-recovery'), 'unsupported baseline fault capability')
    if scenario == 's3-fenced-recovery':
        require(profile['durability'] == 'bucket' and isinstance(config, dict)
                and type(config.get('pause_ms')) is int and config['pause_ms'] > 10000,
                'fenced recovery requires bucket authority and a pause beyond its ten-second lease')
        offered = sum(math.ceil(profile['timing'][k] * profile['arrival']['rate_per_second']) for k in ('warmup_seconds', 'measurement_seconds'))
        require(offered + 2 <= 4096, 'post-restart write exceeds receipt capacity')
    if config is None:
        require(scenario == 'healthy', scenario + ' requires a store fault profile')
    else:
        validate_store_fault(config, profile, recovery=scenario in ('s3-recovery', 's3-fenced-recovery'))


class StorePause:
    """Join the sole fault owner; inspect ambiguous mutations before cleanup."""
    def __init__(self, root, scenario, config, profile, prefix, run_id, control):
        validate_store_scenario(scenario, config, profile)
        self.offsets = validate_store_fault(config, profile, recovery=scenario in ('s3-recovery', 's3-fenced-recovery'))
        self.scenario, self.config = scenario, config
        self.node, self.run_id = prefix + '-store', run_id
        self.control = control
        self.ledger = Ledger(Path(root) / 'faults.jsonl')
        self.stop = threading.Event()
        self.thread = None
        self.failure = None

    def inspect(self):
        return owned_state(self.control, self.node, self.run_id)

    def record(self, cycle, event, **values):
        self.ledger.append({'cycle': cycle, 'event': event, 'monotonic_seconds': time.monotonic(),
                            'container': self.node, 'container_id': self.control.identity, **values})

    def start(self, began):
        require(self.thread is None, 'store fault owner already started')
        self.thread = threading.Thread(target=self.drive, args=(began,))
        self.thread.start()

    def drive(self, began):
        try:
            for cycle, offset in enumerate(self.offsets):
                if self.stop.wait(max(0, began + offset - time.monotonic())):
                    raise ValueError('store fault schedule cancelled')
                before = self.inspect()
                require(before['paused'] is False, 'store already paused')
                require((time.monotonic() - began - offset) * 1000 <= self.config['max_start_lateness_ms'], 'store fault schedule was late')
                requested = time.monotonic()
                self.record(cycle, 'begin', scheduled_seconds=offset, state=before)
                try:
                    if self.scenario != 'healthy':
                        self.control('pause', self.node)
                    during = self.inspect()
                    require(during['paused'] is (self.scenario != 'healthy'), 'store pause was not observed')
                    self.record(cycle, 'held', state=during)
                    require(time.monotonic() + self.config['pause_ms'] / 1000 < requested + self.config['max_pause_ms'] / 1000,
                            'store pause setup exhausted its time budget')
                    if self.stop.wait(self.config['pause_ms'] / 1000):
                        raise ValueError('store pause cancelled')
                    state = self.inspect()
                    require(state['paused'] is (self.scenario != 'healthy'), 'store pause changed before release')
                    self.record(cycle, 'release', state=state)
                finally:
                    # An unsuccessful pause response does not prove non-mutation.
                    # The control adapter pins immutable ID and owner on inspect.
                    state = self.inspect()
                    release_error = None
                    if state['paused']:
                        try:
                            self.control('unpause', self.node)
                        except Exception as error:
                            release_error = error
                    after = self.inspect()
                    require(after['paused'] is False, 'store remains paused')
                    self.record(cycle, 'released', state=after)
                    if release_error is not None:
                        raise release_error
                require((time.monotonic() - requested) * 1000 <= self.config['max_pause_ms'], 'store pause exceeded its time budget')
        except Exception as error:
            self.failure = str(error)
            self.record(-1, 'failure', error=self.failure)

    def finish(self):
        if self.thread:
            self.thread.join()
        require(self.failure is None, self.failure or 'store fault failed')

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join()
        try:
            self.control.close()
        finally:
            self.ledger.close()


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def verify_store_fault(report, events, operations, *, recovery=None):
    """Check request-dispatch windows, not admission or scheduled arrivals.

    Receipt/model validation is additionally performed by the caller's history
    checker. These timestamps establish observations, not causal attribution of
    all request delay to the object store.
    """
    config, profile, scenario = report['fault_profile'], report['profile'], report['scenario']
    validate_store_scenario(scenario, config, profile)
    require(report['fault_profile_sha256'] == input_digest(config), 'changed store fault profile')
    offsets = validate_store_fault(config, profile, recovery=scenario in ('s3-recovery', 's3-fenced-recovery'))
    resolutions = {row['original_id']: row for row in recovery['resolutions']} if recovery is not None else {}
    require((recovery is not None) is (scenario in ('s3-recovery', 's3-fenced-recovery')), 'missing or undeclared store recovery evidence')
    began = report['measurement']['started_monotonic_seconds']
    require(number(began) and began > 0, 'missing workload clock origin')
    require(len(events) == 4 * len(offsets), 'incomplete store fault schedule')
    identity = None
    previous = began
    windows = []
    for cycle, offset in enumerate(offsets):
        rows = events[cycle * 4:cycle * 4 + 4]
        for n, (row, kind, paused) in enumerate(zip(rows, ('begin', 'held', 'release', 'released'),
                (False, scenario != 'healthy', scenario != 'healthy', False))):
            stamp = row['monotonic_seconds']
            require(number(stamp) and stamp >= previous, 'store fault clock changed')
            previous = stamp
            require(type(row['order']) is int and row['order'] == cycle * 4 + n + 1
                    and type(row['cycle']) is int and row['cycle'] == cycle and row['event'] == kind, 'store fault order changed')
            require(row['container'] == report['prefix'] + '-store', 'wrong store target')
            container_id = row['container_id']
            require(isinstance(container_id, str) and re.fullmatch('[0-9a-f]{64}', container_id)
                    and (identity is None or identity == container_id), 'store container identity changed')
            identity = container_id
            state = row['state']
            require(set(state) == {'owner', 'running', 'paused'} and state['owner'] == report['run_id']
                    and state['running'] is True and state['paused'] is paused, 'store ownership or physical pause differs')
        begin, held, release, released = rows
        require(begin['scheduled_seconds'] == offset, 'store fault schedule changed')
        require(0 <= (begin['monotonic_seconds'] - began - offset) * 1000 <= config['max_start_lateness_ms'], 'store fault started outside its window')
        require((release['monotonic_seconds'] - held['monotonic_seconds']) * 1000 >= config['pause_ms'], 'store pause too short')
        require((released['monotonic_seconds'] - begin['monotonic_seconds']) * 1000 <= config['max_pause_ms'], 'store pause exceeded its bound')
        windows.append((held['monotonic_seconds'], release['monotonic_seconds']))

    durability = profile['durability']
    require(durability in ('s3', 'quorum', 'bucket', 'fleet-or-bucket'), 'unsupported acknowledgement policy')
    requires_store = durability in ('s3', 'bucket')
    pending, seen, commands, ambiguous_witnesses = {}, set(), [], set()
    for event in operations:
        if event.get('phase') != 'measurement' or event['operation'] != 'command':
            continue
        key = event['id']
        if event['event'] == 'invoke':
            require(key not in seen, 'duplicate store witness invocation')
            seen.add(key)
            pending[key] = event
        elif event['event'] == 'complete':
            require(key in pending, 'store witness completion without invocation')
            invocation = pending.pop(key)
            require(event['cell'] == invocation['cell'], 'store witness cell changed')
            invoked, sent, finished = invocation['invoke_seconds'], event['sent_seconds'], event['complete_seconds']
            require(all(number(t) for t in (invoked, sent, finished)) and 0 <= invoked <= sent <= finished, 'invalid store witness clock')
            if event['outcome'] == 'ambiguous':
                require(key in resolutions, 'ambiguous store witness requires a recovery drill')
                recovered = resolutions[key]
                require(recovered['cell'] == invocation['cell'] and recovered['command_id'] == invocation['command_id'], 'recovered witness identity differs')
                commands.append((began + invoked, began + sent, recovered['first_acknowledged_monotonic_seconds'],
                                 invocation['cell'], invocation['command_id']))
                ambiguous_witnesses.add(invocation['command_id'])
            elif event['status'] == 200:
                require(event['outcome'] == 'completed' and not event.get('violation')
                        and event['response']['durability'] == durability, 'store witness result differs')
                commands.append((began + invoked, began + sent, began + finished,
                                 invocation['cell'], invocation['command_id']))
    require(not pending, 'incomplete store witness commands')
    commands.sort()
    starts = [item[0] for item in commands]
    witnesses = []
    for cycle, (held, release) in enumerate(windows):
        # A fresh invocation after observed pause cannot refer to a pre-outage
        # archived result. Actual sends must also fall inside the observed hold.
        first, last = bisect.bisect_left(starts, held), bisect.bisect_left(starts, release)
        overlap = [item for item in commands[first:last] if item[1] < release]
        require(overlap, 'no successful new command submitted during store window')
        during = [item for item in overlap if item[2] < release]
        if scenario != 'healthy' and requires_store:
            require(not during, 'object-store acknowledgement returned while store was paused')
        witnesses.append({'cycle': cycle, 'submitted_during_hold': len(overlap),
                          'acknowledged_during_hold': len(during),
                          'commands': [{'cell': cell, 'command_id': command_id, 'ack_after_release_started': finished >= release}
                                       for _, _, finished, cell, command_id in overlap]})
    if scenario in ('s3-recovery', 's3-fenced-recovery'):
        require(any(command['command_id'] in ambiguous_witnesses for window in witnesses for command in window['commands']),
                'no ambiguous command was dispatched in the observed store pause')
    recovered = report['store_recovery_verified_monotonic_seconds']
    elapsed = report['measurement']['elapsed_seconds']
    require(number(elapsed) and elapsed >= profile['timing']['measurement_seconds']
            and number(recovered) and recovered >= began + elapsed, 'store recovery predates workload completion')
    recovery = recovered - windows[-1][1]
    require(0 <= recovery <= profile['slo']['max_recovery_seconds'], 'store recovery exceeded declared bound')
    return {'cycles': len(offsets), 'target': 'object-store', 'container_id': identity,
            'object_store_required_at_ack': requires_store, 'witnesses': witnesses,
            'recovery_upper_seconds': recovery,
            'scope': 'Bounded RustFS process pauses; fresh serialized commands observed in each window. '
                     'Final adapter state checks bound recovery. No crash, packet-loss, Amazon S3, '
                     'causal latency or equivalent-contract claim.'}
