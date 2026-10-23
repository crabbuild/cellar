"""Owned process-pause experiments and independent slow-follower evidence checks.

The target also leads other groups. Only third-voter cells are used to witness
healthy-majority progress; all role cohorts retain their own workload results.
Replication samples are observations, never SDK acknowledgement proofs.
"""
import bisect
import json
import math
from pathlib import Path
import secrets
import threading
import time

from raft_topology import members
from raft_workload import Ledger, input_digest

LABEL = 'io.cellar.mr14.run'


def owned_state(docker, node, run_id, *, allow_stopped=False):
    template = '{"owner":{{json (index .Config.Labels "' + LABEL + '")}},"running":{{.State.Running}},"paused":{{.State.Paused}}}'
    state = json.loads(docker('inspect', '--format', template, node))
    require(state['owner'] == run_id and type(state['running']) is bool and type(state['paused']) is bool
            and (allow_stopped or state['running']), 'fault target ownership or lifecycle changed')
    return state


def preflight_pause(docker, image, control_factory=None):
    """Prove freezer support before creating the workload network/store/nodes."""
    require(docker('info', '--format', '{{.OSType}}') == 'linux', 'process pause requires a Linux Docker engine')
    run_id = secrets.token_hex(12)
    node = 'cellar-mr14-preflight-' + run_id
    control = control_factory(node, run_id) if control_factory else docker
    observed = {}
    created = False
    try:
        docker('run', '-d', '--name', node, '--label', f'{LABEL}={run_id}', '--network', 'none',
               '--cpus', '.1', '--memory', '16m', '--memory-swap', '16m', '--entrypoint', '/bin/sleep', image, '300')
        created = True
        observed['before'] = owned_state(control, node, run_id)
        require(observed['before']['paused'] is False, 'preflight target already paused')
        control('pause', node)
        observed['held'] = owned_state(control, node, run_id)
        require(observed['held']['paused'] is True, 'kernel pause capability unavailable')
    finally:
        if control_factory:
            control.close()
        # A lost creation/pause reply can still have changed the engine. Match
        # both the unpredictable name and owner before cleanup, including exits.
        filters = ('--filter', f'label={LABEL}={run_id}')
        if created or docker('ps', '-aq', *filters):
            state = owned_state(docker, node, run_id, allow_stopped=True)
            if state['paused']:
                docker('unpause', node)
            observed['after'] = owned_state(docker, node, run_id, allow_stopped=True)
            require(observed['after']['paused'] is False, 'preflight target remains paused')
            docker('rm', '-f', node)
        require(not docker('ps', '-aq', *filters), 'preflight target remains')
    return {'container': node, 'run_id': run_id, 'observations': observed, 'removed': True}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_fault(config, profile):
    require(isinstance(config, dict) and set(config) == {
        'format', 'node', 'pause_ms', 'max_pause_ms', 'interval_ms',
        'start_after_seconds', 'stop_before_seconds', 'max_start_lateness_ms'}, 'invalid fault profile fields')
    require(config['format'] == 1, 'unsupported fault profile')
    for key, value in config.items():
        require(type(value) is int and 0 <= value <= 3600000, 'invalid fault profile integer: ' + key)
    require(1 <= config['node'] <= profile['topology']['nodes'], 'invalid fault target')
    require(10 <= config['pause_ms'] < config['max_pause_ms'] < config['interval_ms'], 'invalid pause bounds')
    require(config['max_pause_ms'] < profile['timing']['request_timeout_ms'], 'pause exceeds request budget')
    probes = [cell for cell in range(profile['cells']['active'])
              if members(cell, profile['topology']['nodes'])[2] == config['node']]
    require(1 <= len(probes) <= 32, 'fault needs one to 32 active third-voter probe cells')
    end = profile['timing']['measurement_seconds'] - config['stop_before_seconds']
    start = config['start_after_seconds']
    count = math.floor((end - start - config['max_pause_ms'] / 1000) / (config['interval_ms'] / 1000)) + 1
    require(1 <= count <= 1000, 'fault schedule has no cycles or exceeds the observation bound')
    return probes, [start + n * config['interval_ms'] / 1000 for n in range(count)]


def validate_scenario(scenario, config, profile):
    from store_faults import is_store_fault, validate_store_scenario
    if scenario in ('s3-outage', 's3-recovery', 's3-fenced-recovery') or is_store_fault(config):
        return validate_store_scenario(scenario, config, profile)
    require(scenario in ('healthy', 'slow-follower'), 'fault capability not implemented: refuse before creating resources')
    if config is None:
        require(scenario == 'healthy', 'slow-follower requires a fault profile')
    else:
        validate_fault(config, profile)


class SlowFollower:
    """One joined owner; an ambiguous pause still triggers inspected cleanup."""
    def __init__(self, root, scenario, config, profile, prefix, run_id, docker, probe, close_control=None):
        self.probes, self.offsets = validate_fault(config, profile)
        self.scenario, self.config = scenario, config
        self.node = f"{prefix}-node{config['node']}"
        self.run_id, self.docker, self.probe = run_id, docker, probe
        self.close_control = close_control
        self.ledger = Ledger(Path(root) / 'faults.jsonl')
        self.stop = threading.Event()
        self.thread = None
        self.failure = None

    def inspect(self):
        return owned_state(self.docker, self.node, self.run_id)

    def start(self, began):
        require(self.thread is None, 'fault owner already started')
        self.thread = threading.Thread(target=self.drive, args=(began,))
        self.thread.start()

    def record(self, cycle, event, **values):
        self.ledger.append({'cycle': cycle, 'event': event, 'monotonic_seconds': time.monotonic(), **values})

    def drive(self, began):
        try:
            for cycle, offset in enumerate(self.offsets):
                if self.stop.wait(max(0, began + offset - time.monotonic())):
                    raise ValueError('fault schedule cancelled')
                require((time.monotonic() - began - offset) * 1000 <= self.config['max_start_lateness_ms'], 'fault schedule was late')
                before = self.inspect()
                require(before['paused'] is False, 'fault target already paused')
                self.record(cycle, 'begin', scheduled_seconds=offset, state=before)
                requested = time.monotonic()
                try:
                    if self.scenario == 'slow-follower':
                        self.docker('pause', self.node)
                    during = self.inspect()
                    require(during['paused'] is (self.scenario == 'slow-follower'), 'pause was not observed')
                    self.record(cycle, 'held', state=during)
                    if self.stop.wait(self.config['pause_ms'] / 1000):
                        raise ValueError('pause cancelled')
                    for cell in self.probes:
                        remaining = requested + self.config['max_pause_ms'] / 1000 - time.monotonic()
                        require(remaining > .05, 'pause observation exhausted its time budget')
                        timeout = min(.1, remaining - .05)
                        started = time.monotonic()
                        try:
                            code, reply = self.probe(cell, timeout)
                        except Exception as error:
                            # Record the failed read before cleanup can obscure
                            # its cell/budget. It is never retried or qualified.
                            self.record(cycle, 'probe_failed', cell=cell,
                                        started_monotonic_seconds=started, timeout_seconds=timeout,
                                        elapsed_seconds=time.monotonic() - started,
                                        error_type=type(error).__name__)
                            raise
                        self.record(cycle, 'probe', cell=cell, started_monotonic_seconds=started, status=code, response=reply)
                    release_state = self.inspect()
                    self.record(cycle, 'release', state=release_state)
                    require(release_state['paused'] is (self.scenario == 'slow-follower'), 'pause changed before release')
                finally:
                    # A failed Docker response does not establish whether pause
                    # happened. Inspect and unpause only the exact owned target.
                    state = self.inspect()
                    if state['paused']:
                        self.docker('unpause', self.node)
                    after = self.inspect()
                    require(after['paused'] is False, 'fault target remains paused')
                    self.record(cycle, 'released', state=after)
        except Exception as error:
            self.failure = str(error)
            self.record(-1, 'failure', error=self.failure)

    def finish(self):
        if self.thread:
            self.thread.join()
        require(self.failure is None, self.failure or 'fault failed')

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join()
        if self.close_control:
            self.close_control()
        self.ledger.close()


def verify_fault(report, events, operations, *, recovery=None):
    """Reject missing injection, wrong scope, fabricated progress or no recovery."""
    scenario, config, profile = report['scenario'], report['fault_profile'], report['profile']
    from store_faults import is_store_fault, verify_store_fault
    if scenario in ('s3-outage', 's3-recovery', 's3-fenced-recovery') or is_store_fault(config):
        return verify_store_fault(report, events, operations, recovery=recovery)
    validate_scenario(scenario, config, profile)
    require(report['fault_profile_sha256'] == input_digest(config), 'changed fault profile')
    if config is None:
        require(not events, 'healthy run contains undeclared fault observations')
        return None
    probes, offsets = validate_fault(config, profile)
    began = report['measurement']['started_monotonic_seconds']
    require(type(began) in (int, float) and math.isfinite(began) and began > 0, 'missing workload clock origin')
    cycles = {}
    previous = 0
    previous_cycle = 0
    for order, event in enumerate(events, 1):
        stamp = event['monotonic_seconds']
        require(type(stamp) in (int, float) and math.isfinite(stamp) and stamp >= previous and event['order'] == order, 'fault clock or event order changed')
        require(type(event['cycle']) is int and 0 <= event['cycle'] < len(offsets), 'missing/failed fault cycle')
        require(event['cycle'] >= previous_cycle, 'overlapping fault cycles')
        previous_cycle = event['cycle']
        previous = stamp
        cycles.setdefault(event['cycle'], []).append(event)
    require(set(cycles) == set(range(len(offsets))), 'incomplete fault schedule')
    invokes, completed = {}, {cell: [] for cell in probes}
    for event in operations:
        if event.get('phase') != 'measurement' or event['cell'] not in completed or event['operation'] != 'command':
            continue
        if event['event'] == 'invoke':
            invokes[event['id']] = event
        elif event['event'] == 'complete':
            require(event['id'] in invokes, 'fault completion without invocation')
            invocation = invokes.pop(event['id'])
            require(invocation['cell'] == event['cell'], 'fault command identity mismatch')
            if event['status'] != 200:
                continue
            invoked, finished = invocation['invoke_seconds'], event['complete_seconds']
            require(all(type(t) in (int, float) and math.isfinite(t) for t in (invoked, finished)) and 0 <= invoked <= finished, 'invalid command witness clock')
            receipt = {key: event['response'][key] for key in ('group', 'incarnation', 'term', 'index')}
            completed[event['cell']].append((began + invoked, began + finished, invocation['command_id'], receipt))
    require(not invokes, 'incomplete fault witness commands')
    starts = {}
    for cell, commands in completed.items():
        commands.sort(key=lambda command: command[0])
        starts[cell] = [command[0] for command in commands]
    witnesses, lag_samples = [], []
    for cycle, rows in cycles.items():
        require([row['event'] for row in rows] == ['begin', 'held'] + ['probe'] * len(probes) + ['release', 'released'], 'incomplete fault cycle observations')
        begin, held, release, released = rows[0], rows[1], rows[-2], rows[-1]
        for row, paused in ((begin, False), (held, scenario == 'slow-follower'), (release, scenario == 'slow-follower'), (released, False)):
            state = row['state']
            require(set(state) == {'owner', 'running', 'paused'} and state['owner'] == report['run_id'] and state['running'] is True and state['paused'] is paused, 'fault ownership or physical pause differs')
        require(begin['scheduled_seconds'] == offsets[cycle], 'fault schedule changed')
        late = (begin['monotonic_seconds'] - began - offsets[cycle]) * 1000
        require(0 <= late <= config['max_start_lateness_ms'], 'fault schedule outside declared window')
        require((release['monotonic_seconds'] - held['monotonic_seconds']) * 1000 >= config['pause_ms'], 'pause shorter than declared')
        require((released['monotonic_seconds'] - begin['monotonic_seconds']) * 1000 <= config['max_pause_ms'], 'pause exceeded declared bound')
        for expected_cell, observation in zip(probes, rows[2:-2]):
            cell, response = observation['cell'], observation['response']
            require(cell == expected_cell and observation['status'] == 200, 'missing leader probe')
            require(held['monotonic_seconds'] <= observation['started_monotonic_seconds'] <= observation['monotonic_seconds'] <= release['monotonic_seconds'], 'probe outside pause window')
            assigned = members(cell, profile['topology']['nodes'])
            require(response['cell'] == cell and response['node'] == assigned[0] and response['leader'] == assigned[0] and response['role'] == 'Leader', 'probe is not the expected leader')
            progress = response['replication']
            require(progress['term'] == response['term'], 'incoherent replication term')
            peers = progress['members']
            require(len(peers) == 3 and {p['member'] for p in peers} == set(assigned) and all(type(p['member']) is int and p['voter'] is True for p in peers), 'probe voter set changed')
            require(type(progress['committed']) is int and progress['committed'] >= 0 and all(type(p['matched']) is int and p['matched'] >= 0 for p in peers), 'invalid replication positions')
            matched = {p['member']: p['matched'] for p in peers}
            lag = max(0, progress['committed'] - matched[config['node']])
            lag_samples.append({'cycle': cycle, 'cell': cell, 'entries': lag})
            first = bisect.bisect_left(starts[cell], held['monotonic_seconds'])
            last = bisect.bisect_right(starts[cell], observation['started_monotonic_seconds'])
            for invoked, finished, command_id, receipt in completed[cell][first:last]:
                if not held['monotonic_seconds'] <= invoked <= finished <= observation['started_monotonic_seconds']:
                    continue
                require(receipt['group'] == progress['group'] and receipt['incarnation'] == progress['incarnation'] and receipt['term'] == progress['term'], 'fault witness history or term changed')
                index = receipt['index']
                if matched[config['node']] < index <= progress['committed'] and all(matched[node] >= index for node in assigned[:2]):
                    witnesses.append({'cycle': cycle, 'cell': cell, 'command_id': command_id, 'index': index})
    if scenario == 'slow-follower':
        require(witnesses, 'no observed healthy-majority command completed while the third voter lagged')
    # Start before the unpause request. The later confirmation timestamp would
    # understate actual recovery time by the Docker/inspection round trip.
    last_release = cycles[len(offsets) - 1][-2]['monotonic_seconds']
    recovered = report['replicas_verified_monotonic_seconds']
    require(recovered >= began + report['measurement']['elapsed_seconds'], 'replica recovery observation predates workload completion')
    recovery = recovered - last_release
    require(math.isfinite(recovery) and 0 <= recovery <= profile['slo']['max_recovery_seconds'], 'replica recovery exceeded declared bound')
    cohorts = {role: [] for role in ('target_leader', 'target_query_follower', 'target_third_voter', 'unaffected')}
    for cell in range(profile['cells']['active']):
        assigned = members(cell, profile['topology']['nodes'])
        role = ('target_leader', 'target_query_follower', 'target_third_voter')[assigned.index(config['node'])] if config['node'] in assigned else 'unaffected'
        cohorts[role].append(cell)
    return {'cycles': len(offsets), 'target_node': config['node'], 'cohorts': cohorts,
            'healthy_majority_witnesses': witnesses, 'lag_samples': lag_samples,
            'recovery_upper_seconds': recovery,
            'scope': 'Bounded whole-process pauses. Cohorts use planned placement; witness probes observe the actual leader. Recovery is bounded by final exact replica reads, not inferred from an election.'}


def fault_evidence(root, report, summarize, decode=json.loads):
    path = Path(root) / 'faults.jsonl'
    events = [decode(line) for line in path.read_text().splitlines()] if path.exists() else []
    from workload_recovery import recovery_evidence
    recovery, _ = recovery_evidence(root, report)
    require(recovery == report.get('recovery'), 'recovery summary differs from raw observations')
    with (Path(root) / 'operations.jsonl').open() as stream:
        result = verify_fault(report, events, (decode(line) for line in stream), recovery=recovery)
    if result is not None and 'cohorts' in result:
        result['cohort_summaries'] = {role: summarize(Path(root) / 'operations.jsonl', 'measurement', report['profile']['timing']['measurement_seconds'], cells=set(cells))
                                      for role, cells in result['cohorts'].items() if cells}
    return result
