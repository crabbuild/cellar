# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Source-stable completion windows, separate from controller-clock recovery.

Durations can overlap and include operations started before a capture. A new
host has a lifetime observation, never a fabricated zero baseline. These values
describe APIs and sockets, not physical devices or independent durability proof.
"""
import re

from raft_metrics import decimal, delta, snapshot

PHASES = ('before_fault', 'seed_started', 'seed_installed', 'first_write_verified',
          'payloads_verified', 'before_cleanup', 'after_cleanup')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def phase_work(windows, events=None):
    require(type(windows) is list and len(windows) == len(PHASES), 'incomplete observation windows')
    previous, previous_phase, finished = {}, None, 0
    stamps = {row['name']: row['elapsed_us'] for row in events} if events is not None else None
    result, overhead, unavailable = [], [], []
    previous_unavailable = {}
    for phase, window in zip(PHASES, windows):
        require(type(window) is dict and set(window) == {
            'phase', 'capture_started_us', 'capture_finished_us', 'new_sources', 'hosts'},
            'invalid observation window')
        require(window['phase'] == phase, 'observation phase sequence differs')
        start, end = window['capture_started_us'], window['capture_finished_us']
        require(type(start) is int and type(end) is int and finished <= start <= end < 2**64,
                'invalid observation controller clock')
        finished = end
        if stamps is not None:
            bounds = {
                'before_fault': (0, stamps['fault']),
                'seed_started': (stamps['seed_started'], stamps['seed_installed']),
                'seed_installed': (stamps['seed_installed'], stamps['hydrated']),
                'first_write_verified': (stamps['first_write_verified'], stamps['receipt_verified']),
                'payloads_verified': (stamps['payloads_verified'], stamps['cleanup_joined']),
                'before_cleanup': (stamps['payloads_verified'], stamps['cleanup_joined']),
                'after_cleanup': (stamps['payloads_verified'], stamps['cleanup_joined']),
            }
            lower, upper = bounds[phase]
            require(lower <= start <= end <= upper, 'capture is outside its controller phase')
        overhead.append({'phase': phase, 'capture_us': end - start})
        require(type(window['hosts']) is list and window['hosts'], 'missing host observations')
        hosts, missing = {}, {}
        for raw in window['hosts']:
            require(type(raw) is dict and set(raw) == {
                'source', 'available', 'saturated', 'sampled_at_us', 'counters', 'operations'},
                'invalid host observation envelope')
            source = raw['source']
            require(type(source) is str and re.fullmatch('[0-9a-f]{32}', source), 'invalid host startup source')
            require(source not in hosts, 'duplicate host startup source')
            require(type(raw['available']) is bool and type(raw['saturated']) is bool,
                    'invalid observation availability')
            if raw['sampled_at_us'] is not None:
                decimal(raw['sampled_at_us'])
            if not raw['available'] or raw['saturated'] or raw['sampled_at_us'] is None:
                missing[source] = ('owner clock unavailable' if raw['sampled_at_us'] is None
                                   else 'unavailable or saturated owner observation')
                unavailable.append({'phase': phase, 'source': source, 'reason': missing[source]})
            else:
                snapshot(raw)
            hosts[source] = raw
        require(set(previous) <= set(hosts), 'retained host observation disappeared')
        introduced = sorted(set(hosts) - set(previous))
        require(window['new_sources'] == introduced, 'new host startup sources differ')
        require(not introduced or phase in ('before_fault', 'seed_installed'),
                'unexpected host startup during recovery')
        if previous_phase is not None:
            work = []
            for source, raw in sorted(hosts.items()):
                if source in missing:
                    work.append({'source': source, 'coverage': 'unavailable', 'reason': missing[source]})
                    continue
                if source in previous and source not in previous_unavailable:
                    coverage = 'completed_operation_window'
                    measured = delta(previous[source], raw)
                else:
                    coverage = 'resumed_host_lifetime' if source in previous else 'new_host_lifetime'
                    measured = snapshot(raw)
                work.append({'source': source, 'coverage': coverage, 'work': measured})
            result.append({'from': previous_phase, 'to': phase, 'hosts': work})
        previous, previous_phase, previous_unavailable = hosts, phase, missing
    return {'windows': result, 'capture_overhead': overhead, 'unavailable_samples': unavailable,
            'interpretation': 'Overlapping completion observations; do not sum durations into RTO.'}
