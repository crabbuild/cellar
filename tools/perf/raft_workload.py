"""Bounded open-loop workload and fsynced per-run operation ledger.

The scheduler keeps the original arrival schedule under load. No mutation is
retried. An ambiguous cell is quarantined; it cannot receive a new sequence.
"""
import concurrent.futures
import hashlib
import json
import math
import os
import random
import threading
import time


class Ledger:
    def __init__(self, path):
        self.stream = open(path, 'x', encoding='utf-8')
        self.lock = threading.Lock()
        self.order = 0

    def append(self, record):
        with self.lock:
            self.order += 1
            self.stream.write(json.dumps({**record, 'order': self.order}, sort_keys=True) + '\n')
            self.stream.flush()
            os.fsync(self.stream.fileno())
            return self.order

    def close(self):
        self.stream.close()


def input_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def arrival_count(seconds, rate):
    return math.ceil(seconds * rate)


def model(state, operation):
    """Independent noncommutative reference transition, including failed CAS."""
    if operation['kind'] == 'initialize':
        if state is not None:
            raise ValueError('cell already initialized')
        return {'revision': 0, 'left': 1000000, 'right': 0, 'payload_sha256': hashlib.sha256(b'').hexdigest()}, True
    if state is None:
        raise ValueError('uninitialized cell')
    if state['revision'] != operation['expected_revision']:
        return dict(state), False
    left = operation['value'] if operation['kind'] == 'set' else state['left'] - operation['value']
    if not 0 <= left <= 1000000:
        return dict(state), False
    return {'revision': state['revision'] + 1, 'left': left, 'right': 1000000 - left,
            'payload_sha256': hashlib.sha256(operation['payload'].encode()).hexdigest()}, True


def run_load(profile, phase, seconds, state, ledger, send, run_id, *, clock=time.monotonic, sleep=time.sleep):
    """send(cell, command|query, body, headers) -> (HTTP status, decoded body).

    Invocation records reach disk before send. The bounded executor has no
    unbounded input queue: it receives at most max_inflight unfinished work.
    Per-cell serialization makes acknowledged transition verification exact.
    """
    arrival = profile['arrival']
    rate = arrival['rate_per_second']
    limit = arrival['max_inflight']
    rng = random.Random(f"{profile['seed']}:{phase}")
    active = profile['cells']['active']
    hot = max(1, math.ceil(active * profile['skew']['hot_fraction']))
    lock = threading.Lock()
    busy, blocked = set(), {cell for cell, value in state.items() if value.get('blocked')}
    pending = set()
    began = clock()
    offered = arrival_count(seconds, rate)
    counters = {key: 0 for key in ('offered', 'submitted', 'completed', 'rejected', 'ambiguous', 'failed', 'timed_out', 'generator_rejected')}

    def execute(item, headers, body):
        cell = item['cell']
        started = clock()
        ledger.append({**item, 'event': 'invoke', 'invoke_seconds': started - began,
                       'input': body, 'input_sha256': input_digest(body), **headers})
        # Fsync time is included in schedule-to-completion latency. Keep wire
        # service time distinct; neither can hide generator scheduling delays.
        sent = clock()
        timed_out = False
        try:
            code, response = send(cell, item['operation'], body, headers)
        except Exception as error:
            timed_out = isinstance(error, TimeoutError) or isinstance(getattr(error, 'reason', None), TimeoutError)
            code, response = None, {'transport_error': type(error).__name__}
        completed = clock()
        outcome = 'completed' if code == 200 else 'rejected'
        if code is None or (isinstance(response, dict) and response.get('ambiguous')):
            outcome = 'ambiguous' if item['operation'] == 'command' else 'failed'
        violation = None
        with lock:
            if code == 200:
                if item['operation'] == 'command':
                    expected, changed = model(state[cell]['state'], body)
                    if response.get('state') != expected or response.get('changed') != changed or response.get('durability') != profile['durability']:
                        violation = 'command result or achieved durability differs from reference'
                    else:
                        state[cell]['state'] = expected
                        state[cell]['next_sequence'] += 1
                        state[cell]['version_token'] = response['version_token']
                        state[cell]['index'] = response['index']
                elif profile['read_consistency'] in ('at-least', 'linearizable') and response.get('state') != state[cell]['state']:
                    violation = 'query differs from the preceding acknowledged state'
            if outcome == 'ambiguous' or violation:
                blocked.add(cell)
                state[cell]['blocked'] = True
            counters[outcome] += 1
            counters['timed_out'] += int(timed_out)
        # Keep the cell reserved until its completion record is durable.
        ledger.append({**item, 'event': 'complete', 'outcome': outcome, 'status': code,
                       'response': response, 'violation': violation, 'timed_out': timed_out,
                       'complete_seconds': completed - began, 'wire_ms': (completed - sent) * 1000,
                       'dispatch_delay_ms': (started - began - item['scheduled_seconds']) * 1000,
                       'invocation_sync_ms': (sent - started) * 1000,
                       'latency_ms': (completed - began - item['scheduled_seconds']) * 1000})
        with lock:
            busy.remove(cell)
        if violation:
            raise ValueError(violation)

    with concurrent.futures.ThreadPoolExecutor(max_workers=limit) as executor:
        for index in range(offered):
            scheduled = index / rate
            sleep(max(0, began + scheduled - clock()))
            cell = rng.randrange(hot) if rng.random() < profile['skew']['hot_probability'] else rng.randrange(active)
            operation = 'query' if rng.random() < profile['read_fraction'] else 'command'
            # Draw the full offered operation before admission. A busy cell or
            # slow response must not change later RNG draws and workload skew.
            change = None
            if operation == 'command':
                payload = 'x' * rng.choice(profile['write_bytes'])
                change = {'kind': rng.choice(['transfer', 'set']),
                          'value': rng.randint(0, 1000000), 'payload': payload}
            item = {'phase': phase, 'id': f'{phase}:{index}', 'cell': cell,
                    'operation': operation, 'scheduled_seconds': scheduled}
            with lock:
                counters['offered'] += 1
                reason = ('scheduler_late' if (clock() - began - scheduled) * 1000 > arrival['max_lateness_ms'] else
                          'ambiguous_cell' if cell in blocked else
                          'cell_busy' if cell in busy else
                          'inflight_limit' if len(busy) >= limit else None)
                if reason is None:
                    busy.add(cell)
                    counters['submitted'] += 1
                    current = dict(state[cell])
                else:
                    counters['generator_rejected'] += 1
            if reason:
                ledger.append({**item, 'event': 'reject', 'reason': reason})
                continue
            headers = {'retry_until_ms': int(time.time() * 1000) + profile['timing']['retry_window_ms'],
                       'command_id': f"{hashlib.sha256(f'{run_id}:{cell}'.encode()).hexdigest()[:32]}:{current['next_sequence']}"}
            if operation == 'query':
                headers['min_version'] = current['version_token']
                body = {'consistency': profile['read_consistency']}
            else:
                body = {**change, 'expected_revision': current['state']['revision']}
            pending.add(executor.submit(execute, item, headers, body))
            done = {future for future in pending if future.done()}
            for future in done:
                future.result()
            pending.difference_update(done)
        sleep(max(0, began + seconds - clock()))
        for future in pending:
            future.result()
    return {**counters, 'offered_seconds': seconds, 'elapsed_seconds': clock() - began,
            'blocked_cells': sorted(blocked)}


def reconcile(path):
    """Replay this runner's per-cell serialized history from durable invocations.

    This is deliberately narrower than the MR-15 concurrent linearizability
    checker. A recorded ambiguity or overlapping per-cell invocation refuses
    reconciliation rather than guessing a valid history.
    """
    states, seeds, pending, busy, seen = {}, {}, {}, set(), set()
    order = 0
    commands = queries = 0
    with open(path) as stream:
        for line in stream:
            event = json.loads(line)
            order += 1
            if event['order'] != order:
                raise ValueError('ledger order gap')
            cell = event['cell']
            kind = event['event']
            if kind == 'seed_invoke':
                if cell in seeds or cell in states or input_digest(event['input']) != event['input_sha256']:
                    raise ValueError('invalid seed invocation')
                seeds[cell] = event
            elif kind == 'seed_complete':
                invocation = seeds.pop(cell)
                expected, changed = model(None, invocation['input'])
                reply = event['response']
                if event['status'] != 200 or reply['state'] != expected or reply['changed'] != changed:
                    raise ValueError('seed result mismatch')
                states[cell] = {'state': expected, 'index': reply['index'], 'next_sequence': 2,
                                'client': invocation['command_id'].split(':')[0], 'group': reply['group'],
                                'incarnation': reply['incarnation'], 'durability': reply['durability'],
                                'version_token': reply['version_token'], 'prefixes': {json.dumps(expected, sort_keys=True)}}
                commands += 1
            elif kind in ('invoke', 'reject'):
                key = event['id']
                if key in seen or cell not in states:
                    raise ValueError('duplicate offered identity or unseeded cell')
                seen.add(key)
                if kind == 'reject':
                    continue
                if cell in busy or input_digest(event['input']) != event['input_sha256']:
                    raise ValueError('overlapping cell invocation or changed input')
                busy.add(cell)
                pending[key] = event
            elif kind == 'complete':
                invocation = pending.pop(event['id'])
                if invocation['cell'] != cell or invocation['operation'] != event['operation'] or event.get('violation'):
                    raise ValueError('completion identity or recorded violation')
                busy.remove(cell)
                current = states[cell]
                if event['outcome'] == 'ambiguous':
                    raise ValueError('ambiguous history requires a separate reconciliation drill')
                if event['status'] != 200:
                    if event['outcome'] != 'rejected' and not (event['operation'] == 'query' and event['outcome'] == 'failed'):
                        raise ValueError('response outcome mismatch')
                    continue
                if event['outcome'] != 'completed':
                    raise ValueError('successful response outcome mismatch')
                reply = event['response']
                if reply['group'] != current['group'] or reply['incarnation'] != current['incarnation']:
                    raise ValueError('wrong history scope')
                if event['operation'] == 'command':
                    expected, changed = model(current['state'], invocation['input'])
                    if (invocation['command_id'] != f"{current['client']}:{current['next_sequence']}" or
                            reply['state'] != expected or reply['changed'] != changed or
                            reply['index'] <= current['index'] or reply['durability'] != current['durability']):
                        raise ValueError('command sequence, transition or durability mismatch')
                    current.update(state=expected, index=reply['index'], version_token=reply['version_token'], next_sequence=current['next_sequence']+1)
                    current['prefixes'].add(json.dumps(expected, sort_keys=True))
                    commands += 1
                else:
                    consistency = invocation['input']['consistency']
                    if consistency in ('at-least', 'linearizable'):
                        if reply['state'] != current['state'] or reply['index'] < current['index']:
                            raise ValueError('query does not cover preceding write')
                        if consistency == 'at-least' and invocation['min_version'] != current['version_token']:
                            raise ValueError('query minimum token mismatch')
                    elif json.dumps(reply['state'], sort_keys=True) not in current['prefixes']:
                        raise ValueError('stale query is not an acknowledged prefix')
                    queries += 1
            else:
                raise ValueError('unknown ledger event')
    if seeds or pending or busy:
        raise ValueError('incomplete ledger')
    return {'commands_including_seed': commands, 'queries': queries, 'cells': len(states),
            'final': {cell: {key: value for key, value in state.items() if key != 'prefixes'} for cell, state in states.items()}}
