"""Post-measurement recovery of quarantined commands using their original IDs.

The original ledger is immutable. Two successful requests must return the same
receipt/result before a logical resolution can enter the state checker. The
first retry can either observe an earlier commit or execute a previously absent
command; this drill does not claim which happened before the lost response.
"""
import hashlib
import json
import math
from pathlib import Path
import time

from raft_workload import Ledger, input_digest, model


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value):
    return type(value) in (int, float) and math.isfinite(value)


def read_events(path):
    with Path(path).open() as stream:
        return [json.loads(line) for line in stream]


def ambiguous_commands(events, report):
    """Validate request identity and quarantine before any recovery mutation."""
    seeds, states, pending, busy, blocked, seen, ambiguous = {}, {}, {}, set(), set(), set(), {}
    for order, event in enumerate(events, 1):
        require(type(event['order']) is int and event['order'] == order, 'original ledger order changed')
        kind, cell = event['event'], event['cell']
        require(type(cell) is int and 0 <= cell < report['profile']['cells']['total'], 'invalid recovery cell')
        client = hashlib.sha256(f"{report['run_id']}:{cell}".encode()).hexdigest()[:32]
        if kind == 'seed_invoke':
            require(cell not in seeds and cell not in states and event['command_id'] == client + ':1'
                    and input_digest(event['input']) == event['input_sha256'], 'recovery seed identity differs')
            seeds[cell] = event
        elif kind == 'seed_complete':
            require(cell in seeds and event['status'] == 200, 'unacknowledged recovery seed')
            expected, changed = model(None, seeds.pop(cell)['input'])
            reply = event['response']
            require(reply['state'] == expected and reply['changed'] == changed
                    and reply['durability'] == report['profile']['durability'], 'recovery seed result differs')
            states[cell] = {'state': expected, 'next_sequence': 2, 'reply': reply}
        elif kind in ('invoke', 'reject'):
            key = event['id']
            require(key not in seen and cell in states, 'duplicate recovery offer or missing seed')
            seen.add(key)
            if kind == 'reject':
                continue
            require(cell not in busy and cell not in blocked, 'work admitted to a busy or quarantined cell')
            require(event['operation'] in ('command', 'query') and input_digest(event['input']) == event['input_sha256'], 'recovery operation or input differs')
            if event['operation'] == 'command':
                require(event['command_id'] == f"{client}:{states[cell]['next_sequence']}", 'recovery command sequence differs')
            busy.add(cell)
            pending[key] = event
        elif kind == 'complete':
            require(event['id'] in pending, 'recovery completion has no invocation')
            invocation = pending.pop(event['id'])
            require(cell == invocation['cell'] and event['operation'] == invocation['operation']
                    and not event.get('violation'), 'recovery completion identity or violation')
            busy.remove(cell)
            if event['outcome'] == 'ambiguous':
                require(event['operation'] == 'command' and event['phase'] == invocation['phase'] == 'measurement'
                        and event['status'] != 200, 'unsupported ambiguous operation')
                deadline = invocation['retry_until_ms']
                require(type(deadline) is int and deadline > 0, 'invalid original retry deadline')
                expected, changed = model(states[cell]['state'], invocation['input'])
                ambiguous[event['id']] = {'invocation': invocation, 'completion': event,
                                         'expected': expected, 'changed': changed, 'before': states[cell]['reply']}
                blocked.add(cell)
            elif event['status'] == 200:
                require(event['outcome'] == 'completed', 'original successful outcome differs')
                if event['operation'] == 'command':
                    current, reply = states[cell], event['response']
                    expected, changed = model(current['state'], invocation['input'])
                    require(reply['state'] == expected and reply['changed'] == changed
                            and reply['durability'] == report['profile']['durability'], 'original command transition differs')
                    current.update(state=expected, next_sequence=current['next_sequence'] + 1, reply=reply)
            else:
                require(event['outcome'] in ('rejected', 'failed'), 'original outcome differs')
        else:
            raise ValueError('unknown original recovery event')
    require(not seeds and not pending and not busy, 'original workload has unfinished requests')
    require(ambiguous, 'recovery scenario observed no ambiguous commands')
    declared = report['measurement']
    require(isinstance(declared['blocked_cells'], list) and all(type(cell) is int for cell in declared['blocked_cells'])
            and len(declared['blocked_cells']) == len(blocked) and set(declared['blocked_cells']) == blocked
            and type(declared['ambiguous']) is int and declared['ambiguous'] == len(ambiguous), 'recorded ambiguity/quarantine differs')
    return ambiguous


def recovery_window(root, report):
    require(report['scenario'] == 's3-recovery', 'undeclared command recovery')
    phase = report['measurement']
    require(number(phase['started_monotonic_seconds']) and number(phase['elapsed_seconds'])
            and phase['elapsed_seconds'] >= report['profile']['timing']['measurement_seconds'], 'incomplete measurement before recovery')
    faults = read_events(Path(root) / 'faults.jsonl')
    require(len(faults) >= 4 and faults[-2]['event'] == 'release' and faults[-1]['event'] == 'released', 'store has no confirmed release')
    release, released = faults[-2]['monotonic_seconds'], faults[-1]['monotonic_seconds']
    require(number(release) and number(released) and release <= released
            and faults[-1]['state'] == {'owner': report['run_id'], 'running': True, 'paused': False}, 'store release differs')
    return (max(released, phase['started_monotonic_seconds'] + phase['elapsed_seconds']),
            release + report['profile']['slo']['max_recovery_seconds'])


def check_reply(reply, command, report):
    invocation, before = command['invocation'], command['before']
    require(input_digest(reply['state']) == input_digest(command['expected']) and type(reply['changed']) is bool and reply['changed'] == command['changed']
            and reply['durability'] == report['profile']['durability'], 'recovered command result or durability differs')
    if report.get('adapter', 'raft') == 'raft':
        require(reply['group'] == before['group'] and reply['incarnation'] == before['incarnation']
                and type(reply['index']) is int and reply['index'] > before['index']
                and type(reply['term']) is int and reply['term'] >= before['term']
                and isinstance(reply['version_token'], str) and bool(reply['version_token']), 'recovered Raft position or scope differs')
    else:
        from baseline_history import check_command
        check_command(reply, invocation, report['run_id'], report['profile']['durability'],
                      int(invocation['command_id'].split(':')[1]))


def recovery_evidence(root, report):
    """Return verified logical completions *after* the measured operation log."""
    root = Path(root)
    if report['scenario'] != 's3-recovery':
        require(not (root / 'recovery.jsonl').exists(), 'undeclared recovery ledger')
        return None, []
    operations = read_events(root / 'operations.jsonl')
    commands = ambiguous_commands(operations, report)
    start, end = recovery_window(root, report)
    events = read_events(root / 'recovery.jsonl')
    require(len(events) == 4 * len(commands), 'incomplete recovery/confirmation ledger')
    previous = start
    request_seconds = (report['profile']['timing']['request_timeout_ms'] + 1000) / 1000
    resolved, summaries = [], []
    for original_id, command in commands.items():
        invocation = command['invocation']
        first_reply = None
        first_complete = None
        for attempt in (1, 2):
            offset = 4 * len(resolved) + 2 * (attempt - 1)
            called, completed = events[offset:offset + 2]
            for n, (event, kind) in enumerate(((called, 'invoke'), (completed, 'complete')), offset + 1):
                require(type(event['order']) is int and event['order'] == n and event['event'] == kind
                        and event['original_id'] == original_id and event['cell'] == invocation['cell']
                        and type(event['attempt']) is int and event['attempt'] == attempt, 'recovery event identity or order differs')
            require(all(called[key] == invocation[key] for key in ('command_id', 'retry_until_ms', 'input', 'input_sha256'))
                    and input_digest(called['input']) == called['input_sha256'], 'recovery changed original request identity')
            invoked, sent, finished = called['monotonic_seconds'], completed['sent_monotonic_seconds'], completed['monotonic_seconds']
            require(all(number(t) for t in (invoked, sent, finished)) and previous <= invoked <= sent <= finished <= end
                    and sent + request_seconds <= end, 'recovery outside its declared window')
            for wall in (called['wall_ms'], completed['sent_wall_ms']):
                require(type(wall) is int and wall >= 0 and wall + request_seconds * 1000 < invocation['retry_until_ms'], 'recovery renewed or exceeded retry deadline')
            require(completed['status'] == 200 and completed['outcome'] == 'completed', 'recovery attempt remains unresolved')
            reply = completed['response']
            check_reply(reply, command, report)
            if attempt == 1:
                first_reply, first_complete = reply, finished
            else:
                require(reply == first_reply, 'confirmation changed the recovered receipt or result')
            previous = finished
        resolved.append({'event': 'recovered', 'order': len(operations) + len(resolved) + 1,
                         'id': original_id, 'cell': invocation['cell'], 'operation': 'command', 'phase': 'recovery',
                         'status': 200, 'outcome': 'completed', 'response': first_reply,
                         'confirmed_monotonic_seconds': previous})
        summaries.append({'original_id': original_id, 'cell': invocation['cell'], 'command_id': invocation['command_id'],
                          'receipt_sha256': input_digest(first_reply), 'first_acknowledged_monotonic_seconds': first_complete,
                          'confirmed_monotonic_seconds': previous})
    return {'original_ambiguous_commands': len(commands), 'resolved_commands': len(resolved),
            'recovery_requests': 2 * len(resolved), 'resolutions': summaries,
            'scope': 'Original outcomes remain ambiguous in measurement. Each original identity is resolved and confirmed after workload completion; its prior commit time is unknown.'}, resolved


def recover_commands(root, report, send, *, clock=time.monotonic, wall=time.time):
    """One recovery and one confirming request per identity; failures stop.

    No replacement ID, renewed retry deadline, blind error retry or admission
    of another operation on a quarantined cell is permitted.
    """
    root = Path(root)
    commands = ambiguous_commands(read_events(root / 'operations.jsonl'), report)
    start, end = recovery_window(root, report)
    # Native work has a five-second budget; the existing HTTP adapter allows
    # one extra second to observe that result. Do not overrun the outer bound.
    request_seconds = (report['profile']['timing']['request_timeout_ms'] + 1000) / 1000
    ledger = Ledger(root / 'recovery.jsonl')
    try:
        for original_id, command in commands.items():
            invocation = command['invocation']
            original = {key: invocation[key] for key in ('command_id', 'retry_until_ms', 'input', 'input_sha256')}
            headers = {key: invocation[key] for key in ('command_id', 'retry_until_ms')}
            first = None
            for attempt in (1, 2):
                now, wall_ms = clock(), int(wall() * 1000)
                require(start <= now and now + request_seconds <= end, 'recovery request would exceed its outer deadline')
                require(wall_ms + request_seconds * 1000 < invocation['retry_until_ms'], 'original retry deadline cannot cover recovery request')
                identity = {'original_id': original_id, 'cell': invocation['cell'], 'attempt': attempt}
                ledger.append({**identity, **original, 'event': 'invoke', 'monotonic_seconds': now, 'wall_ms': wall_ms})
                sent = clock()
                sent_wall_ms = int(wall() * 1000)
                # Durable invocation logging can consume time. Recheck both
                # bounds immediately before handing the request to the adapter.
                require(start <= sent and sent + request_seconds <= end, 'journal sync exhausted recovery request window')
                require(sent_wall_ms + request_seconds * 1000 < invocation['retry_until_ms'], 'journal sync exhausted original retry deadline')
                try:
                    status, reply = send(invocation['cell'], 'command', invocation['input'], headers)
                except Exception as error:
                    status, reply = None, {'transport_error': type(error).__name__}
                finished = clock()
                ledger.append({**identity, 'event': 'complete', 'status': status, 'response': reply,
                               'outcome': 'completed' if status == 200 else 'ambiguous',
                               'sent_monotonic_seconds': sent, 'sent_wall_ms': sent_wall_ms, 'monotonic_seconds': finished})
                require(status == 200, 'original identity recovery remains unresolved; retain its ledger')
                check_reply(reply, command, report)
                require(finished <= end, 'recovery exceeded its outer deadline')
                if attempt == 1:
                    first = reply
                else:
                    require(reply == first, 'confirmation changed the recovered receipt or result')
    finally:
        ledger.close()
    return recovery_evidence(root, report)


def apply_resolutions(state, resolutions, position):
    """Call only after the adapter history checker accepts the logical results."""
    for event in resolutions:
        current = state[event['cell']]
        require(current.get('blocked') is True, 'recovery cell was not quarantined')
        current.update(state=event['response']['state'], next_sequence=current['next_sequence'] + 1,
                       blocked=False, **position(event['response']))
