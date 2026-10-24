# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Audit the acknowledged prefix before replay, then verify new post-restart writes."""
import hashlib
from pathlib import Path
import time

from baseline_history import check_command, position
from baseline_restart import SCENARIO, verify_restart
from raft_workload import Ledger, input_digest, model
from workload_recovery import inspect_history, number, read_events, recovery_window, require


def verify_audit(root, report):
    restart = verify_restart(root, report)
    if restart is None:
        return None
    commands, states = inspect_history(read_events(Path(root) / 'operations.jsonl'), report)
    by_cell = {command['invocation']['cell']: command for command in commands.values()}
    rows = read_events(Path(root) / 'before-recovery.jsonl')
    require(len(rows) == 3 * len(states), 'missing pre-retry owner reads')
    previous = restart['ready_monotonic_seconds']
    _, end = recovery_window(root, report)
    present = set()
    for order, row in enumerate(rows, 1):
        cell, ingress = divmod(order - 1, 3)
        require(row['order'] == order and row['cell'] == cell and row['ingress'] == ingress + 1
                and row['status'] == 200, 'invalid pre-retry owner read')
        began, finished = row['invoked_monotonic_seconds'], row['monotonic_seconds']
        require(number(began) and number(finished) and previous <= began <= finished <= end
                and began + 6 <= end, 'pre-retry audit outside recovery bound')
        previous = finished
        current, reply = states[cell], row['response']
        require(reply['scope'] == current['reply']['scope'] and reply['read_contract'] == 'owner'
                and reply['owner'] in ('node1', 'node2', 'node3') and type(reply['sequence']) is int, 'pre-retry read scope differs')
        if reply['sequence'] == current['next_sequence'] - 1:
            require(input_digest(reply['state']) == input_digest(current['state']), 'acknowledged state lost before retry')
        else:
            command = by_cell.get(cell)
            require(command is not None and reply['sequence'] == current['next_sequence']
                    and input_digest(reply['state']) == input_digest(command['expected']), 'lost acknowledged prefix or unknown mutation before retry')
            present.add(cell)
        if ingress:
            require(reply == rows[order - 2]['response'], 'owner routes disagree before retry')
    return {'owner_reads': len(rows), 'ambiguous_cells_present': sorted(present),
            'verified_monotonic_seconds': previous}


def audit_before_retry(root, report, send, *, clock=time.monotonic):
    _, end = recovery_window(root, report)
    ledger = Ledger(Path(root) / 'before-recovery.jsonl')
    try:
        for cell in range(report['profile']['cells']['total']):
            for ingress in range(1, 4):
                began = clock()
                require(began + 6 <= end, 'pre-retry read exceeds recovery bound')
                code, reply = send(cell, 'query', None, {}, member=ingress - 1)
                ledger.append({'cell': cell, 'ingress': ingress, 'status': code, 'response': reply,
                               'invoked_monotonic_seconds': began, 'monotonic_seconds': clock()})
                require(code == 200, 'pre-retry read failed')
    finally:
        ledger.close()
    return verify_audit(root, report)


def continuation_events(root, report, resolutions):
    """Normalize only event order; all submitted identities and replies stay raw."""
    path = Path(root) / 'resumed.jsonl'
    if report['scenario'] != SCENARIO:
        require(not path.exists(), 'undeclared resumed workload')
        return []
    audit = verify_audit(root, report)
    recovery = read_events(Path(root) / 'recovery.jsonl')
    require(recovery[0]['monotonic_seconds'] >= audit['verified_monotonic_seconds'], 'mutation preceded acknowledged-state audit')
    rows = read_events(path)
    require(len(rows) == 2 * report['profile']['cells']['total'], 'missing post-restart write')
    previous = recovery[-1]['monotonic_seconds']
    _, end = recovery_window(root, report)
    for order, row in enumerate(rows, 1):
        cell, kind = (order - 1) // 2, 'invoke' if order % 2 else 'complete'
        require(row['order'] == order and row['event'] == kind and row['cell'] == cell
                and row['id'] == f'post-restart:{cell}' and row['phase'] == 'post-restart' and row['operation'] == 'command', 'post-restart event identity differs')
        stamp = row['monotonic_seconds']
        require(number(stamp) and previous <= stamp <= end, 'resumed command exceeds recovery bound')
        previous = stamp
        if kind == 'invoke':
            require(stamp + 6 <= end and type(row['wall_ms']) is int
                    and row['wall_ms'] + 6000 < row['retry_until_ms'] == row['wall_ms'] + report['profile']['timing']['retry_window_ms'], 'new command deadline differs')
        else:
            require(row['status'] == 200 and row['outcome'] == 'completed', 'new post-restart command remains unresolved')
    offset = len(read_events(Path(root) / 'operations.jsonl')) + len(resolutions)
    return [{**row, 'order': offset + order} for order, row in enumerate(rows, 1)]


def resume_writes(root, report, state, send, *, clock=time.monotonic, wall=time.time):
    _, end = recovery_window(root, report)
    ledger = Ledger(Path(root) / 'resumed.jsonl')
    try:
        for cell, current in sorted(state.items()):
            began, wall_ms = clock(), int(wall() * 1000)
            require(began + 6 <= end, 'new post-restart write exceeds recovery bound')
            body = {'kind': 'set', 'expected_revision': current['state']['revision'],
                    'value': (current['state']['left'] + 1) % 1000001, 'payload': 'post-restart'}
            headers = {'command_id': hashlib.sha256(f"{report['run_id']}:{cell}".encode()).hexdigest()[:32] + f":{current['next_sequence']}",
                       'retry_until_ms': wall_ms + report['profile']['timing']['retry_window_ms']}
            common = {'cell': cell, 'id': f'post-restart:{cell}', 'phase': 'post-restart', 'operation': 'command'}
            invocation = {**common, **headers, 'event': 'invoke', 'input': body, 'input_sha256': input_digest(body),
                          'monotonic_seconds': began, 'wall_ms': wall_ms}
            ledger.append(invocation)
            require(clock() + 6 <= end and int(wall() * 1000) + 6000 < headers['retry_until_ms'], 'journal exhausted new command budget')
            try:
                code, reply = send(cell, 'command', body, headers)
            except Exception as error:
                code, reply = None, {'transport_error': type(error).__name__}
            ledger.append({**common, 'event': 'complete', 'status': code, 'response': reply,
                           'outcome': 'completed' if code == 200 else 'ambiguous', 'monotonic_seconds': clock()})
            expected, changed = model(current['state'], body)
            require(code == 200 and input_digest(reply['state']) == input_digest(expected) and reply['changed'] is changed, 'post-restart mutation failed')
            check_command(reply, invocation, report['run_id'], report['profile']['durability'], current['next_sequence'])
            current.update(state=expected, next_sequence=current['next_sequence'] + 1, **position(reply))
    finally:
        ledger.close()
