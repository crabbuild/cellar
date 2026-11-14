#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Closed-loop public-binding load; run inside the separately capped client."""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import math
from pathlib import Path
import threading
import time

ENTITIES = 24
BATCH = 8


def require(condition, message):
    if not condition:
        raise ValueError(message)


class Client:
    def __init__(self, hosts, token):
        self.connections = [http.client.HTTPConnection(host, timeout=7) for host in hosts]
        self.token = token

    def close(self):
        for connection in self.connections:
            connection.close()

    def request(self, node, operation, value):
        connection = self.connections[node % len(self.connections)]
        body = json.dumps(value, separators=(',', ':')).encode()
        connection.request('POST', '/' + operation, body,
                           {'authorization': 'Bearer ' + self.token, 'content-type': 'application/json'})
        response = connection.getresponse()
        body = response.read(64 * 1024 * 1024 + 1)
        require(len(body) <= 64 * 1024 * 1024, 'response observer bound exceeded')
        require(response.status == 200, f'HTTP {response.status}: {body[:2048].decode(errors="replace")}')
        return json.loads(body)


def value_for(identity):
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return digest * 4  # Exactly 256 ASCII payload bytes in every durability mode.


def latency_summary(micros):
    values = sorted(micros)
    def percentile(p):
        return values[math.ceil(len(values) * p) - 1] / 1000 if values else None
    return {'p50_ms': percentile(.5), 'p95_ms': percentile(.95), 'p99_ms': percentile(.99),
            'max_ms': values[-1] / 1000 if values else None}


def window(hosts, token, family, prefix, phase, concurrency, seconds):
    barrier = threading.Barrier(concurrency + 1)
    deadline = [0.0]
    def worker(worker_id):
        client = Client(hosts, token)
        attempts, successful, failures, expected, batches = [], [], [], {}, {}
        sequence = 0
        barrier.wait()
        try:
            while time.monotonic() < deadline[0]:
                sequence += 1
                entity = ((sequence - 1) * concurrency + worker_id) % ENTITIES
                identity = f'{prefix}.{worker_id}.{sequence}'
                value = value_for(identity)
                key = f'{prefix}.{worker_id}'
                request = {'entity': entity, 'key': key, 'value': value}
                if family == 'queue':
                    messages = [{'id': f'{identity}.{item}', 'value': value} for item in range(BATCH)]
                    request['messages'] = messages
                    batches[identity] = messages
                if phase == 'read':
                    request['key'] = 'readseed'
                entered = time.monotonic_ns()
                try:
                    reply = client.request(worker_id + sequence, 'read' if phase == 'read' else 'write', request)
                    if phase == 'read':
                        require(reply == value_for('readseed'), f'{family} read returned an unexpected value')
                    else:
                        require(reply is True, 'mutation result was not acknowledged')
                    elapsed = (time.monotonic_ns() - entered) // 1000
                    successful.append(elapsed)
                    if family == 'queue':
                        for message in messages:
                            expected[message['id']] = hashlib.sha256(message['value'].encode()).hexdigest()
                    elif phase != 'read':
                        expected[(entity, key)] = value
                except Exception as error:
                    elapsed = (time.monotonic_ns() - entered) // 1000
                    failures.append({'worker': worker_id, 'sequence': sequence, 'entity': entity,
                                     'identity': identity, 'error': str(error), 'mutation_outcome': 'unconfirmed' if phase != 'read' else 'read_failed'})
                    attempts.append(elapsed)
                    break  # Never replace an unknown mutation with a fresh-ID retry.
                attempts.append(elapsed)
        finally:
            client.close()
        return attempts, successful, failures, expected, batches
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, i) for i in range(concurrency)]
        start = time.monotonic()
        deadline[0] = start + seconds
        barrier.wait()
        results = [future.result() for future in futures]
    elapsed = time.monotonic() - start
    attempts = [latency for result in results for latency in result[0]]
    successful = [latency for result in results for latency in result[1]]
    errors = [error for result in results for error in result[2]]
    expected = {key: value for result in results for key, value in result[3].items()}
    batches = {key: value for result in results for key, value in result[4].items()}
    row = {'phase': phase, 'prefix': prefix, 'concurrency': concurrency, 'requested_seconds': seconds,
           'elapsed_seconds': elapsed, 'attempts': len(attempts), 'successful_requests': len(successful),
           'errors': errors, 'requests_per_second': len(successful) / elapsed,
           'attempt_latency_us': attempts, 'success_latency_us': successful,
           'attempt_latency': latency_summary(attempts), 'success_latency': latency_summary(successful),
           'verified': False}
    client = Client(hosts, token)
    try:
        if family == 'queue':
            drain_deadline = time.monotonic() + 60
            observed = {}
            while True:
                observed = {}
                duplicates = 0
                for node in range(3):
                    deliveries = client.request(node, 'deliveries', {'prefix': prefix + '.'})
                    for identity, (body, count) in deliveries.items():
                        require(identity not in observed or observed[identity] == body, 'cross-node Queue body mismatch')
                        duplicates += count - int(identity not in observed)
                        observed[identity] = body
                if set(expected) <= set(observed) or time.monotonic() >= drain_deadline:
                    break
                time.sleep(.2)
            metrics = [client.request(entity % 3, 'metrics', {'entity': entity}) for entity in range(ENTITIES)]
            # Delivery is observed before its ack commits. Wait for actual durable
            # settlement as well as callback observation, under a bounded drain.
            while any(metric['backlog'] != 0 for metric in metrics) and time.monotonic() < drain_deadline:
                time.sleep(.2)
                metrics = [client.request(entity % 3, 'metrics', {'entity': entity}) for entity in range(ENTITIES)]
            drained = time.monotonic() - start
            row.update(acknowledged_messages=len(expected), delivered_messages=len(observed),
                       duplicate_deliveries=duplicates, missing_messages=sorted(set(expected) - set(observed)),
                       extra_messages=sorted(set(observed) - set(expected)), queue_metrics=metrics,
                       enqueue_messages_per_second=len(expected) / elapsed, enqueue_and_drain_seconds=drained,
                       verified_messages_per_second_including_drain=len(expected) / drained,
                       attempted_batches=batches)
            require(observed == expected, 'Queue delivery set/body differs from acknowledged messages')
            require(all(metric['backlog'] == 0 and metric['dead'] == 0 for metric in metrics), 'Queue backlog or dead-letter messages remain')
        elif phase != 'read':
            for index, ((entity, key), value) in enumerate(expected.items()):
                require(client.request(index + 1, 'read', {'entity': entity, 'key': key}) == value, 'write readback mismatch')
            row['verified_keys'] = len(expected)
        require(not errors, 'request failures retained; this window is not qualified')
        require(len(successful) > 0 and elapsed >= seconds, 'incomplete offered measurement interval')
        row['verified'] = True
    except Exception as error:
        row['verification_error'] = str(error)
    finally:
        client.close()
    return row


def run(args):
    require(args.family in ('sql', 'kv', 'queue'), 'invalid family')
    require(1 <= args.seconds <= 120 and 1 <= args.repetitions <= 5 and 1 <= args.warmup <= 30, 'invalid timing')
    require(len(args.hosts) == 3 and all(1 <= c <= 32 for c in args.concurrency), 'invalid offered topology/concurrency')
    token = Path('/config/token').read_text().strip()
    client = Client(args.hosts, token)
    try:
        for node in range(3):
            require(client.request(node, 'ready', {}) == {'family': args.family, 'entities': ENTITIES}, 'readiness identity mismatch')
        if args.family != 'queue':
            for entity in range(ENTITIES):
                if args.family == 'sql':
                    require(client.request(entity, 'setup', {'entity': entity}) is True, 'SQL setup failed')
                require(client.request(entity, 'write', {'entity': entity, 'key': 'readseed', 'value': value_for('readseed')}) is True, 'read seed failed')
    finally:
        client.close()
    for repeat in range(args.repetitions):
        order = args.concurrency if repeat % 2 == 0 else list(reversed(args.concurrency))
        for concurrency in order:
            phases = ['warmup', 'enqueue'] if args.family == 'queue' else ['warmup', 'write', 'read']
            for phase in phases:
                prefix = f'{args.case}.{repeat}.{concurrency}.{phase}'
                row = window(args.hosts, token, args.family, prefix, phase, concurrency,
                             args.warmup if phase == 'warmup' else args.seconds)
                row.update(family=args.family, repeat=repeat)
                print('PRIMITIVE_COMPARE ' + json.dumps(row, separators=(',', ':')), flush=True)
                if not row['verified']:
                    return 1
    return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--family', required=True)
    parser.add_argument('--case', required=True)
    parser.add_argument('--hosts', nargs=3, required=True)
    parser.add_argument('--seconds', type=int, default=30)
    parser.add_argument('--warmup', type=int, default=5)
    parser.add_argument('--repetitions', type=int, default=2)
    parser.add_argument('--concurrency', nargs='+', type=int, default=[1, 4, 16])
    try:
        raise SystemExit(run(parser.parse_args()))
    except Exception as error:
        print('PRIMITIVE_COMPARE_FATAL ' + json.dumps({'error': str(error)}), flush=True)
        raise SystemExit(1)
