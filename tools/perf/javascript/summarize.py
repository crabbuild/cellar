#!/usr/bin/env python3
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Summarize preserved JS bridge trials without discarding failed trials."""
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def summarize(directory):
    report = json.loads((directory / 'report.json').read_text())
    groups = defaultdict(list)
    for trial in report['trials']:
        groups[(trial['backend'], trial['workload'], trial['concurrency'])].append(trial)
    summary = []
    for (backend, workload, concurrency), trials in groups.items():
        samples = []
        for trial in trials:
            samples.extend(json.loads((directory / trial['latency_samples_file']).read_text()))
        samples.sort()
        percentile = lambda q: samples[max(0, math.ceil(q * len(samples)) - 1)] if samples else None
        rates = [t['rps'] for t in trials]
        summary.append({
            'backend': backend, 'workload': workload, 'concurrency': concurrency,
            'trials': len(trials), 'median_rps': statistics.median(rates),
            'min_rps': min(rates), 'max_rps': max(rates),
            'median_body_mib_s': statistics.median(t['body_mib_s'] for t in trials),
            'pooled_latency_ms': {f'p{p}': percentile(p / 100) for p in [50, 95, 99]},
            'completed': sum(t['completed'] for t in trials),
            'failed': sum(t['failed'] for t in trials),
            'median_server_cpu_cores': statistics.median(t['server_cpu_cores'] for t in trials if t['server_cpu_cores'] is not None) if any(t['server_cpu_cores'] is not None for t in trials) else None,
            'median_client_cpu_cores': statistics.median(t['client_cpu_cores'] for t in trials),
            'peak_server_rss_mib': max(t['server_peak_rss_mib'] for t in trials),
        })
    (directory / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    machine = report['machine']
    lines = [
        '# JS/TS bridge performance measurement', '',
        f"Started: {report['started']}. Source baseline: `{report['source']}`.", '',
        f"Machine: {machine['cpu']}, {machine['logical_cpus']} logical CPUs, "
        f"{machine['memory_bytes'] / 2**30:.0f} GiB RAM, {machine['platform']} {machine['arch']}.", '',
        f"Toolchain: {report['rust']}; Node {report['node']}. Rust release profile, "
        'two Tokio workers, default bridge limits, private local SQLite development bucket.', '',
        f"{report['config']['repeats']} repeats; {report['config']['seconds']} seconds measured "
        f"and {report['config']['warmup']} seconds warmup per trial. RPS is the median "
        'of repeated trials at each concurrency; latency percentiles pool successful requests. '
        'Write RPS equals acknowledged transactions per second. MiB/s sums request and response bodies.', '',
        '| Backend | Workload | Clients | Median req/s or TPS | Min–max | MiB/s | p50 ms | p95 ms | p99 ms | Errors |',
        '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
    ]
    for row in summary:
        lat = row['pooled_latency_ms']
        fmt = lambda v: f'{v:.2f}' if v is not None else 'n/a'
        lines.append(f"| {row['backend']} | {row['workload']} | {row['concurrency']} | "
                     f"{row['median_rps']:.1f} | {row['min_rps']:.1f}–{row['max_rps']:.1f} | "
                     f"{row['median_body_mib_s']:.2f} | {fmt(lat['p50'])} | {fmt(lat['p95'])} | "
                     f"{fmt(lat['p99'])} | {row['failed']} |")
    lines.extend(['', '## Correctness and limits', '',
        f"Measured requests: {sum(t['attempted'] for t in report['trials']):,}; "
        f"failures: {sum(t['failed'] for t in report['trials']):,}. "
        f"Warmup failures: {report.get('warmup_failures', 0):,}.", '',
        f"Cold recovery checks: {json.dumps([{k: v for k, v in r.items() if k != 'counts'} for r in report['recovery']])}", '',
        'Every write trial reconciles stored effects against all acknowledged increments. '
        'Recovery follows graceful shutdown with a new node identity and empty local storage, '
        'using the same bucket. This is not a crash or external-side-effect test.', '',
        'Shared development machine; other workloads can affect results. Closed-loop load, '
        'not independent arrival-rate qualification. Five-second defaults are short capacity probes, '
        'not soak tests. Local SQLite bucket results do not establish cloud or multi-node TPS. '
        'The same TypeScript source compiles to JavaScript for both backends. '
        'This measures end-to-end HTTP paths including SDK encoding, not isolated bridge calls.', '',
        'Raw report, latency samples, warmups, resource observations and host logs are in this directory.',
    ])
    if report.get('fatal_error'):
        lines.extend(['', '**Incomplete run:**', '', '```', report['fatal_error'], '```'])
    (directory / 'summary.md').write_text('\n'.join(lines) + '\n')
    print(directory / 'summary.md')


if __name__ == '__main__':
    summarize(Path(sys.argv[1]).resolve())
