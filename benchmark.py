"""Matched-trace execution and unweighted comparison cards.

Protected offered-load runs and identical accepted-work replays answer
separate questions; a replay disables admission and may miss deadlines.
"""
from __future__ import annotations

import dataclasses
import gzip
import hashlib
import json
import math
import shlex
import statistics
import sys
import time
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

from audit import audit_events, audit_requests, crosscheck_engine, engine_time_summary
from workload import read_trace, trace_hash


def percentile(values, q):
    values = sorted(values)
    return values[min(len(values) - 1, math.ceil(q * len(values)) - 1)] if values else None


def customer_gaps(rows, accepted, outcomes, horizon=None):
    """Maximum no-compute-start interval while accepted, unstarted work exists.

    Empty queues reset the clock; simultaneous starts share one reset. This
    independent metric measures batch starts, not foreground-load grants.
    """
    timelines = defaultdict(list)
    for i in accepted:
        ingress, customer, _ = rows[int(i)]
        timelines[customer].append((ingress, 0, 1))
        outcome = outcomes.get(i, {})
        start = outcome.get('engine_start', outcome.get('start'))
        if start is not None:
            timelines[customer].append((start, 1, -1))
    gaps = {}
    for customer, timeline in timelines.items():
        queued, last, gap = 0, 0, 0
        for ts, is_start, delta in sorted(timeline):
            if queued == 0:
                last = ts
            if is_start:
                gap = max(gap, ts - last)
                last = ts
            queued += delta
        if queued and horizon is not None:
            gap = max(gap, horizon - last)
        gaps[customer] = gap
    return gaps


def comparison_card(header, rows, accepted, rejected, outcomes, events, slo, measured_end=None, offered_ids=None):
    conservation = audit_requests(rows, accepted, outcomes, slo, offered_ids=offered_ids)
    offered = {str(i) for i in range(len(rows))} if offered_ids is None else offered_ids
    assert not accepted.intersection(rejected), 'request both accepted and rejected'
    assert accepted | set(rejected) == offered, 'offered request lacks an admission outcome'
    final = max([sum(header['durations']), measured_end or 0] + [o['end'] for o in outcomes.values()])
    offered_counts = (Counter(row[1] for row in rows) if offered_ids is None else
                      Counter(rows[int(i)][1] for i in offered_ids))
    accepted_counts = Counter(rows[int(i)][1] for i in accepted)
    completed_counts = Counter(rows[int(i)][1] for i, o in outcomes.items() if o['status'] == 'completed')
    gaps = customer_gaps(rows, accepted, outcomes, horizon=final)
    waits, adapters = defaultdict(list), defaultdict(list)
    for i, outcome in outcomes.items():
        start = outcome.get('engine_start', outcome.get('start'))
        if start is not None:
            arrival, customer, adapter = rows[int(i)]
            wait = max(0, start - arrival)
            waits[customer].append(wait)
            adapters['base' if adapter is None else adapter].append(wait)
    customers = {}
    for customer in header['customers']:
        offered, admitted = offered_counts[customer], accepted_counts[customer]
        customers[customer] = {'offered': offered, 'accepted': admitted,
                               'completed': completed_counts[customer], 'rejected': offered - admitted,
                               'rejection_rate': (offered - admitted) / offered if offered else 0,
                               'max_wait': max(waits[customer], default=None),
                               'p99_wait': percentile(waits[customer], .99),
                               'max_backlogged_service_gap': gaps.get(customer)}
    windows, begin = {}, 0
    for name, duration in zip(('steady', 'burst', 'recovery'), header['durations']):
        end = begin + duration
        completed = [i for i, o in outcomes.items() if o['status'] == 'completed' and begin <= o['end'] < end]
        good = sum(outcomes[i]['end'] <= rows[int(i)][0] + slo + 1e-7 for i in completed)
        windows[name] = {'begin': begin, 'end': end, 'completed': len(completed),
                         'completed_goodput': len(completed) / duration if duration else 0,
                         'deadline_goodput': good / duration if duration else 0}
        begin = end
    drain_done = sum(o['status'] == 'completed' and o['end'] >= begin for o in outcomes.values())
    drain_good = sum(o['status'] == 'completed' and o['end'] >= begin and
                     o['end'] <= rows[int(i)][0] + slo + 1e-7 for i, o in outcomes.items())
    windows['drain'] = {'begin': begin, 'end': final, 'completed': drain_done,
                         'completed_goodput': drain_done / (final - begin) if final > begin else 0,
                         'deadline_goodput': drain_good / (final - begin) if final > begin else 0}
    fills = [e['size'] for e in events if e['event'] == 'batch_start']
    return {'conservation': conservation, 'windows': windows,
            'rejection_rate': conservation['rejected'] / conservation['offered'] if conservation['offered'] else 0,
            'rejection_reasons': dict(Counter(rejected.values())), 'customers': customers,
            'customers_without_acceptance': [c for c, v in customers.items() if not v['accepted']],
            'batch_fill': {'batches': len(fills), 'mean': statistics.mean(fills) if fills else 0,
                           'p50': percentile(fills, .5), 'p99': percentile(fills, .99)},
            'adapter_max_wait': {a: max(w) for a, w in adapters.items()},
            'max_wait': max((max(w) for w in waits.values() if w), default=0),
            'max_backlogged_service_gap': max(gaps.values(), default=0),
            'outstanding_at_load_end': sum(o['end'] >= begin for o in outcomes.values()) + conservation['outstanding']}


def source_snapshot(directory):
    root = Path(__file__).parent
    paths = sorted([*root.glob('*.py'), *root.glob('scripts/*.py')])
    sources = [(path.relative_to(root).as_posix(), path.read_bytes()) for path in paths]
    digest = hashlib.sha256()
    for name, contents in sources:
        digest.update(name.encode() + b'\0' + contents)
    source_hash = digest.hexdigest()
    target = Path(directory) / 'sources' / (source_hash + '.zip')
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED) as archive:
            for name, contents in sources:
                archive.writestr(name, contents)
    return source_hash


def run(trace, config, output, backend_kind='virtual', speed=10.0, replay=None, raw=True):
    from backends import ThreadedBackend, VirtualBackend
    from scheduler import Request, Scheduler
    header, rows = read_trace(trace)
    input_hash = trace_hash(trace)
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    replay_ids = None
    if replay is not None:
        suffix = '.requests.jsonl.gz'
        if not str(replay).endswith(suffix):
            raise ValueError('replay must name a .requests.jsonl.gz file with an associated result card')
        source_card = Path(str(replay)[:-len(suffix)] + '.json')
        metadata = json.loads(source_card.read_text())
        if metadata.get('trace_hash') != input_hash:
            raise ValueError('replay source trace hash does not match the input trace')
        with gzip.open(replay, 'rt') as source:
            replay_ids = {str(row[0]) for line in source if (row := json.loads(line))[1]}
        try:
            valid = all(str(int(i)) == i and 0 <= int(i) < len(rows) for i in replay_ids)
        except (TypeError, ValueError):
            valid = False
        if not valid:
            raise ValueError('replay request ID is outside input trace bounds')
        config = dataclasses.replace(config, admission_guard=False, tenant_ticket_cap=10**9,
                                     tenant_byte_cap=10**12)
    source_hash = source_snapshot(target.parent)
    control_budget = max(.001, config.controller_margin / 4)
    backend = (VirtualBackend(config.replicas) if backend_kind == 'virtual' else
               ThreadedBackend(config.replicas, speed=speed))
    scheduler = Scheduler(config, backend)
    last_control = backend.now
    control_timing = {'max_gap': 0.0, 'max_tick': 0.0, 'max_admission': 0.0}
    accepted, rejected, lateness = set(), {}, []
    # Replay follows the original row IDs, but excluded arrivals are not events.
    # Scheduler/backend wakeups still drive all accepted-work timers in gaps.
    offered_indices = iter(range(len(rows))) if replay_ids is None else iter(sorted(map(int, replay_ids)))
    wall_start, cpu_start, scheduler_cpu, i = time.perf_counter(), time.process_time(), 0.0, next(offered_indices, len(rows))
    try:
        while i < len(rows) or scheduler.pending or backend.next_event() is not None:
            now = backend.now
            while i < len(rows) and rows[i][0] <= now + 1e-9:
                if backend.now - last_control >= control_budget:
                    control_start = backend.now
                    control_timing['max_gap'] = max(control_timing['max_gap'], control_start-last_control)
                    before = time.process_time()
                    scheduler.tick()
                    scheduler_cpu += time.process_time() - before
                    last_control = backend.now
                    control_timing['max_tick'] = max(control_timing['max_tick'], last_control-control_start)
                arrival, customer, lora = rows[i]
                request_id = str(i)
                if replay_ids is None or request_id in replay_ids:
                    lateness.append(max(0, backend.now - arrival))
                    request = Request(request_id, customer, lora, None if lora is None else 's3://mock/' + lora,
                                      'Example prompt', arrival, arrival + config.slo)
                    before = time.process_time()
                    admission_start = backend.now
                    ok, reason = scheduler.submit(request)
                    control_timing['max_admission'] = max(control_timing['max_admission'], backend.now-admission_start)
                    scheduler_cpu += time.process_time() - before
                    if ok:
                        accepted.add(request_id)
                    else:
                        rejected[request_id] = reason
                i = next(offered_indices, len(rows))
            before = time.process_time()
            control_start = backend.now
            control_timing['max_gap'] = max(control_timing['max_gap'], control_start-last_control)
            scheduler.tick()
            scheduler_cpu += time.process_time() - before
            last_control = backend.now
            control_timing['max_tick'] = max(control_timing['max_tick'], last_control-control_start)
            now = backend.now
            wake = scheduler.next_wakeup()
            next_arrival = rows[i][0] if i < len(rows) else math.inf
            next_time = min(next_arrival, wake if wake is not None else math.inf)
            if not math.isfinite(next_time):
                if scheduler.pending:
                    raise RuntimeError('pending work has no wakeup')
                break
            if backend_kind == 'virtual':
                backend.advance(max(now + 1e-8, next_time))
            else:
                # Scale all waits. Polling is bounded to keep completions visible
                # even when the next request is distant.
                time.sleep(min(.001, max(0, (next_time - now) / speed)))
        # Complete any speculative loader before auditing the engine stream.
        if hasattr(backend, 'drain'):
            backend.drain()
        measured_end = backend.now
        events = backend.event_log()
        audited = audit_events(events)
        crosscheck_engine(rows, accepted, scheduler.completed, audited)
        outcomes = {i: dict(outcome) for i, outcome in scheduler.completed.items()}
        for request_id, engine in audited['requests'].items():
            outcomes[request_id].update(engine_start=engine['start'], engine_end=engine['end'])
        card = comparison_card(header, rows, accepted, rejected, outcomes, events, config.slo,
                               measured_end=measured_end, offered_ids=replay_ids)
        for window in card['windows'].values():
            window.update(engine_time_summary(audited, window['begin'], window['end'], config.replicas))
        audited.update(engine_time_summary(audited, 0, max(measured_end, sum(header['durations'])), config.replicas))
        audited.pop('request_starts')
        audited.pop('requests')
        audited.pop('compute_intervals')
        audited.pop('load_intervals')
        command = [sys.executable, 'run.py', 'bench', '--config', str(output),
                   '--trace', str(trace), '--output', str(output), '--backend', backend_kind,
                   '--speed', str(speed)]
        if replay:
            command.extend(('--replay', str(replay)))
        if not raw:
            command.append('--no-raw')
        card.update({'schema': 1, 'cohort': 'identical_accepted_replay' if replay else 'protected_offered',
                     'trace': str(trace), 'trace_hash': input_hash, 'source_hash': source_hash,
                     'seed': header['seed'], 'backend': backend_kind, 'speed': speed if backend_kind != 'virtual' else 1,
                     'config': dataclasses.asdict(config), 'engine_audit': audited,
                     'scheduler_metrics': scheduler.metrics, 'command': shlex.join(command),
                     'invocation': shlex.join(sys.argv),
                     'wall_seconds': time.perf_counter() - wall_start,
                     'process_cpu_seconds': time.process_time() - cpu_start,
                     'scheduler_cpu_seconds': scheduler_cpu,
                     'control_timing': control_timing,
                     'arrival_lateness': {'p50': percentile(lateness, .5), 'p99': percentile(lateness, .99),
                                          'max': max(lateness, default=0)},
                     'replay_source': str(replay) if replay else None})
        if replay_ids is not None:
            card['replay_input_count'] = len(replay_ids)
            card['replay_rejected'] = len(rejected)
            card['original_trace_offered'] = len(rows)
            card['excluded_from_replay'] = len(rows)-len(replay_ids)
            assert accepted == replay_ids, 'replay did not accept identical work'
        if raw:
            with gzip.open(target.with_suffix('.requests.jsonl.gz'), 'wt') as sink:
                for index in range(len(rows)):
                    rid = str(index)
                    outcome = outcomes.get(rid)
                    if outcome is not None:
                        outcome = {key: outcome.get(key) for key in
                                   ('status', 'start', 'end', 'engine_start', 'engine_end',
                                    'replica', 'attempt', 'error')}
                    # Raw outcomes retain enough evidence to independently
                    # reconstruct admission, wait, deadlines, and windows.
                    value = [index, rid in accepted, rejected.get(rid), outcome]
                    sink.write(json.dumps(value, separators=(',', ':')) + '\n')
            with gzip.open(target.with_suffix('.events.jsonl.gz'), 'wt') as sink:
                for event in events:
                    sink.write(json.dumps(event, separators=(',', ':')) + '\n')
        target.write_text(json.dumps(card, indent=2, sort_keys=True) + '\n')
        return card
    finally:
        if hasattr(backend, 'close'):
            backend.close()


def audit_card(path):
    """Recheck saved raw evidence, independently of scheduler execution."""
    path = Path(path)
    card = json.loads(path.read_text())
    trace = Path(card['trace'])
    assert trace_hash(trace) == card['trace_hash'], 'saved trace hash mismatch'
    _, rows = read_trace(trace)
    accepted, rejected, outcomes, seen = set(), {}, {}, set()
    with gzip.open(path.with_suffix('.requests.jsonl.gz'), 'rt') as source:
        for line in source:
            index, admitted, reason, outcome = json.loads(line)
            rid = str(index)
            assert rid not in seen and 0 <= index < len(rows), 'invalid raw request identity'
            seen.add(rid)
            if admitted:
                accepted.add(rid)
            if reason is not None:
                rejected[rid] = reason
            if outcome is not None:
                outcomes[rid] = outcome
    assert len(seen) == len(rows), 'raw evidence omitted trace rows'
    with gzip.open(path.with_suffix('.events.jsonl.gz'), 'rt') as source:
        audited = audit_events([json.loads(line) for line in source])
    crosscheck_engine(rows, accepted, outcomes, audited)
    if card['cohort'] == 'identical_accepted_replay':
        source = Path(card['replay_source'])
        metadata = json.loads(Path(str(source)[:-len('.requests.jsonl.gz')]+'.json').read_text())
        assert metadata['trace_hash'] == card['trace_hash'], 'replay cohort trace mismatch'
        with gzip.open(source, 'rt') as stream:
            cohort = {str(row[0]) for line in stream if (row := json.loads(line))[1]}
        assert accepted == cohort, 'replay cohort identities changed'
    offered = accepted | set(rejected) if card['cohort'] == 'identical_accepted_replay' else None
    checked = audit_requests(rows, accepted, outcomes, card['config']['slo'], offered)
    assert checked == card['conservation'], 'raw evidence disagrees with comparison card'
    assert accepted.isdisjoint(rejected), 'raw admission decisions overlap'
    assert accepted | set(rejected) == (offered if offered is not None else seen), 'unaccounted offered work'
    return checked
