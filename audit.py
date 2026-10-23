"""Independent checks of engine events and logical request conservation.

This module imports neither scheduler nor backend implementation. A protected
run is healthy only when it has zero failed/missing/late accepted requests.
"""
from __future__ import annotations

from collections import defaultdict


def audit_events(events: list[dict], slots: int = 32, batch_limit: int = 128) -> dict:
    replicas = defaultdict(lambda: {'resident': set(), 'loading': set(), 'busy': None, 'last': float('-inf')})
    done, starts, requests, cold, warm, overlap = set(), {}, {}, 0, 0, 0
    load_starts, load_intervals, compute_intervals = {}, [], []
    for event in events:
        replica = replicas[event['replica']]
        ts, name, lora = event['ts'], event['event'], event.get('lora')
        assert ts >= replica['last'], 'replica event timestamps regressed'
        replica['last'] = ts
        resident, loading, busy = replica['resident'], replica['loading'], replica['busy']
        if name == 'load_start':
            assert lora not in resident | loading, 'duplicate load'
            assert len(resident | loading) < slots, 'adapter slot overflow'
            loading.add(lora)
            load_starts[(event['replica'], lora)] = ts
            cold += event['tier'] == 'cold'
            warm += event['tier'] == 'warm'
            overlap += busy is not None
        elif name == 'load_end':
            assert lora in loading, 'load end without start'
            loading.remove(lora)
            resident.add(lora)
            load_intervals.append((event['replica'], load_starts.pop((event['replica'], lora)), ts))
        elif name == 'evict':
            assert lora in resident and lora not in loading, 'illegal eviction'
            assert busy is None or busy['lora'] != lora, 'eviction of in-use adapter'
            resident.remove(lora)
        elif name == 'batch_start':
            assert busy is None, 'overlapping compute'
            assert 0 < event['size'] <= batch_limit, 'illegal batch size'
            assert lora is None or lora in resident, 'compute before load'
            replica['busy'] = {'lora': lora, 'size': event['size'], 'done': 0, 'start': ts}
        elif name == 'request_done':
            assert busy is not None and lora == busy['lora'], 'request outside homogeneous batch'
            request_id = str(event['id'])
            assert request_id not in done, 'duplicate logical completion'
            done.add(request_id)
            starts[request_id] = busy['start']
            requests[request_id] = {'start': busy['start'], 'end': ts,
                                    'customer': event.get('customer'), 'lora': lora,
                                    'replica': event['replica']}
            busy['done'] += 1
        elif name == 'batch_end':
            assert busy is not None and busy['lora'] == lora, 'batch end without start'
            assert busy['size'] == event['size'] == busy['done'], 'batch lost or invented requests'
            compute_intervals.append((event['replica'], busy['start'], ts))
            replica['busy'] = None
        elif name == 'load_noop':
            assert lora in resident, 'invalid noop'
    for replica in replicas.values():
        assert replica['busy'] is None and not replica['loading'], 'incomplete engine operation'
    return {'completed': len(done), 'cold_loads': cold, 'warm_loads': warm,
            'overlapped_loads': overlap, 'request_starts': starts, 'requests': requests,
            'compute_intervals': compute_intervals, 'load_intervals': load_intervals}


def engine_time_summary(audited, begin, end, replicas):
    """Integrate measured engine intervals within a fixed logical-time window.

    Exposed load is loader time outside compute on the same replica. It is a
    cache/overlap diagnostic; some of this time may not block queued demand.
    Compute idle includes both intentional coalescing and demand-loading stalls.
    """
    assert end >= begin, 'measurement window regressed'
    compute, loads = defaultdict(list), defaultdict(list)
    for destination, intervals in ((compute, audited['compute_intervals']), (loads, audited['load_intervals'])):
        for replica, start, finish in intervals:
            start, finish = max(begin, start), min(end, finish)
            if finish > start:
                destination[replica].append((start, finish))
    compute_seconds = sum(finish-start for intervals in compute.values() for start, finish in intervals)
    load_seconds = sum(finish-start for intervals in loads.values() for start, finish in intervals)
    overlap = 0.0
    for replica, intervals in loads.items():
        batches = compute[replica]
        index = 0
        for start, finish in intervals:
            while index < len(batches) and batches[index][1] <= start:
                index += 1
            candidate = index
            while candidate < len(batches) and batches[candidate][0] < finish:
                left, right = batches[candidate]
                overlap += max(0.0, min(finish, right)-max(start, left))
                candidate += 1
    return {'compute_seconds': compute_seconds, 'load_seconds': load_seconds,
            'overlapped_load_seconds': overlap, 'exposed_load_seconds': max(0.0, load_seconds-overlap),
            'compute_idle_seconds': max(0.0, replicas*(end-begin)-compute_seconds)}


def crosscheck_engine(rows, accepted, outcomes, audited, tolerance=1e-4):
    """Match engine work to logical results, including ownership and timing.

    Controller starts can precede the worker's batch start. Result publication
    can follow engine completion; it cannot precede it. The small tolerance
    accommodates wall/monotonic clock alignment in the original-engine adapter.
    """
    successful = {i for i, out in outcomes.items() if out['status'] in ('completed', 'succeeded')}
    requests = audited['requests']
    assert set(requests) == successful, 'engine/result identity mismatch'
    assert successful <= accepted, 'engine completed unaccepted request'
    for request_id, engine in requests.items():
        ingress, customer, lora = rows[int(request_id)]
        outcome = outcomes[request_id]
        assert engine['customer'] == customer, 'engine customer mismatch'
        assert engine['lora'] == lora, 'engine adapter mismatch'
        assert engine['start'] + tolerance >= ingress, 'engine started before ingress'
        assert engine['end'] + tolerance >= engine['start'], 'engine completion before start'
        if outcome.get('start') is not None:
            assert ingress <= outcome['start'] + tolerance, 'controller started before ingress'
            assert outcome['start'] <= engine['start'] + tolerance, 'controller start after engine start'
        assert outcome['end'] + tolerance >= engine['end'], 'result published before engine completion'
        if 'replica' in outcome:
            assert outcome['replica'] == engine['replica'], 'result replica mismatch'
        output = outcome.get('output', outcome.get('result'))
        if isinstance(output, dict):
            if 'id' in output:
                assert str(output['id']) == request_id, 'result request identity mismatch'
            if 'lora' in output:
                assert output['lora'] == lora, 'result adapter mismatch'


def audit_requests(rows: list, accepted: set[str], outcomes: dict, slo: float, offered_ids=None) -> dict:
    trace_ids = {str(i) for i in range(len(rows))}
    offered_ids = trace_ids if offered_ids is None else offered_ids
    assert offered_ids <= trace_ids, 'invented replay input'
    assert accepted <= offered_ids, 'invented acceptance'
    assert set(outcomes) <= accepted, 'completion without acceptance'
    successes = {i for i, out in outcomes.items() if out['status'] in ('completed', 'succeeded')}
    failed = len(outcomes) - len(successes)
    late = sum(outcomes[i]['end'] > rows[int(i)][0] + slo + 1e-7 for i in successes)
    return {'offered': len(offered_ids), 'accepted': len(accepted), 'rejected': len(offered_ids) - len(accepted),
            'completed': len(successes), 'failed': failed, 'deadline_misses': late,
            'outstanding': len(accepted) - len(outcomes)}
