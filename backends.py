"""Deterministic engine simulator and asynchronous wrappers of the supplied engine.

Both adapters report completions only to their single controller. Tokens are
opaque attempt fences; callers must ignore completions from older attempts.
Virtual timestamps and normalized original-engine timestamps share logical
seconds, so the same scheduler can drive experiments and the HTTP service.
"""
from __future__ import annotations

import heapq
import bisect
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from engine import (BATCH_SECONDS, COLD_LOAD_SECONDS, WARM_LOAD_SECONDS,
                    MAX_BATCH_SIZE, MAX_LORAS_IN_MEMORY, InferenceEngine,
                    EngineError, BatchTooLarge, MixedLoRABatch, ReplicaBusy,
                    LoRACacheFull, LoRAInUse, LoRANotLoaded, DuplicateLoad)


class VirtualBackend:
    def __init__(self, replicas=4, now=0.0, slots=MAX_LORAS_IN_MEMORY,
                 record_events=True):
        self.count = replicas
        self.capacity = slots
        self.now = float(now)
        self.origin = float(now)
        self.speed = 1.0
        self.record_events = record_events
        self.events = []
        self.loaded = [dict() for _ in range(replicas)]
        self.warm = [set() for _ in range(replicas)]
        self.loading = [None] * replicas
        self.busy = [None] * replicas
        self._operations = []
        self._completions = []
        self._seq = 0

    def _log(self, replica, event, **fields):
        if self.record_events:
            self.events.append(dict(ts=self.now, replica=replica,
                                    event=event, **fields))

    def _enqueue(self, at, kind, replica, token, data):
        self._seq += 1
        heapq.heappush(self._operations, (at, self._seq, kind, replica, token, data))
        return at

    def start_load(self, replica, lora, path, token):
        if lora in self.loaded[replica]:
            self.inject_completion(dict(kind='load', replica=replica,
                                        token=token, ok=True, at=self.now, lora=lora))
            self._log(replica, 'load_noop', lora=lora)
            return self.now
        if self.loading[replica] is not None:
            raise DuplicateLoad('one controller loader per replica')
        if len(self.loaded[replica]) >= self.capacity:
            raise LoRACacheFull('adapter slots full')
        tier = 'warm' if lora in self.warm[replica] else 'cold'
        self.loading[replica] = (lora, token)
        self._log(replica, 'load_start', lora=lora, tier=tier)
        duration = WARM_LOAD_SECONDS if tier == 'warm' else COLD_LOAD_SECONDS
        return self._enqueue(self.now + duration, 'load', replica, token,
                             (lora, path, tier))

    def start_batch(self, replica, inputs, token):
        if not inputs:
            raise EngineError('empty batch')
        if len(inputs) > MAX_BATCH_SIZE:
            raise BatchTooLarge('batch exceeds 128')
        targets = {x.get('lora') for x in inputs}
        if len(targets) != 1:
            raise MixedLoRABatch('batch must be homogeneous')
        lora = next(iter(targets))
        if self.busy[replica] is not None:
            raise ReplicaBusy('compute busy')
        if lora is not None and lora not in self.loaded[replica]:
            raise LoRANotLoaded(str(lora))
        self.busy[replica] = (lora, token)
        self._log(replica, 'batch_start', lora=lora, size=len(inputs))
        return self._enqueue(self.now + BATCH_SECONDS, 'batch', replica,
                             token, tuple(inputs))

    def evict(self, replica, lora):
        if self.loading[replica] and self.loading[replica][0] == lora:
            raise LoRAInUse('adapter loading')
        if self.busy[replica] and self.busy[replica][0] == lora:
            raise LoRAInUse('adapter in use')
        if lora not in self.loaded[replica]:
            raise LoRANotLoaded(str(lora))
        del self.loaded[replica][lora]
        self._log(replica, 'evict', lora=lora)

    def advance(self, to):
        if to < self.now - 1e-9:
            raise ValueError('virtual clock cannot go backwards')
        while self._operations and self._operations[0][0] <= to:
            at, _, kind, replica, token, data = heapq.heappop(self._operations)
            self.now = at
            completion = dict(kind=kind, replica=replica, token=token,
                              ok=True, at=at)
            if kind == 'load':
                lora, path, tier = data
                self.loading[replica] = None
                self.loaded[replica][lora] = path
                self.warm[replica].add(lora)
                self._log(replica, 'load_end', lora=lora, tier=tier)
                completion['lora'] = lora
            else:
                self.busy[replica] = None
                result = []
                lora = data[0].get('lora')
                for inp in data:
                    result.append(dict(id=inp.get('id'), lora=lora,
                                       output=f"[{lora or 'base'}] mock completion"))
                    self._log(replica, 'request_done', lora=lora,
                              id=inp.get('id'), customer=inp.get('customer'))
                self._log(replica, 'batch_end', lora=lora, size=len(data))
                completion['result'] = result
            self._completions.append(completion)
        self.now = max(self.now,float(to))

    def inject_completion(self, completion):
        self._completions.append(completion)

    def poll(self):
        completions, self._completions = self._completions, []
        return completions

    def next_event(self):
        if self._completions:
            return self.now
        return self._operations[0][0] if self._operations else None

    def event_log(self):
        return list(self.events)

    def close(self):
        pass


class ThreadedBackend:
    """One nonpreemptible compute worker and one loader per original engine.

    Call poll frequently enough to fit Config.controller_margin. Sleep/poll
    delays are logical seconds multiplied by speed; high acceleration needs
    correspondingly larger timing margins. This adapter never edits engine.py.
    """
    def __init__(self, replicas=4, speed=50.0, origin=None,
                 record_events=True, engines=None):
        if speed <= 0:
            raise ValueError('speed must be positive')
        self.count = replicas
        self.capacity = MAX_LORAS_IN_MEMORY
        self.speed = float(speed)
        self.origin = 0.0 if origin is None else float(origin)
        self.wall_origin = time.time()
        self._mono_origin = time.monotonic()
        self.record_events = record_events
        self.engines = engines or [InferenceEngine(str(i), sim_speed=speed)
                                   for i in range(replicas)]
        self._pool = ThreadPoolExecutor(max_workers=replicas * 2,
                                        thread_name_prefix='replica')
        self._done = queue.SimpleQueue()
        self._warm = [set() for _ in range(replicas)]
        self._busy = [False] * replicas
        self._loading = [False] * replicas
        self._lock = threading.Lock()
        self._clock_anchors = []
        self._clock_anchor()

    @property
    def now(self):
        return self.origin + (time.monotonic() - self._mono_origin) * self.speed

    def _clock_anchor(self):
        # The threaded engine logs wall time, while deadlines use monotonic
        # time. Clock discipline can slew these clocks differently. Bracket
        # every operation rather than extrapolating one startup offset.
        with self._lock:
            self._clock_anchors.append((time.time(),self.now))

    def _run(self, kind, replica, token, operation):
        self._clock_anchor()
        completion = dict(kind=kind, replica=replica, token=token)
        try:
            completion.update(ok=True, result=operation())
        except Exception as exc:
            completion.update(ok=False, error=f'{type(exc).__name__}: {exc}')
        self._clock_anchor()
        completion['at'] = self.now
        with self._lock:
            self._done.put(completion)
            (self._busy if kind == 'batch' else self._loading)[replica] = False

    def start_load(self, replica, lora, path, token):
        with self._lock:
            if self._loading[replica]:
                raise DuplicateLoad('one controller loader per replica')
            self._loading[replica] = True
            duration = WARM_LOAD_SECONDS if lora in self._warm[replica] else COLD_LOAD_SECONDS
        def operation():
            self.engines[replica].load_lora(lora, path)
            with self._lock:
                self._warm[replica].add(lora)
            return lora
        self._pool.submit(self._run, 'load', replica, token, operation)
        return self.now + duration

    def start_batch(self, replica, inputs, token):
        with self._lock:
            if self._busy[replica]:
                raise ReplicaBusy('compute busy')
            self._busy[replica] = True
        self._pool.submit(self._run, 'batch', replica, token,
                          lambda: self.engines[replica].process_inputs(inputs))
        return self.now + BATCH_SECONDS

    def evict(self, replica, lora):
        self._clock_anchor()
        self.engines[replica].evict_lora(lora)
        self._clock_anchor()

    def poll(self):
        result = []
        while True:
            try:
                result.append(self._done.get_nowait())
            except queue.Empty:
                return result

    def next_event(self):
        # Wall workers wake the service loop; this is a conservative poll hint.
        # Publication and the active flags form one state transition. Without
        # this shared snapshot, an empty-queue read can race its final callback.
        with self._lock:
            if not self._done.empty():
                return self.now
            return self.now + .001 * self.speed if any(self._busy + self._loading) else None

    def event_log(self):
        if not self.record_events:
            return []
        raw_events = []
        for i, engine in enumerate(self.engines):
            for raw in engine.event_log():
                e = dict(raw)
                e['replica'] = i
                raw_events.append(e)
        self._clock_anchor()
        with self._lock:
            anchors = sorted(self._clock_anchors)
        walls = [a[0] for a in anchors]
        events = []
        for e in raw_events:
            raw = e['ts']
            j = bisect.bisect_right(walls,raw)
            if j == 0:
                logical = anchors[0][1]+(raw-walls[0])*self.speed
            elif j == len(anchors):
                logical = anchors[-1][1]+(raw-walls[-1])*self.speed
            else:
                left,right = anchors[j-1],anchors[j]
                fraction = (raw-left[0])/(right[0]-left[0])
                logical = left[1]+fraction*(right[1]-left[1])
            e['raw_ts'] = raw
            e['ts'] = logical
            events.append(e)
        return sorted(events, key=lambda e: e['ts'])

    @property
    def events(self):
        return self.event_log()

    def close(self):
        self._pool.shutdown(wait=True)
