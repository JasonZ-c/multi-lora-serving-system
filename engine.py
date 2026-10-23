"""Discrete-event replica of a multi-LoRA inference engine.

One replica serves a resident base model plus up to MAX_LORAS_IN_MEMORY LoRA
adapters. Sleeps sit where a real engine is slow, and every sleep is explicit
so scheduler timing can be checked against the event log.

Timing (wall clock, divided by SIM_SPEED):
    process_inputs(...)      BATCH_SECONDS      (1.25s, regardless of batch fill)
    load_lora(...) cold      COLD_LOAD_SECONDS  (2.0s — first load on this replica)
    load_lora(...) warm      WARM_LOAD_SECONDS  (0.2s — reload of a previously
                                                 evicted LoRA, served from host memory)
    evict_lora(...)          instant

Concurrency contract:
    - At most ONE process_inputs call may be in flight per replica. A second
      concurrent call raises ReplicaBusy.
    - load_lora MAY be called while a batch is processing.
    - evict_lora is instant, but cannot evict a LoRA that an in-flight batch
      is using or that is currently loading.

Every state change is appended to the replica's event log with a timestamp.
The event log is the ground truth for any measurement you build.
"""

from __future__ import annotations

import os
import threading
import time

BATCH_SECONDS = 1.25
COLD_LOAD_SECONDS = 2.0
WARM_LOAD_SECONDS = 0.2
MAX_LORAS_IN_MEMORY = 32
MAX_BATCH_SIZE = 128

# The base model is always resident. Inputs that omit "lora" (or set it to
# None) run against the base model and never require a load.
BASE_MODEL = None


class EngineError(Exception):
    """Base class for every error this engine raises."""


class BatchTooLarge(EngineError):
    """process_inputs received more than MAX_BATCH_SIZE inputs."""


class MixedLoRABatch(EngineError):
    """process_inputs received inputs targeting more than one LoRA."""


class LoRANotLoaded(EngineError):
    """The requested LoRA is not resident in this replica's memory."""


class ReplicaBusy(EngineError):
    """A batch is already in flight on this replica."""


class LoRACacheFull(EngineError):
    """All MAX_LORAS_IN_MEMORY slots are taken; evict before loading."""


class LoRAInUse(EngineError):
    """The LoRA is currently loading or serving an in-flight batch."""


class DuplicateLoad(EngineError):
    """A load for this LoRA is already in progress."""


# Sentinel meaning "no batch in flight" (None is a valid batch target: base model).
_IDLE = object()


class InferenceEngine:
    """One replica of the model. Instantiate one per replica in your design.

    sim_speed divides every sleep, so timing relationships stay identical
    while wall-clock time shrinks (sim_speed=10 makes a 2.0s cold load take
    0.2s). Defaults to the SIM_SPEED environment variable, then 1.0.
    """

    def __init__(self, replica_id: str, sim_speed: float | None = None):
        self.replica_id = replica_id
        self._sim_speed = float(
            sim_speed if sim_speed is not None else os.environ.get("SIM_SPEED", 1.0)
        )
        if self._sim_speed <= 0:
            raise ValueError("sim_speed must be positive")
        self._lock = threading.Lock()
        self._events_lock = threading.Lock()
        self._loaded: dict[str, str] = {}  # lora name -> path
        self._loading: set[str] = set()
        self._host_cache: set[str] = set()  # ever loaded on this replica
        self._busy_with: object = _IDLE
        self._events: list[dict] = []

    # ------------------------------------------------------------- interface

    def load_lora(self, name: str, path: str) -> None:
        """Load a LoRA into one of the replica's memory slots.

        Blocks for COLD_LOAD_SECONDS on first load, WARM_LOAD_SECONDS when the
        LoRA was previously loaded on this replica. May run concurrently with
        process_inputs. Loading an already-loaded LoRA is a fast no-op.
        """
        with self._lock:
            if name in self._loaded:
                self._log("load_noop", lora=name)
                return
            if name in self._loading:
                raise DuplicateLoad(f"{name} is already loading")
            if len(self._loaded) + len(self._loading) >= MAX_LORAS_IN_MEMORY:
                raise LoRACacheFull(
                    f"{MAX_LORAS_IN_MEMORY} slots in use; evict a LoRA first"
                )
            self._loading.add(name)
            tier = "warm" if name in self._host_cache else "cold"

        self._log("load_start", lora=name, tier=tier)
        self._sleep(WARM_LOAD_SECONDS if tier == "warm" else COLD_LOAD_SECONDS)

        with self._lock:
            self._loading.discard(name)
            self._loaded[name] = path
            self._host_cache.add(name)
        self._log("load_end", lora=name, tier=tier)

    def process_inputs(self, inputs: list[dict]) -> list[dict]:
        """Run one batch of inputs against a single LoRA (or the base model).

        Each input is a dict; its "lora" key (default None = base model) must
        be identical across the batch, and that LoRA must already be loaded.
        Blocks for BATCH_SECONDS regardless of how full the batch is. Only one
        batch may be in flight per replica.
        """
        if not inputs:
            raise EngineError("empty batch")
        if len(inputs) > MAX_BATCH_SIZE:
            raise BatchTooLarge(f"{len(inputs)} inputs; max is {MAX_BATCH_SIZE}")
        targets = {inp.get("lora", BASE_MODEL) for inp in inputs}
        if len(targets) > 1:
            raise MixedLoRABatch(f"one LoRA per batch; got {sorted(map(str, targets))}")
        (lora,) = targets

        with self._lock:
            if self._busy_with is not _IDLE:
                raise ReplicaBusy("a batch is already in flight on this replica")
            if lora is not BASE_MODEL and lora not in self._loaded:
                raise LoRANotLoaded(f"{lora} is not loaded on this replica")
            self._busy_with = lora

        try:
            self._log("batch_start", lora=lora, size=len(inputs))
            self._sleep(BATCH_SECONDS)
            outputs = []
            for inp in inputs:
                out = {
                    "id": inp.get("id"),
                    "lora": lora,
                    "output": f"[{lora or 'base'}] mock completion",
                }
                outputs.append(out)
                # "customer" is echoed from the input so per-customer metrics
                # can be computed from the event log alone.
                self._log(
                    "request_done",
                    lora=lora,
                    id=out["id"],
                    customer=inp.get("customer"),
                )
            self._log("batch_end", lora=lora, size=len(inputs))
            return outputs
        finally:
            with self._lock:
                self._busy_with = _IDLE

    def evict_lora(self, name: str) -> None:
        """Free the memory slot held by a LoRA. Instant.

        Cannot evict a LoRA that is currently loading or serving an in-flight
        batch. An evicted LoRA stays in host memory, so reloading it is warm.
        """
        with self._lock:
            if name in self._loading:
                raise LoRAInUse(f"{name} is still loading")
            if name not in self._loaded:
                raise LoRANotLoaded(f"{name} is not loaded on this replica")
            if self._busy_with == name:
                raise LoRAInUse(f"{name} is serving an in-flight batch")
            del self._loaded[name]
        self._log("evict", lora=name)

    # ----------------------------------------------------------- observation

    def loaded_loras(self) -> list[str]:
        """Names of the LoRAs currently resident on this replica."""
        with self._lock:
            return sorted(self._loaded)

    def event_log(self) -> list[dict]:
        """A copy of every event this replica has emitted, in order.

        Events are dicts: {"ts": <float epoch seconds>, "replica": <id>,
        "event": <name>, ...}. Event names: load_start, load_end, load_noop,
        evict, batch_start, batch_end, request_done.
        """
        with self._events_lock:
            return list(self._events)

    # -------------------------------------------------------------- internal

    def _sleep(self, seconds: float) -> None:
        time.sleep(seconds / self._sim_speed)

    def _log(self, event: str, **fields) -> None:
        record = {"ts": time.time(), "replica": self.replica_id, "event": event}
        record.update(fields)
        with self._events_lock:
            self._events.append(record)
