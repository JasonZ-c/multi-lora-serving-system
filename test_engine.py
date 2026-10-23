"""Tests for the mock inference engine.

These tests are the executable version of the contract in the README: every
limit the engine enforces is exercised here. Run with:

    python3 -m unittest -v
"""

import threading
import time
import unittest

from engine import (
    BATCH_SECONDS,
    COLD_LOAD_SECONDS,
    MAX_BATCH_SIZE,
    MAX_LORAS_IN_MEMORY,
    WARM_LOAD_SECONDS,
    BatchTooLarge,
    DuplicateLoad,
    EngineError,
    InferenceEngine,
    LoRACacheFull,
    LoRAInUse,
    LoRANotLoaded,
    MixedLoRABatch,
    ReplicaBusy,
)

# Fast enough that the suite runs in seconds, slow enough that timing
# relationships (cold > warm, overlap) are still measurable.
SIM_SPEED = 50.0


def make_engine(replica_id="r0"):
    return InferenceEngine(replica_id, sim_speed=SIM_SPEED)


def inputs_for(lora, n, start_id=0):
    return [{"id": start_id + i, "lora": lora, "prompt": f"p{i}"} for i in range(n)]


class BatchLimits(unittest.TestCase):
    def test_base_model_needs_no_load(self):
        engine = make_engine()
        outputs = engine.process_inputs([{"id": 1, "prompt": "hi"}])
        self.assertEqual(outputs[0]["lora"], None)

    def test_batch_size_is_capped(self):
        engine = make_engine()
        with self.assertRaises(BatchTooLarge):
            engine.process_inputs(inputs_for(None, MAX_BATCH_SIZE + 1))

    def test_full_batch_is_accepted(self):
        engine = make_engine()
        outputs = engine.process_inputs(inputs_for(None, MAX_BATCH_SIZE))
        self.assertEqual(len(outputs), MAX_BATCH_SIZE)

    def test_empty_batch_rejected(self):
        engine = make_engine()
        with self.assertRaises(EngineError):
            engine.process_inputs([])

    def test_one_lora_per_batch(self):
        engine = make_engine()
        engine.load_lora("a", "s3://a")
        engine.load_lora("b", "s3://b")
        with self.assertRaises(MixedLoRABatch):
            engine.process_inputs(inputs_for("a", 2) + inputs_for("b", 2))

    def test_unloaded_lora_rejected(self):
        engine = make_engine()
        with self.assertRaises(LoRANotLoaded):
            engine.process_inputs(inputs_for("ghost", 1))

    def test_batch_takes_fixed_time_regardless_of_fill(self):
        engine = make_engine()
        expected = BATCH_SECONDS / SIM_SPEED

        start = time.monotonic()
        engine.process_inputs(inputs_for(None, 1))
        small = time.monotonic() - start

        start = time.monotonic()
        engine.process_inputs(inputs_for(None, MAX_BATCH_SIZE))
        full = time.monotonic() - start

        self.assertGreaterEqual(small, expected)
        self.assertGreaterEqual(full, expected)
        # A full batch costs the same as a batch of one (within scheduling noise).
        self.assertLess(abs(full - small), expected)

    def test_one_batch_in_flight_per_replica(self):
        engine = make_engine()
        errors = []
        started = threading.Event()

        def run_first():
            started.set()
            engine.process_inputs(inputs_for(None, 1))

        t = threading.Thread(target=run_first)
        t.start()
        started.wait()
        time.sleep(BATCH_SECONDS / SIM_SPEED / 4)  # first batch now in flight
        try:
            engine.process_inputs(inputs_for(None, 1))
        except ReplicaBusy as e:
            errors.append(e)
        t.join()
        self.assertEqual(len(errors), 1)


class LoadingAndEviction(unittest.TestCase):
    def test_cache_capacity_is_enforced_without_auto_eviction(self):
        engine = make_engine()
        for i in range(MAX_LORAS_IN_MEMORY):
            engine.load_lora(f"lora-{i}", f"s3://lora-{i}")
        with self.assertRaises(LoRACacheFull):
            engine.load_lora("one-too-many", "s3://x")
        engine.evict_lora("lora-0")
        engine.load_lora("one-too-many", "s3://x")  # now it fits

    def test_warm_reload_is_faster_than_cold(self):
        engine = make_engine()

        start = time.monotonic()
        engine.load_lora("a", "s3://a")
        cold = time.monotonic() - start

        engine.evict_lora("a")

        start = time.monotonic()
        engine.load_lora("a", "s3://a")
        warm = time.monotonic() - start

        self.assertGreaterEqual(cold, COLD_LOAD_SECONDS / SIM_SPEED)
        self.assertGreaterEqual(warm, WARM_LOAD_SECONDS / SIM_SPEED)
        self.assertLess(warm, cold)

    def test_loading_a_loaded_lora_is_a_fast_noop(self):
        engine = make_engine()
        engine.load_lora("a", "s3://a")
        start = time.monotonic()
        engine.load_lora("a", "s3://a")
        self.assertLess(time.monotonic() - start, WARM_LOAD_SECONDS / SIM_SPEED)

    def test_concurrent_duplicate_load_rejected(self):
        engine = make_engine()
        started = threading.Event()

        def load():
            started.set()
            engine.load_lora("a", "s3://a")

        t = threading.Thread(target=load)
        t.start()
        started.wait()
        time.sleep(COLD_LOAD_SECONDS / SIM_SPEED / 4)  # load now in progress
        with self.assertRaises(DuplicateLoad):
            engine.load_lora("a", "s3://a")
        t.join()

    def test_evicting_unknown_lora_rejected(self):
        engine = make_engine()
        with self.assertRaises(LoRANotLoaded):
            engine.evict_lora("ghost")

    def test_cannot_evict_lora_serving_a_batch(self):
        engine = make_engine()
        engine.load_lora("a", "s3://a")
        started = threading.Event()

        def run_batch():
            started.set()
            engine.process_inputs(inputs_for("a", 1))

        t = threading.Thread(target=run_batch)
        t.start()
        started.wait()
        time.sleep(BATCH_SECONDS / SIM_SPEED / 4)
        with self.assertRaises(LoRAInUse):
            engine.evict_lora("a")
        t.join()

    def test_load_may_overlap_an_in_flight_batch(self):
        engine = make_engine()
        engine.load_lora("a", "s3://a")
        batch_done = []

        def run_batch():
            engine.process_inputs(inputs_for("a", 1))
            batch_done.append(time.monotonic())

        t = threading.Thread(target=run_batch)
        t.start()
        time.sleep(BATCH_SECONDS / SIM_SPEED / 4)
        engine.load_lora("b", "s3://b")  # loads while the batch is running
        load_done = time.monotonic()
        t.join()
        self.assertIn("b", engine.loaded_loras())
        # The cold load (2.0s) outlasts the batch (1.25s) it started inside of,
        # which is only possible if the two genuinely overlapped.
        self.assertLess(batch_done[0], load_done)


class EventLog(unittest.TestCase):
    def test_lifecycle_is_recorded_in_order(self):
        engine = make_engine()
        engine.load_lora("a", "s3://a")
        engine.process_inputs(inputs_for("a", 2))
        engine.evict_lora("a")

        names = [e["event"] for e in engine.event_log()]
        self.assertEqual(
            names,
            [
                "load_start",
                "load_end",
                "batch_start",
                "request_done",
                "request_done",
                "batch_end",
                "evict",
            ],
        )

    def test_events_carry_timestamps_and_replica_id(self):
        engine = make_engine("replica-7")
        engine.process_inputs(inputs_for(None, 1))
        log = engine.event_log()
        self.assertTrue(all(e["replica"] == "replica-7" for e in log))
        timestamps = [e["ts"] for e in log]
        self.assertEqual(timestamps, sorted(timestamps))

    def test_request_done_events_carry_request_ids(self):
        engine = make_engine()
        engine.process_inputs(inputs_for(None, 3, start_id=100))
        done = [e for e in engine.event_log() if e["event"] == "request_done"]
        self.assertEqual([e["id"] for e in done], [100, 101, 102])

    def test_request_done_events_echo_customer(self):
        engine = make_engine()
        engine.process_inputs(
            [
                {"id": 1, "customer": "acme", "prompt": "a"},
                {"id": 2, "prompt": "b"},  # no customer field
            ]
        )
        done = [e for e in engine.event_log() if e["event"] == "request_done"]
        self.assertEqual([e["customer"] for e in done], ["acme", None])


if __name__ == "__main__":
    unittest.main()
