"""Small deterministic traces checked against the unmodified original engine.

Compare identities and batch membership rather than exact wall-clock timing.
This supplements virtual experiments; it does not validate a physical GPU model.
"""
from collections import Counter
from pathlib import Path
import tempfile
import time
import unittest

from audit import audit_events, audit_requests, crosscheck_engine, engine_time_summary
from backends import ThreadedBackend, VirtualBackend
from scheduler import Config, Request, Scheduler
from service import ProductService
from store import Store


class BackendAgreement(unittest.TestCase):
    def execute(self, config, requests, threaded):
        backend = (ThreadedBackend(config.replicas, speed=10) if threaded else
                   VirtualBackend(config.replicas))
        scheduler = Scheduler(config, backend)
        accepted = set()
        try:
            # Submit the complete time-zero snapshot before either backend ticks:
            # wall jitter cannot change the intended coalescing opportunities.
            for request in requests:
                ok, reason = scheduler.submit(request)
                self.assertTrue(ok, (request.id, reason))
                accepted.add(request.id)
            wall_deadline = time.monotonic()+5
            while scheduler.pending:
                self.assertLess(time.monotonic(), wall_deadline, "original engine did not drain")
                scheduler.tick()
                if scheduler.pending:
                    wake = scheduler.next_wakeup()
                    self.assertIsNotNone(wake, "accepted work has no wakeup")
                    if threaded:
                        time.sleep(.0005)
                    else:
                        backend.advance(max(backend.now, wake))
            if not threaded:
                while backend.next_event() is not None:
                    backend.advance(backend.next_event())
                    backend.poll()
            backend.close()  # Finish speculative original-engine loads as well.
            events = backend.event_log()
            audited = audit_events(events)
            rows = [(request.ingress, request.customer, request.lora) for request in requests]
            crosscheck_engine(rows, accepted, scheduler.completed, audited)
            conservation = audit_requests(rows, accepted, scheduler.completed, config.slo)
            self.assertEqual(conservation["completed"], len(requests))
            self.assertEqual((conservation["failed"], conservation["deadline_misses"], conservation["outstanding"]), (0,0,0))
            for request in requests:
                self.assertLessEqual(scheduler.completed[request.id]["end"], request.deadline)
            shapes = Counter((event.get("lora"), event["size"]) for event in events if event["event"] == "batch_start")
            return {"accepted": accepted, "statuses": {key: outcome["status"] for key, outcome in scheduler.completed.items()},
                    "shape": shapes, "audit": audited,
                    "time": engine_time_summary(audited, 0, backend.now, config.replicas)}
        finally:
            backend.close()

    def compare(self, config, requests):
        virtual = self.execute(config, requests, False)
        original = self.execute(config, requests, True)
        self.assertEqual(virtual["accepted"], original["accepted"])
        self.assertEqual(virtual["statuses"], original["statuses"])
        self.assertEqual(virtual["shape"], original["shape"])
        return virtual, original

    def test_prefetch_load_and_compute_overlap_match_original_engine(self):
        config = Config(replicas=1, policy="locality", prefetch=True,
                        max_batch_wait=0, starvation_threshold=3, slo=30,
                        controller_margin=.1, response_margin=.1)
        requests = [Request(str(i), customer, lora, "/weights/"+lora if lora else None,
                            "hello", 0, 30)
                    for i, (customer,lora) in enumerate((("a",None), ("b","cold-a"),
                                                        ("b","cold-a"), ("a",None), ("b","cold-b")))]
        virtual, original = self.compare(config, requests)
        self.assertEqual(virtual["shape"], Counter({(None,2): 1, ("cold-a",2): 1, ("cold-b",1): 1}))
        for result in (virtual, original):
            self.assertGreaterEqual(result["audit"]["overlapped_loads"], 1)
            self.assertGreater(result["time"]["overlapped_load_seconds"], 0)

    def test_edf_early_flush_and_partial_coalescing_match_original_engine(self):
        config = Config(replicas=1, policy="fair", max_batch_wait=10,
                        starvation_threshold=15, slo=30,
                        controller_margin=.15, response_margin=.15)
        requests = [Request("0", "urgent", None, None, "urgent", 0, 2),
                    Request("1", "later", "cold", "/weights/cold", "later", 0, 20),
                    Request("2", "urgent", None, None, "joined", 0, 2)]
        virtual, _ = self.compare(config, requests)
        self.assertEqual(virtual["shape"], Counter({(None,2): 1, ("cold",1): 1}))
        self.assertLess(virtual["audit"]["request_starts"]["0"], config.max_batch_wait)

    def test_single_young_partial_dispatches_immediately_on_both_backends(self):
        config=Config(replicas=1,policy='fair',max_batch_wait=120,eligibility='pending',
                      controller_margin=.5,response_margin=.5)
        requests=[Request('0','customer','cold','/weights/cold','hello',0,30)]
        virtual,original=self.compare(config,requests)
        self.assertEqual(virtual['audit']['request_starts']['0'],2)
        self.assertLess(original['audit']['request_starts']['0'],5)


class OriginalEngineRecovery(unittest.TestCase):
    def test_epoch_restart_keeps_pinned_version_original_deadline_and_idempotency(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory)/"durable.sqlite")
            store = Store(path)
            key = store.provision_customer("customer")
            config = Config(replicas=4, max_batch_wait=0, starvation_threshold=3,
                            slo=30, controller_margin=.15, response_margin=.15)
            first_backend = ThreadedBackend(4, speed=10, origin=store.clock_origin(10))
            first = ProductService(store, Scheduler(config,first_backend), slo=20)
            first.register_version(key,"support","v1","s3://v1")
            first.register_version(key,"support","v2","s3://v2")
            first.release(key,"stable","support","v1",0,"first")
            payload = {"model":"endpoint:stable","prompt":"keep original weights"}
            _, accepted = first.infer(key,payload,"same-logical-request")
            first.release(key,"stable","support","v2",1,"promote")
            before_restart = first_backend.now
            first.close()  # Stop before any dispatch; durable pending survives.
            store.close()

            time.sleep(.05)  # Half a logical second must count toward the SLO.
            recovered_store = Store(path)
            backend = ThreadedBackend(4,speed=10,origin=recovered_store.clock_origin(10))
            self.assertGreaterEqual(backend.now-before_restart,.4)
            self.assertLess(accepted["deadline"]-backend.now,accepted["deadline"]-before_restart-.4)
            recovered = ProductService(recovered_store,Scheduler(config,backend),slo=999,poll_interval=.001)
            try:
                replay = recovered.get_request(key,accepted["id"])
                self.assertGreater(replay["ingress"], 1_000_000_000)
                self.assertEqual((replay["id"],replay["ingress"],replay["deadline"],replay["resolved_model"]),
                                 (accepted["id"],accepted["ingress"],accepted["deadline"],"lora:support@v1"))
                self.assertGreater(replay["attempt"],accepted["attempt"])
                self.assertEqual(recovered.infer(key,payload,"same-logical-request")[1]["id"], accepted["id"])
                recovered.start_worker()
                until = time.monotonic()+3
                while time.monotonic() < until:
                    outcome = recovered.get_request(key,accepted["id"])
                    if outcome["status"] in ("completed","failed"):
                        break
                    time.sleep(.002)
                self.assertEqual(outcome["status"],"completed",outcome)
                self.assertFalse(outcome["slo_violation"])
                self.assertEqual(outcome["deadline"],accepted["deadline"])
                self.assertIn('"v1"',outcome["result"]["lora"])
                audited = audit_events(backend.event_log())
                self.assertEqual(audited["completed"],1)
                self.assertEqual(set(audited["requests"]),{accepted["id"]})
            finally:
                recovered.close()
                recovered_store.close()


if __name__ == "__main__":
    unittest.main()
