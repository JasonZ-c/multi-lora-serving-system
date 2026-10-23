"""Product contracts, durable recovery, and real HTTP authentication tests."""
import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

from store import APIError, Store
from unittest.mock import patch


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.directory.name, "product.sqlite")
        self.store = Store(self.path)
        self.key = self.store.provision_customer("alice", "alice-key")
        self.other = self.store.provision_customer("bob", "bob-key")

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def test_clock_anchor_persists_and_accounts_for_downtime_at_previous_speed(self):
        with patch("time.time", return_value=1000):
            self.assertEqual(self.store.clock_origin(10),1000)
        self.store.close()
        self.store = Store(self.path)
        with patch("time.time", return_value=1005):
            self.assertEqual(self.store.clock_origin(10),1050)
        with patch("time.time", return_value=1007):
            self.assertEqual(self.store.clock_origin(2),1070)
        with patch("time.time", return_value=1008):
            self.assertEqual(self.store.clock_origin(100),1072)
        with patch("time.time", return_value=1008):
            self.assertEqual(self.store.clock_origin(1),1072)

    def test_clock_anchor_clamps_backward_wall_jumps_and_validates_speed(self):
        with patch("time.time", return_value=1000):
            self.store.clock_origin(10)
        with patch("time.time", return_value=990):
            self.assertEqual(self.store.clock_origin(2),1000)
        with patch("time.time", return_value=1001):
            self.assertEqual(self.store.clock_origin(2),1002)
        for speed in (0,-1,float("inf"),float("nan")):
            with self.assertRaises(ValueError):
                self.store.clock_origin(speed)

    def test_credentials_are_hashed_and_survive_restart(self):
        self.assertEqual(self.store.authenticate(self.key), "alice")
        self.assertNotIn(self.key, " ".join(str(tuple(r)) for r in self.store.connection.execute("SELECT * FROM credentials")))
        self.store.close()
        self.store = Store(self.path)
        self.assertEqual(self.store.authenticate(self.key), "alice")
        with self.assertRaises(APIError) as caught:
            self.store.authenticate("wrong")
        self.assertEqual(caught.exception.status, 401)

    def test_version_is_immutable_customer_scoped_and_idempotent(self):
        first = self.store.register_version("alice", "support", "v1", "s3://alice/v1")
        self.assertEqual(first, self.store.register_version("alice", "support", "v1", "s3://alice/v1"))
        with self.assertRaises(APIError) as caught:
            self.store.register_version("alice", "support", "v1", "s3://changed")
        self.assertEqual(caught.exception.status, 409)
        with self.assertRaises(APIError) as caught:
            self.store.resolve("bob", "lora:support@v1")
        self.assertEqual(caught.exception.status, 404)
        resolved = self.store.resolve("alice", "lora:support@v1")
        self.assertEqual(resolved["path"], "s3://alice/v1")
        self.assertIsNone(self.store.resolve("alice", "base")["lora"])

    def test_releases_compare_revision_replay_and_rollback(self):
        for version in ("v1", "v2"):
            self.store.register_version("alice", "support", version, "s3://" + version)
        first = self.store.release("alice", "stable", "support", "v1", 0, "release-1")
        self.assertEqual(first["revision"], 1)
        second = self.store.release("alice", "stable", "support", "v2", 1, "release-2")
        self.assertEqual(second["revision"], 2)
        self.assertEqual(first, self.store.release("alice", "stable", "support", "v1", 0, "release-1"))
        with self.assertRaises(APIError) as caught:
            self.store.release("alice", "stable", "support", "v2", 0, "stale")
        self.assertEqual(caught.exception.status, 409)
        with self.assertRaises(APIError):
            self.store.release("alice", "stable", "support", "v2", 1, "release-1")
        self.store.release("alice", "stable", "support", "v1", 2, "rollback")
        self.assertEqual(self.store.resolve("alice", "endpoint:stable")["model"], "lora:support@v1")


class ManualBackend:
    def __init__(self, now=100.0):
        self.now = now


class RecordingScheduler:
    """Deterministic admission boundary; real scheduler has separate contract tests."""
    def __init__(self, now=100.0, accept=True):
        self.backend = ManualBackend(now)
        self.accept = accept
        self.requests = {}
        self.completed = {}

    def submit(self, request):
        if not self.accept:
            return False, "conservative_capacity"
        self.requests[request.id] = request
        return True, "accepted"

    def cancel(self, request_id):
        return self.requests.pop(request_id, None) is not None

    def tick(self):
        pass

    def request_status(self, request_id):
        return {"status": "queued"}


class ProductTests(unittest.TestCase):
    def setUp(self):
        StoreTests.setUp(self)
        from service import ProductService
        self.scheduler = RecordingScheduler()
        self.service = ProductService(self.store, self.scheduler, slo=20)
        self.service.register_version(self.key, "support", "v1", "s3://v1")
        self.service.register_version(self.key, "support", "v2", "s3://v2")
        self.service.release(self.key, "stable", "support", "v1", 0, "first")

    def tearDown(self):
        self.service.close()
        StoreTests.tearDown(self)

    def infer(self, **changes):
        payload = {"model": "endpoint:stable", "prompt": "hello", "input_tokens": 10, "max_output_tokens": 5}
        payload.update(changes)
        return self.service.infer(self.key, payload, "logical-1")

    def test_acceptance_is_durable_and_idempotency_pins_original_route(self):
        status, first = self.infer()
        self.assertEqual(status, 202)
        self.assertEqual(first["resolved_model"], "lora:support@v1")
        self.assertEqual(self.store.connection.execute("SELECT status FROM requests WHERE id=?", (first["id"],)).fetchone()[0], "queued")
        self.service.release(self.key, "stable", "support", "v2", 1, "next")
        self.scheduler.backend.now = 105
        self.assertEqual(first, self.infer()[1])
        self.assertEqual(len(self.scheduler.requests), 1)
        with self.assertRaises(APIError) as caught:
            self.infer(prompt="changed")
        self.assertEqual(caught.exception.status, 409)
        request = self.scheduler.requests[first["id"]]
        self.assertEqual((request.ingress, request.deadline, request.path), (100, 120, "s3://v1"))

    def test_concurrent_idempotency_is_customer_scoped(self):
        accepted = []
        failures = []
        barrier = threading.Barrier(8)
        def submit():
            try:
                barrier.wait()
                accepted.append(self.service.infer(self.key, {"model": "base"}, "shared-key")[1])
            except Exception as error:
                failures.append(error)
        threads = [threading.Thread(target=submit) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
        self.assertEqual(failures, [])
        self.assertEqual(len(accepted), 8)
        self.assertEqual(len({row["id"] for row in accepted}), 1)
        other = self.service.infer(self.other, {"model": "base"}, "shared-key")[1]
        self.assertNotEqual(other["id"], accepted[0]["id"])
        self.assertEqual(len(self.scheduler.requests), 2)

    def test_recovery_across_new_sqlite_connection_retains_payload_and_identity(self):
        from service import ProductService
        _, original = self.infer()
        separate_store = Store(self.path)
        scheduler = RecordingScheduler(now=110)
        recovered = ProductService(separate_store, scheduler, slo=200)
        try:
            replay = scheduler.requests[original["id"]]
            self.assertEqual((replay.ingress, replay.deadline, replay.path, replay.prompt), (100,120,"s3://v1","hello"))
            status, same = recovered.infer(self.key, {"model": "endpoint:stable", "prompt": "hello", "input_tokens": 10, "max_output_tokens": 5}, "logical-1")
            self.assertEqual((status, same["id"]), (202, original["id"]))
        finally:
            recovered.close()
            separate_store.close()

    def test_auth_ownership_context_and_capacity_rejections(self):
        with self.assertRaises(APIError) as caught:
            self.service.infer("invalid", {"model": "base"})
        self.assertEqual(caught.exception.status, 401)
        with self.assertRaises(APIError) as caught:
            self.service.infer(self.other, {"model": "lora:support@v1"})
        self.assertEqual(caught.exception.status, 404)
        for field, value in (("input_tokens", True), ("input_tokens", -1), ("max_output_tokens", 0), ("max_output_tokens", 32769)):
            with self.assertRaises(APIError):
                self.infer(**{field: value})
        status, first = self.infer(input_tokens=32763, max_output_tokens=5)
        self.assertEqual(status, 202)
        with self.assertRaises(APIError) as caught:
            self.service.get_request(self.other, first["id"])
        self.assertEqual(caught.exception.status, 404)
        self.scheduler.accept = False
        with self.assertRaises(APIError) as caught:
            self.service.infer(self.key, {"model": "base"}, "another")
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(self.store.connection.execute("SELECT COUNT(*) FROM requests").fetchone()[0], 1)

    def test_single_terminal_outcome_and_late_success_is_visible_violation(self):
        _, request = self.infer()
        self.scheduler.completed[request["id"]] = {"status": "completed", "output": {"output": "ok"}, "end": 121, "attempt": 1}
        self.service.tick()
        outcome = self.service.get_request(self.key, request["id"])
        self.assertEqual(outcome["status"], "completed")
        self.assertTrue(outcome["slo_violation"])
        self.assertEqual(outcome["result"], {"output": "ok"})
        self.scheduler.completed[request["id"]] = {"status": "failed", "error": "stale", "end": 122, "attempt": 1}
        self.service.tick()
        self.assertEqual(self.service.get_request(self.key, request["id"]), outcome)

    def test_restart_preserves_deadline_version_and_fences_old_actor(self):
        from service import ProductService
        _, request = self.infer()
        self.service.release(self.key, "stable", "support", "v2", 1, "new-version")
        replacement = RecordingScheduler(now=110)
        new_service = ProductService(self.store, replacement, slo=200)
        try:
            replay = replacement.requests[request["id"]]
            self.assertEqual((replay.ingress, replay.deadline, replay.path), (100, 120, "s3://v1"))
            self.assertGreater(replay.attempt, self.scheduler.requests[request["id"]].attempt)
            self.scheduler.completed[request["id"]] = {"status": "completed", "output": {"output": "stale"}, "end": 111, "attempt": 1}
            self.service.tick()
            self.assertEqual(new_service.get_request(self.key, request["id"])["status"], "queued")
            replacement.completed[request["id"]] = {"status": "completed", "output": {"output": "fresh"}, "end": 112, "attempt": replay.attempt}
            new_service.tick()
            self.assertEqual(new_service.get_request(self.key, request["id"])["result"], {"output": "fresh"})
        finally:
            new_service.close()

    def test_expired_or_infeasible_recovery_becomes_durable_failure(self):
        from service import ProductService
        _, request = self.infer()
        replacement = ProductService(self.store, RecordingScheduler(now=121), slo=999)
        try:
            outcome = replacement.get_request(self.key, request["id"])
            self.assertEqual(outcome["status"], "failed")
            self.assertTrue(outcome["slo_violation"])
            self.assertIn("recovery", outcome["error"])
        finally:
            replacement.close()


class HTTPTests(unittest.TestCase):
    def setUp(self):
        from http_api import make_server
        from service import ProductService
        StoreTests.setUp(self)
        self.scheduler = RecordingScheduler()
        self.service = ProductService(self.store, self.scheduler, slo=20)
        self.server = make_server(self.service, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_address[1])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.service.close()
        StoreTests.tearDown(self)

    def call(self, path, payload=None, key="alice-key", headers=None):
        request_headers = {"Content-Type": "application/json"}
        if key is not None:
            request_headers["Authorization"] = "Bearer " + key
        request_headers.update(headers or {})
        request = urllib.request.Request(self.url+path, json.dumps(payload).encode() if payload is not None else None, request_headers)
        try:
            response = urllib.request.urlopen(request, timeout=3)
        except urllib.error.HTTPError as response_error:
            response = response_error
        with response:
            return response.status, json.load(response)

    def test_http_auth_registration_release_infer_and_owned_polling(self):
        self.assertEqual(self.call("/v1/loras/support/versions", {"version": "v1", "blob_path": "s3://v1"}, key=None)[0], 401)
        self.assertEqual(self.call("/v1/loras/support/versions", {"version": "v1", "blob_path": "s3://v1"})[0], 201)
        self.assertEqual(self.call("/v1/endpoints/support/releases", {"version": "v1", "expected_revision": 0, "release_id": "release"})[0], 200)
        status, first = self.call("/v1/inferences", {"model": "endpoint:support", "prompt": "hello"}, headers={"Idempotency-Key": "http-first"})
        self.assertEqual(status, 202)
        self.assertEqual(self.call("/v1/requests/"+first["id"], key=None)[0], 401)
        self.assertEqual(self.call("/v1/requests/"+first["id"], key=self.other)[0], 404)
        self.assertEqual(self.call("/v1/requests/"+first["id"]), (200, first))
        self.assertEqual(self.call("/v1/inferences", {"model": "endpoint:support", "prompt": "different"}, headers={"Idempotency-Key": "http-first"})[0], 409)
        self.assertEqual(self.call("/v1/inferences", ["bad", "body"])[0], 422)

    def test_http_real_engine_worker_returns_completion(self):
        from backends import ThreadedBackend
        from scheduler import Config, Scheduler
        from service import ProductService
        live_backend = ThreadedBackend(replicas=4, speed=50, origin=time.time())
        live = ProductService(self.store, Scheduler(Config(max_batch_wait=0, starvation_threshold=1, slo=20, controller_margin=.1, response_margin=.1), live_backend), slo=20, recover=False)
        self.server.product_service = live
        live.start_worker()
        try:
            status, request = self.call("/v1/inferences", {"model": "base", "prompt": "live"})
            self.assertEqual(status, 202)
            deadline = time.monotonic()+3
            while time.monotonic() < deadline:
                status, outcome = self.call("/v1/requests/"+request["id"])
                if outcome["status"] in ("completed", "failed"):
                    break
                time.sleep(.01)
            self.assertEqual(outcome["status"], "completed", outcome)
            self.assertFalse(outcome["slo_violation"])
            self.assertIn("mock completion", str(outcome["result"]))
        finally:
            self.server.product_service = self.service
            live.close()


class HTTPBindingTests(unittest.TestCase):
    def test_local_bind_needs_no_reverse_dns(self):
        from http_api import make_server
        with patch("socket.getfqdn", side_effect=RuntimeError("reverse DNS unavailable")):
            server = make_server(None, port=0)
        server.server_close()


class PublicationTests(unittest.TestCase):
    setUp = ProductTests.setUp
    tearDown = ProductTests.tearDown
    infer = ProductTests.infer

    def test_delayed_poll_counts_result_availability_against_original_deadline(self):
        _, request = self.infer()
        self.scheduler.completed[request["id"]] = {"status": "completed", "output": {"output": "ok"}, "end": 119, "attempt": 1}
        self.scheduler.backend.now = 121
        self.service.tick()
        result = self.service.get_request(self.key, request["id"])
        self.assertGreaterEqual(result["completed_at"], 121)
        self.assertTrue(result["slo_violation"])

    def test_durable_commit_delay_counts_against_deadline(self):
        _, request = self.infer()
        self.scheduler.completed[request["id"]] = {"status": "completed", "output": {"output": "ok"}, "end": 118, "attempt": 1}
        self.scheduler.backend.now = 119
        delayed = [False]
        def trace(sql):
            if sql == "COMMIT" and not delayed[0]:
                delayed[0] = True
                self.scheduler.backend.now = 121
        self.store.connection.set_trace_callback(trace)
        self.service.tick()
        self.store.connection.set_trace_callback(None)
        result = self.service.get_request(self.key, request["id"])
        self.assertGreaterEqual(result["completed_at"], 121)
        self.assertTrue(result["slo_violation"])

    def test_delayed_final_metadata_commit_is_observed_and_violation_survives_restart(self):
        from service import ProductService
        _, request = self.infer()
        self.scheduler.completed[request["id"]] = {"status": "completed", "output": {"output": "ok"}, "end": 118, "attempt": 1}
        self.scheduler.backend.now = 119
        commits = [0]
        def trace(sql):
            if sql == "COMMIT":
                commits[0] += 1
                if commits[0] == 1:
                    self.scheduler.backend.now = 119.5
                elif commits[0] == 2:
                    self.scheduler.backend.now = 121
        self.store.connection.set_trace_callback(trace)
        self.service.tick()
        self.store.connection.set_trace_callback(None)
        result = self.service.get_request(self.key,request["id"])
        self.assertGreaterEqual(commits[0],2)
        self.assertGreaterEqual(result["completed_at"],121)
        self.assertTrue(result["slo_violation"])
        second_store = Store(self.path)
        second = ProductService(second_store,RecordingScheduler(now=122))
        try:
            self.assertTrue(second.get_request(self.key,request["id"])["slo_violation"])
        finally:
            second.close()
            second_store.close()

    def test_ingress_precedes_wait_for_actor_lock(self):
        result = []
        self.service.lock.acquire()
        thread = threading.Thread(target=lambda: result.append(self.infer()[1]))
        try:
            thread.start()
            time.sleep(.02)
            self.scheduler.backend.now = 105
        finally:
            self.service.lock.release()
        thread.join(timeout=2)
        self.assertEqual(result[0]["ingress"], 100)
        self.assertEqual(result[0]["deadline"], 120)


class FailureTests(unittest.TestCase):
    setUp = ProductTests.setUp
    tearDown = ProductTests.tearDown
    infer = ProductTests.infer

    def test_durability_failure_releases_unstarted_admission(self):
        self.store.connection.execute("CREATE TRIGGER deny_accept BEFORE INSERT ON requests BEGIN SELECT RAISE(ABORT, 'disk write failed'); END")
        with self.assertRaises(APIError) as caught:
            self.infer()
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(self.scheduler.requests, {})
        self.assertEqual(self.store.pending_requests(), [])

    def test_actor_failure_is_visible_terminal_failure_and_rejects_new_work(self):
        _, request = self.infer()
        def fail():
            raise RuntimeError("actor fault")
        self.scheduler.tick = fail
        self.service.start_worker()
        self.service._worker.join(timeout=1)
        outcome = self.service.get_request(self.key, request["id"])
        self.assertEqual(outcome["status"], "failed")
        self.assertTrue(outcome["slo_violation"])
        self.assertIn("actor_failure", outcome["error"])
        with self.assertRaises(APIError) as caught:
            self.service.infer(self.key, {"model": "base"}, "new-after-fault")
        self.assertEqual(caught.exception.status, 503)

    def test_storage_fault_leaves_recoverable_work_with_visible_worker_failure(self):
        import sqlite3
        _, request = self.infer()
        def fail():
            raise sqlite3.OperationalError("storage fault")
        self.scheduler.tick = fail
        original_finish = self.store.finish
        self.store.finish = lambda *args, **kwargs: fail()
        try:
            self.service.start_worker()
            self.service._worker.join(timeout=1)
            outcome = self.service.get_request(self.key, request["id"])
            self.assertEqual(outcome["status"], "queued")
            self.assertIn("restart", outcome["worker_error"])
        finally:
            self.store.finish = original_finish

    def test_idempotent_retry_checkpoints_existing_durable_work(self):
        _, original = self.infer()
        self.scheduler.completed[original["id"]] = {"status":"completed","output":{"output":"ok"},"end":101,"attempt":1}
        self.scheduler.backend.now = 101
        _, result = self.infer()
        self.assertEqual(result["id"],original["id"])
        self.assertEqual(result["status"],"completed")
        self.assertEqual(len(self.scheduler.requests),1)

    def test_inference_checkpoint_failure_rejects_current_admission_and_exposes_fault(self):
        _, original = self.infer()
        def fail():
            raise RuntimeError("controller fault during admission")
        self.scheduler.tick = fail
        self.scheduler.backend.now = 101
        with self.assertRaises(APIError) as caught:
            self.service.infer(self.key,{"model":"base"},"after-controller-fault")
        self.assertEqual(caught.exception.status,503)
        self.assertIsNotNone(self.service.worker_error)
        self.assertEqual(set(self.scheduler.requests),{original["id"]})
        result = self.service.get_request(self.key,original["id"])
        self.assertEqual(result["status"],"failed")
        self.assertTrue(result["slo_violation"])

    def test_unsupported_payload_fields_are_rejected(self):
        with self.assertRaises(APIError) as caught:
            self.infer(unknown="changed")
        self.assertEqual(caught.exception.status, 422)


class AdmissionProgressTests(unittest.TestCase):
    def test_continuous_admissions_checkpoint_only_previously_durable_work(self):
        from types import SimpleNamespace
        from service import ProductService
        with tempfile.TemporaryDirectory() as directory:
            store = Store(os.path.join(directory,"progress.sqlite"))
            key = store.provision_customer("customer")
            checked = []
            class SlowAdmissions(RecordingScheduler):
                config = SimpleNamespace(controller_margin=.4)
                def submit(self,request):
                    self.backend.now += .02
                    return super().submit(request)
                def tick(inner):
                    durable = {row[0] for row in store.connection.execute("SELECT id FROM requests")}
                    self.assertLessEqual(set(inner.requests),durable,"checkpoint dispatched an uncommitted reservation")
                    checked.append(inner.backend.now)
                    for request_id,request in inner.requests.items():
                        if request_id not in inner.completed and inner.backend.now >= request.ingress+1:
                            inner.completed[request_id] = {"status":"completed","output":{"output":"ok"},"end":inner.backend.now,"attempt":request.attempt}
            scheduler = SlowAdmissions()
            service = ProductService(store,scheduler,slo=2)
            try:
                accepted = [service.infer(key,{"model":"base"},str(i))[1] for i in range(240)]
                for _ in range(12):
                    scheduler.backend.now += .1
                    service.tick()
                outcomes = [service.get_request(key,row["id"]) for row in accepted]
                self.assertGreater(len(checked),20,"admission flood starved the controller")
                self.assertTrue(all(row["status"] == "completed" for row in outcomes))
                self.assertEqual(sum(row["slo_violation"] for row in outcomes),0)
            finally:
                service.close()
                store.close()


if __name__ == "__main__":
    unittest.main()
