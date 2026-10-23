"""Serialized product actor connecting durable requests to a replaceable scheduler.

SQLite transactions contain metadata updates only. Engine work runs in the
backend's workers; neither an HTTP handler nor a transaction waits for inference.
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import uuid

from store import APIError, Store, canonical


class ProductService:
    def __init__(self, store: Store, scheduler, *, clock=None, slo=900.0,
                 poll_interval=0.01, recover=True):
        if not math.isfinite(slo) or slo <= 0:
            raise ValueError("slo must be finite and positive")
        self.store = store
        self.scheduler = scheduler
        self.clock = clock or (lambda: self.scheduler.backend.now)
        self.slo = float(slo)
        self.poll_interval = poll_interval
        self.lock = threading.RLock()
        self.active_attempts = {}
        self._publication_times = {}
        margin = getattr(getattr(scheduler, "config", None), "controller_margin", 0)
        logical_poll = poll_interval * getattr(scheduler.backend, "speed", 1)
        self._control_budget = margin / 4 if margin > 0 else max(logical_poll, 1e-6)
        self._last_control = self.clock()
        self._stop = threading.Event()
        self._worker = None
        self.worker_error = None
        if recover:
            self.recover()

    def register_version(self, key, name, version, blob_path):
        with self.lock:
            customer = self.store.authenticate(key)
            return self.store.register_version(customer, name, version, blob_path)

    def release(self, key, name, lora_name, version, expected_revision, release_id):
        with self.lock:
            customer = self.store.authenticate(key)
            return self.store.release(customer, name, lora_name, version, expected_revision, release_id)

    @staticmethod
    def _payload(payload):
        if not isinstance(payload, dict):
            raise APIError(422, "inference body must be an object")
        unknown = set(payload) - {"model", "prompt", "input_tokens", "max_output_tokens", "idempotency_key"}
        if unknown:
            raise APIError(422, "unsupported inference fields: " + ", ".join(sorted(unknown)))
        result = {"model": payload.get("model", "base"), "prompt": payload.get("prompt", ""),
                  "input_tokens": payload.get("input_tokens", 1024),
                  "max_output_tokens": payload.get("max_output_tokens", 500)}
        if not isinstance(result["model"], str) or not isinstance(result["prompt"], str):
            raise APIError(422, "model and prompt must be strings")
        for field, minimum in (("input_tokens", 0), ("max_output_tokens", 1)):
            if type(result[field]) is not int or result[field] < minimum:
                raise APIError(422, f"{field} must be an integer >= {minimum}")
        if result["input_tokens"] + result["max_output_tokens"] > 32768:
            raise APIError(422, "input_tokens plus max_output_tokens exceeds 32768")
        # Prompt contents are opaque; declared counts stand in for a tokenizer.
        return result

    @staticmethod
    def _scheduler_request(record):
        from scheduler import Request
        return Request(id=record["id"], customer=record["customer"], lora=record["lora"],
                       path=record["path"], prompt=json.loads(record["payload"])["prompt"],
                       ingress=record["ingress"], deadline=record["deadline"], attempt=record["attempt"])

    def _public(self, record):
        completed_at = record.get("completed_at")
        if completed_at is not None:
            completed_at = max(completed_at, self._publication_times.get(record["id"], completed_at))
        terminal = record["status"] in ("completed", "failed")
        return {"id": record["id"], "status": record["status"],
                "resolved_model": record["model"], "ingress": record["ingress"],
                "deadline": record["deadline"], "attempt": record["attempt"],
                "started_at": record.get("started_at"), "completed_at": completed_at,
                "result": json.loads(record["result"]) if record.get("result") else None,
                "error": record.get("error"),
                "slo_violation": terminal and (bool(record.get("deadline_violation")) or record["status"] != "completed" or completed_at > record["deadline"])}

    def infer(self, key, payload, idempotency_key=None, *, ingress=None):
        # Capture API ingress before waiting for the actor or doing auth/resolve.
        ingress = self.clock() if ingress is None else ingress
        with self.lock:
            customer = self.store.authenticate(key)
            normalized = self._payload(payload)
            body_key = payload.get("idempotency_key")
            if idempotency_key is not None and body_key is not None and body_key != idempotency_key:
                raise APIError(422, "header and body idempotency keys differ")
            idempotency_key = idempotency_key if idempotency_key is not None else body_key
            if idempotency_key is not None and (not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 256):
                raise APIError(422, "idempotency_key must be a string of 1-256 characters")
            fingerprint = canonical(normalized)
            # Continuous HTTP admissions can repeatedly acquire this lock ahead
            # of the worker. Advance previously durable work within the same
            # bounded controller budget before reserving the current request.
            if not self.worker_error and self.active_attempts and self.clock()-self._last_control >= self._control_budget:
                try:
                    self.tick()
                except Exception as error:
                    self._fail_actor(error)
                    raise APIError(503, "scheduler worker is unavailable") from error
            previous = self.store.idempotent_request(customer, idempotency_key)
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise APIError(409, "idempotency key was used with another inference payload")
                return 202, self._public(previous)
            if self.worker_error:
                raise APIError(503, "scheduler worker is unavailable")
            resolved = self.store.resolve(customer, normalized["model"])
            now = ingress
            record = {"id": uuid.uuid4().hex, "customer": customer,
                      "idempotency_key": idempotency_key, "fingerprint": fingerprint,
                      "payload": fingerprint, "model": resolved["model"],
                      "lora": resolved["lora"], "path": resolved["path"],
                      "ingress": now, "deadline": now+self.slo, "status": "queued", "attempt": 1}
            accepted, reason = self.scheduler.submit(self._scheduler_request(record))
            if not accepted:
                status = 503 if reason in ("no_healthy_replica", "no_eligible_replica", "unavailable") else 429
                raise APIError(status, reason)
            try:
                self.store.insert_request(record)
            except sqlite3.Error as error:
                # submit never dispatches: cancellation under this same actor lock
                # removes the reservation before the next tick can launch it.
                self.scheduler.cancel(record["id"])
                raise APIError(503, "durable acceptance unavailable") from error
            self.active_attempts[record["id"]] = record["attempt"]
            return 202, self._public(record)

    def get_request(self, key, request_id):
        with self.lock:
            customer = self.store.authenticate(key)
            response = self._public(self.store.request(request_id, customer))
            if self.worker_error and response["status"] in ("queued", "running"):
                response["worker_error"] = "worker unavailable; restart required for durable recovery"
            return response

    def recover(self):
        """Cold restart: claim generations and re-admit within original deadlines."""
        with self.lock:
            for original in self.store.pending_requests():
                record = self.store.claim_attempt(original["id"])
                if record["status"] not in ("queued", "running"):
                    continue
                self.active_attempts[record["id"]] = record["attempt"]
                now = self.clock()
                if now >= record["deadline"]:
                    accepted, reason = False, "deadline_expired"
                else:
                    accepted, reason = self.scheduler.submit(self._scheduler_request(record))
                if not accepted:
                    self.store.finish(record["id"], record["attempt"], "failed", None,
                                      "recovery_"+reason, None, now, clock=self.clock)
                    self.active_attempts.pop(record["id"], None)

    def tick(self):
        with self.lock:
            self._last_control = self.clock()
            self.scheduler.tick()
            published = {}
            for request_id, generation in list(self.active_attempts.items()):
                outcome = self.scheduler.completed.get(request_id)
                if outcome:
                    # Engine retries may increase the scheduler attempt number.
                    # The DB generation stays the actor's claim: an old actor
                    # cannot overwrite a replacement even if its retry completes.
                    if outcome.get("attempt", generation) < generation:
                        continue
                    changed = self.store.finish(request_id, generation, outcome["status"],
                                      outcome.get("output", outcome.get("result")),
                                      outcome.get("error"), outcome.get("start"),
                                      max(self.clock(), outcome.get("end", outcome.get("completed_at", self.clock()))),
                                      clock=self.clock)
                    if changed:
                        published[request_id] = self.store.request(request_id)
                    self.active_attempts.pop(request_id, None)
                else:
                    status = self.scheduler.request_status(request_id)
                    if status and status.get("status") in ("running", "loading"):
                        self.store.running(request_id, generation, status.get("start"))

            self._observe_publications(published)

    def _observe_publications(self, records):
        # The actor holds its lock across all result/metadata commits. Observe
        # after the final write, then persist an irreversible late flag. Those
        # flag writes can delay other results too, so recheck until no newly
        # late result remains. The final timestamp is in-memory; physical commit
        # time cannot atomically be backwritten into its own transaction.
        unchecked = dict(records)
        while records:
            observed = self.clock()
            late = [row for row in unchecked.values() if observed > row["deadline"]]
            if not late:
                for request_id in records:
                    self._publication_times[request_id] = observed
                break
            for row in late:
                self.store.mark_deadline_violation(row["id"], row["attempt"])
                unchecked.pop(row["id"])

    def start_worker(self):
        with self.lock:
            if self._worker is not None:
                return
            self._worker = threading.Thread(target=self._work, name="product-actor", daemon=True)
            self._worker.start()

    def _work(self):
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception as error:
                self._fail_actor(error)
                return
            self._stop.wait(self.poll_interval)

    def _fail_actor(self, error):
        with self.lock:
            self.worker_error = error
            # A controller fault must not leave accepted work appearing
            # healthy forever. If storage itself failed, retain the row
            # for restart and expose worker_error when it is polled.
            for request_id, generation in list(self.active_attempts.items()):
                try:
                    self.store.finish(request_id, generation, "failed", None,
                                      "actor_failure", None, self.clock(), clock=self.clock)
                    self.active_attempts.pop(request_id, None)
                except sqlite3.Error:
                    break

    def close(self):
        self._stop.set()
        if self._worker is not None:
            self._worker.join(timeout=2)
        close = getattr(self.scheduler.backend, "close", None)
        if close:
            close()
