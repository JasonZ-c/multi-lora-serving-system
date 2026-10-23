"""SQLite is the authority for credentials, immutable artifacts and request outcomes."""
from __future__ import annotations

import hashlib
import json
import math
import re
import secrets
import sqlite3
import threading
import time
from contextlib import contextmanager


class APIError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def identifier(value, field="name") -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise APIError(422, f"{field} must be 1-128 letters, digits, dots, underscores or hyphens")
    return value


class Store:
    def __init__(self, path: str):
        self.connection = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.connection.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS customers (id TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS credentials (
                key_hash TEXT PRIMARY KEY, customer TEXT NOT NULL REFERENCES customers(id));
            CREATE TABLE IF NOT EXISTS versions (
                customer TEXT NOT NULL REFERENCES customers(id), name TEXT NOT NULL,
                version TEXT NOT NULL, blob_path TEXT NOT NULL,
                PRIMARY KEY(customer,name,version));
            CREATE TABLE IF NOT EXISTS endpoints (
                customer TEXT NOT NULL, name TEXT NOT NULL, lora_name TEXT NOT NULL,
                version TEXT NOT NULL, revision INTEGER NOT NULL,
                PRIMARY KEY(customer,name),
                FOREIGN KEY(customer,lora_name,version) REFERENCES versions(customer,name,version));
            CREATE TABLE IF NOT EXISTS releases (
                customer TEXT NOT NULL, endpoint TEXT NOT NULL, release_id TEXT NOT NULL,
                fingerprint TEXT NOT NULL, response TEXT NOT NULL,
                PRIMARY KEY(customer,endpoint,release_id));
            CREATE TABLE IF NOT EXISTS requests (
                id TEXT PRIMARY KEY, customer TEXT NOT NULL REFERENCES customers(id),
                idempotency_key TEXT, fingerprint TEXT NOT NULL, payload TEXT NOT NULL,
                model TEXT NOT NULL, lora TEXT, path TEXT, ingress REAL NOT NULL,
                deadline REAL NOT NULL, status TEXT NOT NULL, attempt INTEGER NOT NULL,
                result TEXT, error TEXT, started_at REAL, completed_at REAL,
                deadline_violation INTEGER NOT NULL DEFAULT 0,
                UNIQUE(customer,idempotency_key));
        """)
        with self.transaction():
            columns = {row["name"] for row in self.connection.execute("PRAGMA table_info(requests)")}
            if "deadline_violation" not in columns:
                self.connection.execute("ALTER TABLE requests ADD COLUMN deadline_violation INTEGER NOT NULL DEFAULT 0")

    @contextmanager
    def transaction(self):
        with self.lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.connection.execute("COMMIT")
            except BaseException:
                self.connection.execute("ROLLBACK")
                raise

    def clock_origin(self, speed: float) -> float:
        """Continue logical time across restart, including accelerated downtime.

        The running backend uses monotonic elapsed time. Across process starts
        we assume a stable wall clock: downtime advances at the previous rate,
        and a new rate applies from this persisted anchor onward. Backward wall
        jumps are clamped; forward jumps conservatively consume deadline slack.
        Call once when constructing the process's single scheduler backend.
        """
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError("speed must be finite and positive")
        with self.transaction():
            wall = time.time()
            row = self.connection.execute("SELECT value FROM meta WHERE key='clock_anchor'").fetchone()
            if row:
                old = json.loads(row["value"])
                wall = max(wall, old["wall"])
                logical = old["logical"] + (wall-old["wall"])*old["speed"]
            else:
                logical = wall
            anchor = canonical({"wall": wall, "logical": logical, "speed": speed})
            self.connection.execute("INSERT INTO meta VALUES ('clock_anchor',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (anchor,))
        return logical

    def provision_customer(self, customer: str, api_key: str | None = None) -> str:
        identifier(customer, "customer")
        key = api_key or secrets.token_urlsafe(32)
        if not isinstance(key, str) or not key:
            raise ValueError("API key cannot be empty")
        digest = hashlib.sha256(key.encode()).hexdigest()
        with self.transaction():
            existing = self.connection.execute("SELECT customer FROM credentials WHERE key_hash=?", (digest,)).fetchone()
            if existing and existing["customer"] != customer:
                raise APIError(409, "API key is already provisioned")
            self.connection.execute("INSERT OR IGNORE INTO customers VALUES (?)", (customer,))
            self.connection.execute("INSERT OR IGNORE INTO credentials VALUES (?,?)", (digest, customer))
        return key

    def authenticate(self, key: str) -> str:
        if not isinstance(key, str) or not key:
            raise APIError(401, "invalid API key")
        digest = hashlib.sha256(key.encode()).hexdigest()
        with self.lock:
            row = self.connection.execute("SELECT customer FROM credentials WHERE key_hash=?", (digest,)).fetchone()
        if row is None:
            raise APIError(401, "invalid API key")
        return row["customer"]

    def register_version(self, customer: str, name: str, version: str, blob_path: str) -> dict:
        identifier(name)
        identifier(version, "version")
        if not isinstance(blob_path, str) or not blob_path.strip() or len(blob_path) > 4096:
            raise APIError(422, "blob_path must be a nonempty string of at most 4096 characters")
        with self.transaction():
            row = self.connection.execute("SELECT blob_path FROM versions WHERE customer=? AND name=? AND version=?", (customer, name, version)).fetchone()
            if row and row["blob_path"] != blob_path:
                raise APIError(409, "registered versions are immutable; register a new version")
            self.connection.execute("INSERT OR IGNORE INTO versions VALUES (?,?,?,?)", (customer, name, version, blob_path))
        return {"name": name, "version": version, "blob_path": blob_path}

    def release(self, customer: str, endpoint: str, lora_name: str, version: str,
                expected_revision: int, release_id: str) -> dict:
        for value, field in ((endpoint,"endpoint"), (lora_name,"lora_name"), (version,"version"), (release_id,"release_id")):
            identifier(value, field)
        if type(expected_revision) is not int or expected_revision < 0:
            raise APIError(422, "expected_revision must be a nonnegative integer")
        fingerprint = canonical([lora_name, version, expected_revision])
        with self.transaction():
            previous = self.connection.execute("SELECT fingerprint,response FROM releases WHERE customer=? AND endpoint=? AND release_id=?", (customer, endpoint, release_id)).fetchone()
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise APIError(409, "release_id was used with another payload")
                return json.loads(previous["response"])
            self._version(customer, lora_name, version)
            row = self.connection.execute("SELECT revision FROM endpoints WHERE customer=? AND name=?", (customer, endpoint)).fetchone()
            revision = row["revision"] if row else 0
            if revision != expected_revision:
                raise APIError(409, f"routing revision is {revision}, expected {expected_revision}")
            response = {"endpoint": endpoint, "lora_name": lora_name, "version": version,
                        "revision": revision + 1, "release_id": release_id}
            self.connection.execute("INSERT INTO endpoints VALUES (?,?,?,?,?) ON CONFLICT(customer,name) DO UPDATE SET lora_name=excluded.lora_name,version=excluded.version,revision=excluded.revision", (customer, endpoint, lora_name, version, revision+1))
            self.connection.execute("INSERT INTO releases VALUES (?,?,?,?,?)", (customer, endpoint, release_id, fingerprint, canonical(response)))
        return response

    def _version(self, customer, name, version):
        row = self.connection.execute("SELECT blob_path FROM versions WHERE customer=? AND name=? AND version=?", (customer, name, version)).fetchone()
        if not row:
            raise APIError(404, "LoRA version not found")
        return row["blob_path"]

    def resolve(self, customer: str, model: str) -> dict:
        if model == "base":
            return {"model": "base", "lora": None, "path": None}
        if not isinstance(model, str):
            raise APIError(422, "model must be base, lora:name@version or endpoint:name")
        with self.lock:
            if model.startswith("endpoint:"):
                endpoint = identifier(model[len("endpoint:"):], "endpoint")
                route = self.connection.execute("SELECT lora_name,version FROM endpoints WHERE customer=? AND name=?", (customer, endpoint)).fetchone()
                if not route:
                    raise APIError(404, "endpoint not found")
                name, version = route["lora_name"], route["version"]
            elif model.startswith("lora:") and model.count("@") == 1:
                name, version = model[len("lora:"):].split("@")
                identifier(name)
                identifier(version, "version")
            else:
                raise APIError(422, "model must be base, lora:name@version or endpoint:name")
            path = self._version(customer, name, version)
        # Encoding the tuple avoids collisions between customers, names or versions.
        return {"model": f"lora:{name}@{version}", "lora": canonical([customer, name, version]), "path": path}

    def close(self):
        with self.lock:
            self.connection.close()

    def request(self, request_id: str, customer: str | None = None):
        with self.lock:
            row = self.connection.execute("SELECT * FROM requests WHERE id=?", (request_id,)).fetchone()
        if row is None or (customer is not None and row["customer"] != customer):
            raise APIError(404, "request not found")
        return dict(row)

    def idempotent_request(self, customer, key):
        if key is None:
            return None
        with self.lock:
            row = self.connection.execute("SELECT * FROM requests WHERE customer=? AND idempotency_key=?", (customer, key)).fetchone()
        return dict(row) if row else None

    def insert_request(self, record):
        fields = ("id", "customer", "idempotency_key", "fingerprint", "payload", "model", "lora", "path", "ingress", "deadline", "status", "attempt")
        with self.transaction():
            self.connection.execute("INSERT INTO requests (" + ",".join(fields) + ") VALUES (" + ",".join("?" for _ in fields) + ")", tuple(record[field] for field in fields))

    def pending_requests(self):
        with self.lock:
            return [dict(row) for row in self.connection.execute("SELECT * FROM requests WHERE status IN ('queued','running') ORDER BY ingress,id")]

    def claim_attempt(self, request_id):
        with self.transaction():
            self.connection.execute("UPDATE requests SET attempt=attempt+1,status='queued',started_at=NULL WHERE id=? AND status IN ('queued','running')", (request_id,))
            return self.request(request_id)

    def running(self, request_id, attempt, started_at=None):
        with self.transaction():
            self.connection.execute("UPDATE requests SET status='running',started_at=COALESCE(started_at,?) WHERE id=? AND attempt=? AND status='queued'", (started_at, request_id, attempt))

    def finish(self, request_id, attempt, status, result, error, started_at, completed_at, clock=None):
        if status not in ("completed", "failed"):
            raise ValueError("terminal status required")
        with self.transaction():
            cursor = self.connection.execute("UPDATE requests SET status=?,result=?,error=?,started_at=COALESCE(started_at,?),completed_at=? WHERE id=? AND attempt=? AND status IN ('queued','running')", (status, canonical(result) if result is not None else None, error, started_at, completed_at, request_id, attempt))
            changed = cursor.rowcount == 1
        if changed and clock is not None:
            # Observe the first durable result commit. The actor separately
            # observes the final metadata commit and persists any late flag.
            # An abrupt crash between these commits remains a fault window;
            # bounded storage/response margins are part of the healthy SLO.
            published_at = max(completed_at, clock())
            if published_at > completed_at:
                with self.transaction():
                    self.connection.execute("UPDATE requests SET completed_at=? WHERE id=? AND attempt=? AND status IN ('completed','failed')", (published_at, request_id, attempt))
        return changed


    def mark_deadline_violation(self, request_id, attempt):
        """Monotonic audit bit: later metadata cannot erase an observed miss."""
        with self.transaction():
            self.connection.execute("UPDATE requests SET deadline_violation=1 WHERE id=? AND attempt=? AND status IN ('completed','failed')", (request_id, attempt))
