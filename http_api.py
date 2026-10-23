"""Small authenticated JSON HTTP surface; all mutations go through one actor."""
from __future__ import annotations

import json
import sqlite3
from socketserver import TCPServer
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

from store import APIError

MAX_BODY_BYTES = 1024 * 1024


class ProductHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, service):
        self.product_service = service
        super().__init__(address, ProductHandler)

    def server_bind(self):
        # The API needs a bound address, never reverse DNS during local startup.
        TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]


class ProductHandler(BaseHTTPRequestHandler):
    server_version = "MultiLoRA/1"

    def log_message(self, format, *args):
        pass

    def _key(self):
        authorization = self.headers.get("Authorization", "")
        scheme, _, key = authorization.partition(" ")
        if scheme.lower() != "bearer" or not key or " " in key:
            raise APIError(401, "Bearer API key required")
        # Authenticate before handling the body or answering any route.
        self.server.product_service.store.authenticate(key)
        return key

    def _body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise APIError(400, "invalid Content-Length") from error
        if not 0 < length <= MAX_BODY_BYTES:
            raise APIError(413 if length > MAX_BODY_BYTES else 400, "JSON body must contain 1-1048576 bytes")
        try:
            body = json.loads(self.rfile.read(length))
        except (ValueError, UnicodeDecodeError) as error:
            raise APIError(400, "invalid JSON body") from error
        if not isinstance(body, dict):
            raise APIError(422, "JSON body must be an object")
        return body

    def _send(self, status, response):
        encoded = json.dumps(response, allow_nan=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        if status == 401:
            self.send_header("WWW-Authenticate", "Bearer")
        if status == 202 and "id" in response:
            self.send_header("Location", "/v1/requests/"+response["id"])
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def _handle(self, method):
        ingress = self.server.product_service.clock()
        try:
            key = self._key()
            service = self.server.product_service
            parts = [unquote(part) for part in urlsplit(self.path).path.strip("/").split("/")]
            if method == "GET" and len(parts) == 3 and parts[:2] == ["v1", "requests"]:
                status, response = 200, service.get_request(key, parts[2])
            elif method == "POST":
                body = self._body()
                if parts == ["v1", "inferences"]:
                    status, response = service.infer(key, body, self.headers.get("Idempotency-Key"), ingress=ingress)
                elif len(parts) == 4 and parts[:2] == ["v1", "loras"] and parts[3] == "versions":
                    response = service.register_version(key, parts[2], body.get("version"), body.get("blob_path"))
                    status = 201
                elif len(parts) == 4 and parts[:2] == ["v1", "endpoints"] and parts[3] == "releases":
                    response = service.release(key, parts[2], body.get("lora_name", parts[2]), body.get("version"), body.get("expected_revision"), body.get("release_id"))
                    status = 200
                else:
                    raise APIError(404, "route not found")
            else:
                raise APIError(404, "route not found")
            self._send(status, response)
        except APIError as error:
            self._send(error.status, {"error": error.message})
        except sqlite3.Error:
            self._send(503, {"error": "durable storage unavailable"})


def make_server(service, host="127.0.0.1", port=0):
    """Return a bound server. Start service.start_worker() before serve_forever()."""
    return ProductHTTPServer((host, port), service)
