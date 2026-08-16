#!/usr/bin/env python3
"""Small deterministic HTTP proxy used only by the real MinIO fault E2E."""

import argparse
import http.client
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


MODES = {
    "NONE",
    "PUT_503_AFTER_COMMIT",
    "PUT_503_BEFORE_COMMIT",
    "PUT_TIMEOUT_AFTER_COMMIT",
}


class FaultState:
    def __init__(self):
        self._lock = threading.Lock()
        self.mode = "NONE"
        self.triggered = False

    def set_mode(self, mode):
        if mode not in MODES:
            raise ValueError(f"unsupported fault mode: {mode}")
        with self._lock:
            self.mode = mode
            self.triggered = False

    def consume_for(self, method, request_path):
        path = request_path.split("?", 1)[0]
        if method != "PUT" or not path.endswith("/manifest.json"):
            return None
        with self._lock:
            if self.mode == "NONE" or self.triggered:
                return None
            self.triggered = True
            return self.mode


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        if self.path == "/__health":
            self._send_bytes(200, b"ok\n", "text/plain")
            return
        self._forward()

    def do_PUT(self):
        self._forward()

    def do_DELETE(self):
        self._forward()

    def do_POST(self):
        if self.path != "/__fault":
            self._send_bytes(404, b"not found\n", "text/plain")
            return
        length = int(self.headers.get("Content-Length", "0"))
        mode = self.rfile.read(length).decode("ascii").strip()
        try:
            self.server.fault_state.set_mode(mode)
        except ValueError as failure:
            self._send_bytes(400, f"{failure}\n".encode("utf-8"), "text/plain")
            return
        self._send_bytes(200, f"mode={mode}\n".encode("ascii"), "text/plain")

    def _forward(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        fault = self.server.fault_state.consume_for(self.command, self.path)
        if fault == "PUT_503_BEFORE_COMMIT":
            self._send_bytes(503, b"injected before commit\n", "text/plain")
            return

        headers = {}
        for name, value in self.headers.items():
            if name.lower() in {"connection", "proxy-connection", "keep-alive"}:
                continue
            headers[name] = value
        # SigV4 signs the proxy Host; preserve it while forwarding to MinIO.
        headers["Host"] = self.headers.get("Host", self.server.backend_host)
        headers["Connection"] = "close"

        connection = http.client.HTTPConnection(
            self.server.backend_host,
            self.server.backend_port,
            timeout=30,
        )
        try:
            connection.request(self.command, self.path, body=body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            response_headers = response.getheaders()
            status = response.status
        except OSError as failure:
            self._send_bytes(502, f"backend failure: {failure}\n".encode("utf-8"), "text/plain")
            return
        finally:
            connection.close()

        if fault == "PUT_503_AFTER_COMMIT":
            self._send_bytes(503, b"injected after commit\n", "text/plain")
            return
        if fault == "PUT_TIMEOUT_AFTER_COMMIT":
            # The backend has already committed the object. Hold this client
            # connection beyond the adapter request timeout; the next exact
            # GET is handled by another ThreadingHTTPServer worker.
            time.sleep(3)
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            return

        self.send_response(status)
        for name, value in response_headers:
            if name.lower() in {"connection", "content-length", "transfer-encoding"}:
                continue
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(response_body)

    def _send_bytes(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format_string, *args):
        # Keep the runner log useful without duplicating every signed request.
        if self.path.startswith("/__"):
            super().log_message(format_string, *args)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--backend-host", default="127.0.0.1")
    parser.add_argument("--backend-port", type=int, required=True)
    args = parser.parse_args()

    server = ThreadingHTTPServer(("127.0.0.1", args.listen_port), ProxyHandler)
    server.daemon_threads = True
    server.backend_host = args.backend_host
    server.backend_port = args.backend_port
    server.fault_state = FaultState()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

