#!/usr/bin/env python3
"""Private technician API, WebSocket echo and deliberately untrusted TLS peer."""
import base64
import hashlib
import http.server
import json
import socket
import ssl
import struct
import subprocess
import threading

LOCK = threading.Lock()
EFFECTS = {}
ATTEMPTS = {}


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_):
        pass

    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path == "/reset":
            with LOCK:
                EFFECTS.clear()
                ATTEMPTS.clear()
            return self.reply(200, {"reset": True})
        size = int(self.headers.get("content-length", "0"))
        if self.path != "/notify" or size > 65536:
            return self.reply(400, {"error": "invalid notification"})
        value = json.loads(self.rfile.read(size))
        identity = value["effect_id"]
        with LOCK:
            attempt = ATTEMPTS.get(identity, 0) + 1
            ATTEMPTS[identity] = attempt
            if value["order_id"].startswith("probe-reject"):
                return self.reply(503, {"error": "controlled technician rejection"})
            prior = EFFECTS.setdefault(identity, value)
            if prior != value:
                return self.reply(409, {"error": "changed idempotency payload"})
        if value["order_id"].startswith("probe-ambiguous") and attempt == 1:
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        self.reply(200, {"effect_id": identity, "deliveries": 1})

    def do_GET(self):
        if self.path == "/health":
            return self.reply(200, {"status":"ok"})
        if self.path == "/ws":
            key = self.headers["Sec-WebSocket-Key"]
            accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            while True:
                head = self.rfile.read(2)
                if len(head) != 2:
                    break
                opcode = head[0] & 15
                size = head[1] & 127
                if size == 126:
                    size = struct.unpack("!H", self.rfile.read(2))[0]
                if size == 127 or size > 65536:
                    break
                mask = self.rfile.read(4) if head[1] & 128 else b""
                payload = self.rfile.read(size)
                if mask:
                    payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
                out_opcode = 10 if opcode == 9 else opcode
                prefix = bytes([128 | out_opcode, size]) if size < 126 else bytes([128 | out_opcode, 126]) + struct.pack("!H", size)
                self.wfile.write(prefix + payload)
                self.wfile.flush()
                if opcode == 8:
                    break
            self.close_connection = True
            return
        with LOCK:
            self.reply(200, {"effects": dict(EFFECTS), "attempts": dict(ATTEMPTS)})


def main():
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-keyout", "/tmp/peer.key", "-out", "/tmp/peer.crt", "-subj", "/CN=technician",
                    "-addext", "subjectAltName=DNS:technician"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain("/tmp/peer.crt", "/tmp/peer.key")
    secure = http.server.ThreadingHTTPServer(("0.0.0.0", 8443), Handler)
    secure.socket = tls.wrap_socket(secure.socket, server_side=True)
    threading.Thread(target=secure.serve_forever, daemon=True).start()
    http.server.ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()


if __name__ == "__main__":
    main()
