"""Dropping a held reply after source death must still join the measuring proxy."""
import json
import base64
from contextlib import contextmanager
import http.server
from pathlib import Path
import socket
import struct
import tempfile
import threading
import unittest

from faults import Ledger, cleanup_all, require
from fleet_faults import PeerProxy, message


class PeerProxyTests(unittest.TestCase):
    def test_source_reset_while_proven_reply_is_held(self):
        with tempfile.TemporaryDirectory() as directory, socket.socket() as backend:
            backend.bind(("127.0.0.1", 0)); backend.listen(1); backend.settimeout(5)
            failures=[]
            def serve():
                try:
                    connection,_=backend.accept()
                    with connection:
                        connection.settimeout(5)
                        with connection.makefile("rb") as incoming:
                            self.assertIsNotNone(message(incoming))
                        connection.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nreply")
                except Exception as error:failures.append(error)
            thread=threading.Thread(target=serve);thread.start()
            ledger=Ledger(Path(directory),commands=(("cross",725,1),))
            proxy=PeerProxy(backend.getsockname(),ledger)
            try:
                proxy.arm()
                source=socket.create_connection(proxy.address,timeout=5)
                try:
                    source.sendall(b"POST / HTTP/1.1\r\nContent-Length: 7\r\nx-cells-rpc-method: __streamAppend\r\nx-cells-scope: __EventStream:"+b"d"*64+b"\r\n\r\nrequest")
                    self.assertTrue(proxy.entered.wait(5))
                    source.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack("ii",1,0))
                finally:source.close()
                proxy.release.set()
                self.assertTrue(proxy.lost.wait(5))
            finally:
                try:proxy.close()
                finally:thread.join(timeout=5);ledger.close()
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures,[])
            events=[json.loads(line)["event"] for line in ledger.path.read_text().splitlines()]
            self.assertEqual(events,["plan","append_reply_held","append_reply_lost"])


class PeerUpgradeTests(unittest.TestCase):
    @contextmanager
    def connection(self,payload=b"",rpc=False):
        with tempfile.TemporaryDirectory() as directory:
            class Handler(http.server.BaseHTTPRequestHandler):
                protocol_version="HTTP/1.1"
                def do_POST(self):
                    self.connection.settimeout(3)
                    self.rfile.read(int(self.headers["Content-Length"]))
                    self.send_response(101);self.send_header("Connection","upgrade");self.send_header("Upgrade","cellar-log-stream");self.end_headers()
                    try:
                        self.wfile.write(b"\x00\xffserver");self.wfile.flush()
                        received=self.rfile.read(14)
                        self.wfile.write(received);self.wfile.flush()
                    except OSError:pass
                    self.close_connection=True
                def do_GET(self):
                    self.connection.settimeout(3)
                    self.send_response(101);self.send_header("Connection","upgrade");self.send_header("Upgrade","cellar-tunnel");self.end_headers()
                    try:
                        request=message(self.rfile)
                        require(request["headers"][b"x-cells-rpc-method"]==b"__streamAck","missing inner RPC")
                        self.wfile.write(b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nreply");self.wfile.flush()
                    except OSError:pass
                    self.close_connection=True
                def log_message(self,*args):pass
            backend=http.server.ThreadingHTTPServer(("127.0.0.1",0),Handler)
            backend.daemon_threads=False
            thread=threading.Thread(target=backend.serve_forever);thread.start()
            ledger=Ledger(Path(directory),commands=(("cross",725,1),))
            proxy=PeerProxy(backend.server_address,ledger,"__streamAck")
            proxy.armed=True;proxy.paused=not rpc
            source=None;reader=None
            def close_source():
                if source is not None:
                    try:source.shutdown(socket.SHUT_RDWR)
                    except OSError:pass
                if reader is not None:reader.close()
                if source is not None:source.close()
            def join_backend():
                thread.join(timeout=5)
                require(not thread.is_alive(),"upgrade backend did not join")
            try:
                source=socket.create_connection(proxy.address,timeout=2)
                head=b"GET /peer/tunnel HTTP/1.1\r\nHost: fixture\r\nContent-Length: 0\r\nConnection: upgrade\r\nUpgrade: cellar-tunnel\r\n\r\n" if rpc else b"POST /peer/log/tail HTTP/1.1\r\nHost: fixture\r\nContent-Length: 0\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\n\r\n"
                source.sendall(head+payload)
                reader=source.makefile("rb")
                reply=message(reader,response=True)
                self.assertEqual(reply["first"].split()[1],b"101")
                yield proxy,source,reader,ledger
            finally:
                cleanup_all([("source",close_source),("proxy",proxy.close),("backend",backend.shutdown),("backend handlers",backend.server_close),("backend thread",join_backend),("ledger",ledger.close)])

    def test_tail_upgrade_relays_buffered_binary_without_opening_rpc_gate(self):
        payload=b"\x00\x01binary\x00tail\xff"
        self.assertEqual(len(payload),14)
        with self.connection(payload) as (proxy,source,reader,ledger):
            expected=b"\x00\xffserver"+payload
            self.assertEqual(reader.read(len(expected)),expected)
            self.assertEqual(reader.read(1),b"")
            self.assertTrue(proxy.armed);self.assertTrue(proxy.paused)
            self.assertEqual([json.loads(line)["event"] for line in ledger.path.read_text().splitlines()],["plan"])

    def test_close_joins_idle_upgrade_with_client_still_connected(self):
        with self.connection() as (proxy,source,reader,ledger):
            done=threading.Event();errors=[]
            def close():
                try:proxy.close()
                except Exception as error:errors.append(error)
                finally:done.set()
            worker=threading.Thread(target=close);worker.start()
            finished=done.wait(2)
            if not finished:
                try:source.shutdown(socket.SHUT_RDWR)
                except OSError:pass
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive());self.assertTrue(finished,"close waited for the upgrade client to disconnect")
            self.assertEqual(errors,[])

    def test_outer_rpc_upgrade_preserves_inner_ack_reply_gate(self):
        request=b"POST / HTTP/1.1\r\nContent-Length: 7\r\nx-cells-rpc-method: __streamAck\r\nx-cells-scope: __EventStream:"+b"d"*64+b"\r\n\r\nrequest"
        with self.connection(request,rpc=True) as (proxy,source,reader,ledger):
            self.assertTrue(proxy.entered.wait(2),"opaque relay hid the inner proven ACK from its gate")
            self.assertEqual(base64.b64decode(proxy.capture["request"]),b"request")
            self.assertEqual(base64.b64decode(proxy.capture["response"]),b"reply")
            source.settimeout(.1)
            with self.assertRaises(TimeoutError):source.recv(1)
            source.settimeout(2);proxy.release.set()
            self.assertTrue(proxy.lost.wait(2));self.assertEqual(source.recv(1),b"")
            self.assertTrue(proxy.paused)
            self.assertEqual([json.loads(line)["event"] for line in ledger.path.read_text().splitlines()],["plan","ack_reply_held","ack_reply_lost"])
