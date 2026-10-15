"""Dropping a held reply after source death must still join the measuring proxy."""
import json
from pathlib import Path
import socket
import struct
import tempfile
import threading
import unittest

from faults import Ledger
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
