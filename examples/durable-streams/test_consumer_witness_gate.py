"""The private fault refuses only the selected seal; it never supplies proof."""
import http.server
import http.client
import json
import socket
import threading
import unittest
import urllib.error
import urllib.request

from consumer_witness_gate import WitnessGate
from fleet_faults import message


class WitnessGateTests(unittest.TestCase):
    def setUp(self):
        self.requests=[];requests=self.requests
        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version="HTTP/1.1"
            def log_message(self,*args):pass
            def do_POST(self):
                body=self.rfile.read(int(self.headers["Content-Length"]))
                requests.append((self.path,body,self.headers.get("x-private-fixture")))
                if self.headers.get("Upgrade")=="cellar-log-stream":
                    self.send_response(101);self.send_header("Connection","upgrade");self.send_header("Upgrade","cellar-log-stream");self.end_headers()
                    self.wfile.write(b"\x00\xffserver");self.wfile.flush()
                    try:
                        data=self.rfile.read(14)
                        self.wfile.write(data);self.wfile.flush()
                    except (BrokenPipeError,ConnectionResetError):pass
                    self.close_connection=True
                    return
                if self.server.reject:
                    body=b"invalid follower state: checksum mismatch"
                    self.send_response(400);self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body);return
                self.send_response(200);self.send_header("Content-Length",str(len(body)));self.end_headers();self.wfile.write(body)
        self.server=http.server.ThreadingHTTPServer(("127.0.0.1",0),Handler)
        self.server.reject=False
        self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()
        self.gate=WitnessGate(self.server.server_address,"consumer/"+"a"*32+"/"+"b"*64,7)

    def tearDown(self):
        self.gate.close();self.server.shutdown();self.server.server_close();self.thread.join(timeout=5)
        self.assertFalse(self.thread.is_alive())

    def request(self,path,leader=None,epoch=7):
        body=json.dumps(dict(leader=leader or self.gate.leader,epoch=epoch)).encode()
        request=urllib.request.Request("http://%s:%s%s"%(*self.gate.address,path),data=body,headers={"x-private-fixture":"unchanged"})
        try:
            with urllib.request.urlopen(request,timeout=5) as response:return response.status,response.read(),body
        except urllib.error.HTTPError as error:
            with error:return error.code,error.read(),body

    def test_selected_unavailability_then_actual_response_after_release(self):
        status,_,body=self.request("/peer/log/seal")
        self.assertEqual(status,503);self.assertEqual(self.requests,[])
        self.assertEqual(len(self.gate.observations()),1)
        self.gate.release();status,response,body=self.request("/peer/log/seal")
        self.assertEqual((status,response),(200,body));self.assertEqual(self.requests,[("/peer/log/seal",body,"unchanged")])

    def test_other_sessions_epochs_and_methods_keep_exact_bytes(self):
        for path,leader,epoch in (("/peer/log/tail",None,7),("/peer/log/seal","other",7),("/peer/log/seal",None,8)):
            status,response,body=self.request(path,leader,epoch)
            self.assertEqual((status,response),(200,body));self.assertEqual(self.requests[-1],(path,body,"unchanged"))
        self.assertEqual(self.gate.observations(),[])

    def test_joined_upstream_restart_propagates_frontend_eof(self):
        self.gate.release()
        connection=http.client.HTTPConnection(*self.gate.address,timeout=5)
        body=json.dumps(dict(leader=self.gate.leader,epoch=7)).encode()
        try:
            connection.request("POST","/peer/log/seal",body)
            response=connection.getresponse();self.assertEqual(response.read(),body)
            self.assertGreaterEqual(self.gate.disconnect(),1)
            self.assertEqual(connection.sock.recv(1),b"")
            status,reply,sent=self.request("/peer/log/seal")
            self.assertEqual((status,reply),(200,sent))
        finally:connection.close()

    def test_actual_non_json_peer_error_is_preserved_and_observed(self):
        self.gate.release();self.server.reject=True
        status,reply,_=self.request("/peer/log/seal")
        self.assertEqual((status,reply),(400,b"invalid follower state: checksum mismatch"))
        observed=self.gate.observations()[0]
        self.assertIs(observed["blocked"],False);self.assertIsNone(observed["response"])
        self.assertEqual(observed["status"],400);self.assertEqual(observed["response_body"],reply.decode())
        self.assertEqual(len(self.requests),1)

    def test_close_disconnects_idle_keepalive_handlers_before_joining(self):
        self.gate.release()
        connection=http.client.HTTPConnection(*self.gate.address,timeout=5)
        body=json.dumps(dict(leader=self.gate.leader,epoch=7)).encode()
        connection.request("POST","/peer/log/seal",body)
        self.assertEqual(connection.getresponse().read(),body)
        done=threading.Event();errors=[]
        def close():
            try:self.gate.close()
            except Exception as error:errors.append(error)
            finally:done.set()
        thread=threading.Thread(target=close);thread.start()
        finished=done.wait(2)
        # Failure cleanup must itself release the blocked handler so this
        # regression reports an assertion rather than hanging the test runner.
        if not finished:self.gate.disconnect()
        connection.close();thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(finished,"close waited for a persistent client to disconnect")
        self.assertEqual(errors,[])

    def upgrade(self,payload=b""):
        connection=socket.create_connection(self.gate.address,timeout=2)
        connection.sendall(b"POST /peer/log/tail HTTP/1.1\r\nHost: fixture\r\nContent-Length: 0\r\nConnection: upgrade\r\nUpgrade: cellar-log-stream\r\n\r\n"+payload)
        stream=connection.makefile("rb")
        response=message(stream,response=True)
        self.assertEqual(response["first"].split()[1],b"101")
        return connection,stream

    def test_upgraded_tail_forwards_buffered_binary_both_ways_and_eof(self):
        payload=b"\x00\x01binary\x00tail\xff"
        self.assertEqual(len(payload),14)
        connection,stream=self.upgrade(payload)
        try:
            expected=b"\x00\xffserver"+payload
            self.assertEqual(stream.read(len(expected)),expected)
            self.assertEqual(stream.read(1),b"")
        finally:stream.close();connection.close()

    def test_close_disconnects_idle_upgraded_tail_before_joining(self):
        connection,stream=self.upgrade()
        try:
            self.assertEqual(stream.read(8),b"\x00\xffserver")
            done=threading.Event();errors=[]
            def close():
                try:self.gate.close()
                except Exception as error:errors.append(error)
                finally:done.set()
            worker=threading.Thread(target=close);worker.start()
            finished=done.wait(2)
            if not finished:self.gate.disconnect()
            worker.join(timeout=5)
            self.assertTrue(finished,"close abandoned an upgraded tail relay")
            self.assertFalse(worker.is_alive());self.assertEqual(errors,[])
        finally:stream.close();connection.close()


if __name__=="__main__":unittest.main()
