"""Private loopback fault: refuse seal of one real retained consumer session."""
import hashlib
import json
import socket
import socketserver
import threading

from faults import require
from fleet_faults import message


class WitnessGate:
    def __init__(self,target,leader,epoch):
        self.target,self.leader,self.epoch=target,leader,epoch
        self.lock=threading.Lock();self.blocked=True;self.observed=[];self.errors=[]
        gate=self
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                upstream=None
                try:
                    stream=self.request.makefile("rb")
                    while (request:=message(stream)) is not None:
                        path=request["first"].split()[1]
                        matches=False
                        if path==b"/peer/log/seal":
                            value=json.loads(request["body"])
                            matches=value.get("leader")==gate.leader and value.get("epoch")==gate.epoch
                        with gate.lock:
                            blocked=gate.blocked and matches
                            if blocked:
                                require(len(gate.observed)<1000,"private witness observation bound exceeded")
                                gate.observed.append(dict(leader=value["leader"],epoch=value["epoch"],request_sha256=hashlib.sha256(request["body"]).hexdigest()))
                        if blocked:
                            body=b'{"error":"private consumer witness unavailable"}'
                            self.request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: "+str(len(body)).encode()+b"\r\n\r\n"+body)
                            return
                        if upstream is None:
                            upstream=socket.create_connection(gate.target,timeout=10);upstream.settimeout(None);owner=upstream.makefile("rb")
                        upstream.sendall(request["wire"])
                        response=message(owner,response=True)
                        if response is None:return
                        self.request.sendall(response["wire"])
                except (ConnectionResetError,BrokenPipeError):pass
                except Exception as error:
                    with gate.lock:gate.errors.append(repr(error))
                finally:
                    if upstream is not None:upstream.close()
        class Server(socketserver.ThreadingTCPServer):
            allow_reuse_address=True
        self.server=Server(("127.0.0.1",0),Handler);self.address=self.server.server_address
        self.thread=threading.Thread(target=self.server.serve_forever);self.thread.start()

    def observations(self):
        with self.lock:return list(self.observed)

    def release(self):
        with self.lock:self.blocked=False

    def close(self):
        self.release();self.server.shutdown();self.server.server_close();self.thread.join(timeout=5)
        require(not self.thread.is_alive() and not self.errors,f"consumer witness gate did not join cleanly: {self.errors}")
