// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import net from "node:net";

// An owned peer outside the qualifier's namespace. A callback verifies that
// offline policy permits replies to an inbound connection without opening
// arbitrary outbound connections.
const server = net.createServer((socket) => {
  socket.setTimeout(5000, () => socket.destroy());
  socket.on("error", () => socket.destroy());
  let line = "";
  socket.on("data", (bytes) => {
    line += bytes.toString("utf8");
    if (line.length > 1024) return socket.destroy();
    if (!line.endsWith("\n")) return;
    socket.removeAllListeners("data");
    let request;
    try {
      request = JSON.parse(line);
    } catch {
      return socket.destroy();
    }
    if (request.callbackPort === undefined) return socket.end("online");
    if (request.callbackPort !== 9091) return socket.destroy();
    const callback = net.connect(9091, socket.remoteAddress);
    callback.setTimeout(5000, () => callback.destroy());
    callback.on("error", () => socket.destroy());
    callback.once("connect", () => callback.write("ingress"));
    let reply = "";
    callback.on("data", (bytes) => {
      reply += bytes.toString("utf8");
      if (reply.length > 5) callback.destroy();
    });
    callback.once("end", () => {
      if (reply === "reply") socket.end("callback-ok");
      else socket.destroy();
    });
    socket.once("close", () => callback.destroy());
  });
});

server.listen(9080, "0.0.0.0");
