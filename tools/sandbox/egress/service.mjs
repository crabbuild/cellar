// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import http from "node:http";

const server = http.createServer((request, response) => {
  if (request.method !== "GET" || request.url !== "/ready") {
    response.writeHead(404).end();
    return;
  }
  response.writeHead(200, {
    "content-type": "text/plain",
    "x-cellar-network-fixture": "1",
  });
  response.end("owned-network");
});

server.listen(8080, "0.0.0.0");
