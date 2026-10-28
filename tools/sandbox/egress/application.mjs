// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import net from "node:net";

const request =
  "GET /gateway HTTP/1.1\r\n" +
  "Host: backup.internal\r\n" +
  "Connection: close\r\n\r\n";
const expected = Buffer.from(
  "HTTP/1.1 200 OK\r\n" +
    "Content-Length: 13\r\n" +
    "Connection: close\r\n\r\n" +
    "engine-native",
);

const socket = net.connect(80, "backup.internal");
const expectClosed = process.env.CELLAR_EGRESS_EXPECT_CLOSED === "1";
const received = [];
let bytes = 0;

socket.on("connect", () => socket.end(request));
socket.on("data", (chunk) => {
  bytes += chunk.length;
  if (bytes > expected.length) process.exit(4);
  received.push(chunk);
});
socket.on("error", (error) => {
  const retired = expectClosed && bytes === 0 && error.code === "ECONNRESET";
  process.exit(retired ? 0 : 2);
});
socket.on("end", () => {
  const correct = expectClosed
    ? bytes === 0
    : Buffer.concat(received).equals(expected);
  process.exit(correct ? 0 : 3);
});
