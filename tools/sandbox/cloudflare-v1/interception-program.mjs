// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Runs inside the matching Sandbox image. One agent retains one TCP connection
// while the owning Durable Object installs and replaces its HTTP handler.
export const interceptionProgram = String.raw`
const http = require("node:http");
const { createHash } = require("node:crypto");
const { createInterface } = require("node:readline");
const body = Buffer.alloc(2 * 1024 * 1024 + 17);
for (let i = 0; i < body.length; i++) body[i] = i % 251;
const agent = new http.Agent({ keepAlive: true, maxSockets: 1 });
let firstPort;
async function request(host) {
  const response = await new Promise((resolve, reject) => {
    const request = http.request({
      host: "127.0.0.2", port: 80, path: "/message", method: "POST", agent,
      headers: { host, "content-length": body.length },
    }, resolve);
    request.on("error", reject);
    request.end(body);
  });
  const port = response.socket.localPort;
  firstPort ??= port;
  if (port !== firstPort) throw new Error("interception replaced the TCP connection");
  const hash = createHash("sha256");
  let bytes = 0;
  for await (const chunk of response) { bytes += chunk.length; hash.update(chunk); }
  console.log(JSON.stringify({ status: response.statusCode, phase: response.headers["x-phase"], bytes, sha256: hash.digest("hex") }));
}
async function main() {
  const input = createInterface({ input: process.stdin });
  for await (const line of input) {
    if (line !== "next" && line !== "before") throw new Error("unexpected probe command");
    await request(line === "before" ? "before.test" : "route.test");
  }
  agent.destroy();
}
main().catch((error) => {
  agent.destroy();
  process.stdout.write(JSON.stringify({ failure: error.message }) + "\n", () => process.exit(1));
});
`;

export const directServer = String.raw`
require("node:http").createServer((request, response) => {
  response.writeHead(200, { "x-phase": "direct" });
  request.pipe(response);
}).listen(80, "127.0.0.2");
`;
