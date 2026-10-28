// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// This program runs in the pinned image, across real nft/TLS interception.
// One agent retains its authenticated TLS connection as handlers change.
export const httpsProgram = String.raw`
const https = require("node:https");
const http = require("node:http");
const { readFileSync } = require("node:fs");
const { createHash } = require("node:crypto");
const { createInterface } = require("node:readline");
const ca = readFileSync("/etc/cloudflare/certs/cloudflare-containers-ca.crt");
const body = Buffer.alloc(2 * 1024 * 1024 + 17);
for (let i = 0; i < body.length; i++) body[i] = i % 251;
const agent = new https.Agent({ keepAlive: true, maxSockets: 1, ca });
let firstPort;
async function probe(command) {
  const secure = command !== "http";
  const untrusted = command === "untrusted";
  const payload = untrusted ? Buffer.alloc(0) : body;
  const response = await new Promise((resolve, reject) => {
    const request = (secure ? https : http).request(
      {
        host: "127.0.0.2",
        port: secure ? 443 : 80,
        path: "/message",
        method: "POST",
        servername: secure ? "route.test" : undefined,
        agent: secure && !untrusted ? agent : false,
        headers: {
          host: command === "mismatch" ? "wrong.test" : "route.test",
          "content-length": payload.length,
        },
      },
      resolve,
    );
    request.on("error", reject);
    request.end(payload);
  });
  if (secure && !untrusted) {
    const port = response.socket.localPort;
    firstPort ??= port;
    if (command === "reconnect") {
      if (port === firstPort) throw new Error("refused TLS connection was reused");
      firstPort = port;
    } else if (port !== firstPort) throw new Error("TLS connection was replaced");
  }
  const hash = createHash("sha256");
  let bytes = 0;
  for await (const chunk of response) {
    bytes += chunk.length;
    hash.update(chunk);
  }
  return {
    status: response.statusCode,
    connection: response.headers.connection,
    phase: response.headers["x-phase"],
    bytes,
    sha256: hash.digest("hex"),
  };
}
async function main() {
  const input = createInterface({ input: process.stdin });
  for await (const command of input) {
    if (!["next", "reconnect", "mismatch", "untrusted", "http"].includes(command))
      throw new Error("unexpected TLS command");
    try {
      const result = await probe(command);
      if (command === "untrusted") throw new Error("untrusted CA was accepted");
      console.log(JSON.stringify(result));
    } catch (error) {
      if (
        command !== "untrusted" ||
        ![
          "SELF_SIGNED_CERT_IN_CHAIN",
          "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
        ].includes(error.code)
      )
        throw error;
      console.log(JSON.stringify({ untrusted: true }));
    }
  }
  agent.destroy();
}
main().catch((error) => {
  agent.destroy();
  process.stdout.write(JSON.stringify({ failure: error.message }) + "\n", () =>
    process.exit(1),
  );
});
`;

export const httpsEntrypoint = String.raw`
// Missing injection must fail before the application becomes useful.
require("node:fs").statSync("/etc/cloudflare/certs/cloudflare-containers-ca.crt");
require("node:http")
  .createServer((request, response) => {
    response.writeHead(200, { "x-phase": "http-direct" });
    request.pipe(response);
  })
  .listen(80, "127.0.0.2");
`;
