// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { getSandbox, proxyToSandbox, Sandbox } from "@cloudflare/sandbox";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

class PreviewProbe extends Sandbox {
  // Stop the runtime without revoking durable preview authorization. Published
  // monitor/state/onStop code must invalidate its current-runtime activation.
  async stopForPreview() {
    // Exercise the real runtime stop. The SDK's own monitor and subsequent
    // alarm/start path synchronize hooks; no extra harness input gate is added.
    await this.ctx.container.destroy();
  }
}

const server = `
  const http = require('http');
  http.createServer((req, res) => {
    const chunks = [];
    req.on('data', chunk => chunks.push(chunk));
    req.on('end', () => {
      res.statusCode = 207;
      res.setHeader('x-preview-method', req.method);
      res.setHeader('x-preview-path', req.url);
      res.setHeader('x-preview-host', req.headers['x-forwarded-host'] ?? '');
      res.setHeader('x-preview-internal', Object.keys(req.headers).some(k => k.startsWith('x-sandbox-preview-')) ? 'leaked' : 'clean');
      res.end(Buffer.concat(chunks));
    });
  }).listen(8080, '0.0.0.0');
`;

export default workersApplication(
  {
    Sandbox: PreviewProbe,
    default: {
      async fetch(request, env) {
        try {
          // The actual SDK helper parses the preview hostname, strips forged
          // internal headers and lets Sandbox validate token + runtime identity.
          const preview = await proxyToSandbox(request, env);
          if (preview) return preview;
          const sandbox = getSandbox(env.Sandbox, "preview");
          const url = new URL(request.url);
          if (url.pathname === "/setup") {
            const process = await sandbox.startProcess(
              "node -e " + JSON.stringify(server.replaceAll("\n", " ")),
            );
            await process.waitForPort(8080, { mode: "tcp", timeout: 10000 });
            return Response.json(
              await sandbox.exposePort(8080, { hostname: "preview.test" }),
            );
          }
          if (url.pathname === "/stop") {
            await sandbox.stopForPreview();
            return new Response(null, { status: 204 });
          }
          if (url.pathname === "/wake") {
            const result = await sandbox.exec("printf awake");
            return new Response(result.stdout);
          }
          if (url.pathname === "/expose")
            return Response.json(
              await sandbox.exposePort(8080, { hostname: "preview.test" }),
            );
          if (url.pathname === "/unexpose") {
            await sandbox.unexposePort(8080);
            return new Response(null, { status: 204 });
          }
          if (url.pathname === "/ports")
            return Response.json(await sandbox.getExposedPorts("preview.test"));
          if (url.pathname === "/destroy") {
            await sandbox.destroy();
            return new Response(null, { status: 204 });
          }
          return new Response("missing", { status: 404 });
        } catch (error) {
          console.error("Sandbox preview request failed", error);
          return Response.json(
            {
              name: error?.name,
              message: error?.message,
              code: error?.code,
              outcome: error?.outcome,
            },
            { status: 500 },
          );
        }
      },
    },
  },
  { namespaces: { Sandbox: "Sandbox" }, containers: ["Sandbox"] },
);
