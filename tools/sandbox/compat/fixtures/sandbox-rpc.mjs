// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { getSandbox, Sandbox } from "@cloudflare/sandbox";
import { workersApplication } from "../../../../packages/cellar-cloudflare/index.js";

const gates = new Map();
const path = "/workspace/cellar-upload.bin";
const app = workersApplication(
  {
    Sandbox,
    default: {
      async fetch(request, env) {
        try {
          const url = new URL(request.url);
          const sandbox = getSandbox(env.Sandbox, "rpc-upload");
          if (url.pathname === "/destroy") {
            await sandbox.destroy();
            return new Response("destroyed");
          }
          if (url.pathname === "/release") {
            gates.get("upload")?.resolve();
            return new Response(null, { status: 204 });
          }
          if (url.pathname === "/progress") {
            // The pinned server writes an owned .tmp.<uuid> file and renames
            // only after EOF. Inspect that real prefix in a separate session;
            // commands in the writer's own execution session serialize behind it.
            const command = `node -e "const fs=require('fs'),root='/workspace',base='cellar-upload.bin';const sizes=fs.readdirSync(root).filter(n=>n.startsWith(base+'.tmp.')).map(n=>fs.statSync(root+'/'+n).size);process.stdout.write(JSON.stringify({size:Math.max(0,...sizes),published:fs.existsSync(root+'/'+base)}))"`;
            const result = await sandbox.exec(command, {
              sessionId: "cellar-upload-observer",
            });
            return Response.json({
              exit: result.exitCode,
              ...JSON.parse(result.stdout),
            });
          }
          if (url.pathname === "/verify") {
            const result = await sandbox.exec(
              `node -e "const b=require('fs').readFileSync('${path}');process.stdout.write(JSON.stringify({size:b.length,valid:b.every((v,i)=>v===i%256)}))"`,
            );
            return Response.json({
              exit: result.exitCode,
              ...JSON.parse(result.stdout),
            });
          }
          if (url.pathname === "/upload") {
            const gate = Promise.withResolvers();
            gates.set("upload", gate);
            let offset = 0;
            const size = 2 * 1024 * 1024 + 17;
            const content = new ReadableStream({
              type: "bytes",
              async pull(controller) {
                if (offset >= 65536) await gate.promise;
                if (offset === size) {
                  const pending = controller.byobRequest;
                  controller.close();
                  pending?.respond(0);
                  return;
                }
                const data = new Uint8Array(Math.min(65536, size - offset)).map(
                  (_, i) => (i + offset) % 256,
                );
                offset += data.length;
                controller.enqueue(data);
              },
              cancel() {
                gate.resolve();
              },
            });
            // Actual SDK method. RPC is selected by the declared host variable;
            // neither its file client nor the Cap'n Web transport is rewritten.
            const result = await sandbox.writeFile(path, content);
            return Response.json({
              result,
              bytes: offset,
              locked: content.locked,
            });
          }
          return new Response("missing", { status: 404 });
        } catch (error) {
          return Response.json(
            {
              error: error.message,
              name: error.name,
              code: error.code,
              outcome: error.outcome,
              stack: error.stack,
            },
            { status: 500 },
          );
        }
      },
    },
  },
  {
    namespaces: { Sandbox: "Sandbox" },
    containers: ["Sandbox"],
    variables: ["SANDBOX_TRANSPORT"],
  },
);
export default app;
