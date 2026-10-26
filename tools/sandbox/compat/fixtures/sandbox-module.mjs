// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import * as module from "./celld/sandbox/index.js";
import { parseSSEStream } from "@cloudflare/sandbox";
import {
  durableObjectNamespace,
  workersApplication,
} from "../../../../packages/cellar-cloudflare/index.js";

function gatedCommand(gate) {
  return (
    "node -e " +
    JSON.stringify(
      `
      const fs = require('fs');
      process.stdout.write('cellar-first\\n');
      let attempts = 0;
      const timer = setInterval(() => {
        if (fs.existsSync('${gate}')) {
          clearInterval(timer);
          process.stderr.write('cellar-stderr\\n');
          process.stdout.write('cellar-last\\n');
          process.exitCode = 7;
        } else if (++attempts === 150) {
          clearInterval(timer);
          process.exitCode = 23;
        }
      }, 100);
    `.replaceAll("\n", " "),
    )
  );
}

// Extend the published class only to observe its actual streaming method. The
// unchanged upstream routes below continue to use the same SDK implementation.
const RPC_GATE = "/workspace/cellar-rpc-stream-release";
const HTTP_GATE = "/workspace/cellar-http-stream-release";
class StreamingProbe extends module.Sandbox {
  async releaseHttpStream() {
    await this.exec(`touch ${HTTP_GATE}`);
  }

  async fetch(request) {
    if (new URL(request.url).pathname === "/cellar-http-stream") {
      await this.exec(`rm -f ${HTTP_GATE}`);
      return new Response(await this.execStream(gatedCommand(HTTP_GATE)), {
        headers: { "content-type": "text/event-stream" },
      });
    }
    return super.fetch(request);
  }
  async qualifyStream() {
    const gate = "/workspace/cellar-stream-release";
    await this.exec(`rm -f ${gate}`);
    const command = gatedCommand(gate);
    let released = false;
    const events = [];
    try {
      const stream = await this.execStream(command);
      for await (const event of parseSSEStream(stream)) {
        events.push(event);
        if (
          !released &&
          event.type === "stdout" &&
          event.data?.includes("cellar-first")
        ) {
          // The command cannot finish until the reader observes its first
          // stdout event. A buffered port fetch cannot pass this handshake.
          await this.exec(`touch ${gate}`);
          released = true;
        }
      }
      return { released, events };
    } finally {
      await this.exec(`rm -f ${gate}`);
    }
  }
}

const app = workersApplication(
  {
    ...module,
    Sandbox: StreamingProbe,
    default: {
      async fetch(request, env, execution) {
        const url = new URL(request.url);
        const stub = env.Sandbox.getByName(
          url.searchParams.get("id") ?? "demo",
        );
        if (url.pathname === "/__stream_rpc") {
          await stub.exec(`rm -f ${RPC_GATE}`);
          return new Response(await stub.execStream(gatedCommand(RPC_GATE)), {
            headers: { "content-type": "text/event-stream" },
          });
        }
        if (url.pathname === "/__stream_rpc_release") {
          await stub.exec(`touch ${RPC_GATE}`);
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/__stream_http")
          return stub.fetch("http://sandbox/cellar-http-stream");
        if (url.pathname === "/__stream_release") {
          await stub.releaseHttpStream();
          return new Response(null, { status: 204 });
        }
        return module.default.fetch(request, env, execution);
      },
    },
  },
  {
    namespaces: { Sandbox: "Sandbox" },
    containers: ["Sandbox"],
  },
);
export const cells = app.cells;
export async function fetch(request, context) {
  const url = new URL(request.url);
  if (url.pathname === "/__stream") {
    const result = await durableObjectNamespace(context, "Sandbox")
      .getByName(url.searchParams.get("id") ?? "demo")
      .qualifyStream();
    return {
      status: 200,
      headers: [],
      body: context._encode(JSON.stringify(result)),
    };
  }
  // Harness-only cleanup; the upstream application and SDK stay unchanged.
  if (url.pathname === "/__destroy") {
    await durableObjectNamespace(context, "Sandbox")
      .getByName(url.searchParams.get("id") ?? "demo")
      .destroy();
    return { status: 200, headers: [], body: context._encode("destroyed") };
  }
  try {
    return await app.fetch(request, context);
  } catch (error) {
    console.error("Sandbox module request failed", error);
    throw error;
  }
}
