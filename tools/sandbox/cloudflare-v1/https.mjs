// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";
import { Files } from "@cloudflare/sandbox";
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { workersApplication } from "../../../packages/cellar-cloudflare/index.js";
import { httpsEntrypoint, httpsProgram } from "./https-program.mjs";

function check(condition, message) {
  if (!condition) throw new Error(message);
}
const sha256 = (bytes) => createHash("sha256").update(bytes).digest("hex");
const bytes = Uint8Array.from(
  { length: 2 * 1024 * 1024 + 17 },
  (_, i) => i % 251,
);

class Echo extends WorkerEntrypoint {
  fetch(request) {
    check(request.url === "https://route.test/message", "HTTPS gateway URL");
    return new Response(request.body, {
      headers: { "x-phase": this.ctx.props.phase },
    });
  }
}

class Workspace extends DurableObject {
  async fetch(request) {
    try {
      return await this.qualify(request);
    } catch (error) {
      console.error(
        "Sandbox HTTPS qualification",
        error.name,
        error.message,
        error.stack,
      );
      throw error;
    }
  }
  async qualify(request) {
    const container = this.ctx.container;
    const route = new URL(request.url).pathname;
    if (route === "/install") {
      await container.interceptOutboundHttps(
        "route.test",
        this.ctx.exports.Echo({ props: { phase: "before" } }),
      );
      container.start({
        entrypoint: ["node", "-e", httpsEntrypoint],
        enableInternet: true,
      });
      await container.setInactivityTimeout(60_000);
      return Response.json({ installed: true });
    }
    if (route === "/destroy") {
      await container.destroy();
      return Response.json({ destroyed: true });
    }
    check(
      route === "/routing" && container.running,
      "install before TLS qualification",
    );
    // Read through the actual published Files implementation and matching shim.
    const certificate = await new Files(container).readFile(
      "/etc/cloudflare/certs/cloudflare-containers-ca.crt",
    );
    const ca = new Uint8Array(await certificate.arrayBuffer());
    check(
      new TextDecoder().decode(ca).includes("BEGIN CERTIFICATE"),
      "missing public CA",
    );
    const process = await container.exec(["node", "-e", httpsProgram], {
      stdin: "pipe",
      stderr: "ignore",
    });
    const writer = process.stdin.getWriter();
    const reader = process.stdout.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    async function reply(command = "next") {
      await writer.write(new TextEncoder().encode(`${command}\n`));
      for (;;) {
        const end = buffered.indexOf("\n");
        if (end >= 0) {
          const line = buffered.slice(0, end);
          buffered = buffered.slice(end + 1);
          const value = JSON.parse(line);
          check(!value.failure, `TLS probe: ${value.failure}`);
          console.error(
            "Sandbox TLS probe",
            command,
            value.status,
            value.phase,
            value.bytes,
          );
          return value;
        }
        const { value, done } = await reader.read();
        check(
          !done && buffered.length + value.length <= 4096,
          "bounded TLS probe output",
        );
        buffered += decoder.decode(value, { stream: true });
      }
    }
    try {
      const replies = [await reply()];
      for (const phase of ["one", "two"]) {
        await container.interceptOutboundHttps(
          "route.test",
          this.ctx.exports.Echo({ props: { phase } }),
        );
        replies.push(await reply());
      }
      check(
        replies.every(
          (value, i) =>
            value.status === 200 &&
            value.phase === ["before", "one", "two"][i] &&
            value.bytes === bytes.length &&
            value.sha256 === sha256(bytes),
        ),
        "retained TLS handler replacement",
      );
      const mismatch = await reply("mismatch");
      check(
        mismatch.status === 502 && mismatch.connection === "close",
        "TLS/HTTP authority mismatch must refuse and close",
      );
      check(
        (await reply("reconnect")).phase === "two",
        "refusal corrupted the run's route",
      );
      check(
        (await reply("untrusted")).untrusted === true,
        "client CA verification",
      );
      const plain = await reply("http");
      check(
        plain.phase === "http-direct" &&
          plain.bytes === bytes.length &&
          plain.sha256 === sha256(bytes),
        "HTTP and HTTPS rules collided",
      );
      await writer.close();
      check(
        (await reader.read()).done && (await process.exitCode) === 0,
        "TLS probe did not finish",
      );
      return Response.json({
        replies: replies.length,
        ca: sha256(ca),
        mismatch: true,
        untrusted: true,
        http: true,
      });
    } finally {
      writer.releaseLock();
      reader.releaseLock();
    }
  }
}

export default workersApplication(
  {
    Workspace,
    Echo,
    default: {
      fetch(request, env) {
        return env.WORKSPACES.getByName("qualified").fetch(request);
      },
    },
  },
  { namespaces: { WORKSPACES: "Workspace" }, containers: ["Workspace"] },
);
