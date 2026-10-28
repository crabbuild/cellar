// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DirectoryBackupGateway } from "@cloudflare/sandbox";
import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import { workersApplication } from "../../../packages/cellar-cloudflare/index.js";

let savedBody, savedBinding;
const grant = {
  protocolVersion: 1,
  binding: "BACKUPS",
  key: "backups/object.zst",
};
class Counter extends DurableObject {
  async fetch() {
    let refusal;
    await this.ctx.storage.transaction(async () => {
      try {
        await this.env.BACKUPS.createMultipartUpload("transaction-leak");
      } catch (error) {
        refusal = { code: error.code, outcome: error.outcome };
      }
    });
    return Response.json(refusal ?? { unexpected: "accepted" });
  }
}
const app = workersApplication(
  {
    DirectoryBackupGateway,
    Counter,
    default: class extends WorkerEntrypoint {
      async fetch(request) {
        const url = new URL(request.url);
        const control = this.ctx.exports.DirectoryBackupGateway({
          props: { ...grant, mode: "control" },
        });
        const uploadId = url.searchParams.get("upload");
        if (url.pathname === "/part-admission") {
          const upload =
            await this.env.BACKUPS.createMultipartUpload("backups/refused");
          const { readable } = new FixedLengthStream(256 * 1024 * 1024 + 1);
          try {
            await upload.uploadPart(1, readable);
            throw new Error("oversized stage admitted");
          } catch (error) {
            return Response.json({
              code: error.code,
              outcome: error.outcome,
              locked: readable.locked,
            });
          } finally {
            await upload.abort();
          }
        }
        if (url.pathname === "/large-stream") {
          const key = "backups/large-stream";
          const upload = await this.env.BACKUPS.createMultipartUpload(key);
          const length = 256 * 1024 + 17;
          const { readable, writable } = new FixedLengthStream(length);
          const writer = writable.getWriter();
          const producer = (async () => {
            try {
              for (let offset = 0; offset < length; offset += 16384)
                await writer.write(
                  Uint8Array.from(
                    { length: Math.min(16384, length - offset) },
                    (_, index) => (offset + index) % 251,
                  ),
                );
              await writer.close();
            } finally {
              writer.releaseLock();
            }
          })();
          try {
            const [part] = await Promise.all([
              upload.uploadPart(1, readable),
              producer,
            ]);
            await upload.complete([part]);
            const object = await this.env.BACKUPS.get(key);
            const body = new Uint8Array(await object.arrayBuffer());
            if (
              body.length !== length ||
              !body.every((byte, index) => byte === index % 251)
            )
              throw new Error("streamed part corrupted bytes");
            await this.env.BACKUPS.delete(key);
            return Response.json({ bytes: body.length, verified: true });
          } catch (error) {
            await upload.abort();
            throw error;
          }
        }
        if (url.pathname === "/short-part") {
          const gateway = this.ctx.exports.DirectoryBackupGateway({
            props: { ...grant, mode: "write", uploadId },
          });
          return gateway.fetch(
            new Request("http://backup/parts/1", {
              method: "PUT",
              body: Uint8Array.of(1),
              headers: { "content-length": "2" },
            }),
          );
        }
        if (url.pathname === "/oversized") {
          let cancelled = false;
          const source = new ReadableStream({
            type: "bytes",
            pull(controller) {
              controller.enqueue(new Uint8Array(1024));
            },
            cancel() {
              cancelled = true;
            },
          });
          try {
            await this.env.BACKUPS.resumeMultipartUpload(
              grant.key,
              uploadId,
            ).uploadPart(1, source);
          } catch (error) {
            return Response.json({ cancelled, name: error.name });
          }
          return Response.json({ unexpected: "accepted" });
        }
        if (url.pathname === "/create")
          return Response.json({
            uploadId: await control.createUpload("backup 雪"),
          });
        if (url.pathname.startsWith("/parts/")) {
          const gateway = this.ctx.exports.DirectoryBackupGateway({
            props: { ...grant, mode: "write", uploadId },
          });
          return gateway.fetch(
            new Request(`http://backup${url.pathname}`, request),
          );
        }
        if (url.pathname === "/complete")
          return Response.json({
            size: await control.completeUpload(uploadId, await request.json()),
          });
        if (url.pathname === "/abort") {
          await control.abortUpload(uploadId);
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/delete") {
          await control.deleteObject();
          return new Response(null, { status: 204 });
        }
        if (url.pathname === "/object") {
          return this.ctx.exports
            .DirectoryBackupGateway({ props: { ...grant, mode: "read" } })
            .fetch(new Request("http://backup/object", request));
        }
        if (url.pathname === "/head") {
          const object = await this.env.BACKUPS.head(grant.key);
          if (object === null) return Response.json(null);
          const headers = new Headers();
          object.writeHttpMetadata(headers);
          return Response.json({
            size: object.size,
            custom: object.customMetadata,
            contentType: headers.get("content-type"),
            uploaded: object.uploaded instanceof Date,
            etag: object.etag,
            httpEtag: object.httpEtag,
          });
        }
        if (url.pathname === "/get") {
          let stage = "get";
          try {
            const object = await this.env.BACKUPS.get(grant.key);
            if (!object) return new Response(null, { status: 404 });
            const before = object.bodyUsed;
            stage = "arrayBuffer";
            const bytes = new Uint8Array(await object.arrayBuffer());
            return Response.json({
              bytes: [...bytes],
              before,
              after: object.bodyUsed,
            });
          } catch (error) {
            return Response.json(
              {
                stage,
                message: error.message,
                code: error.code,
                outcome: error.outcome,
              },
              { status: 500 },
            );
          }
        }
        if (url.pathname === "/save") {
          savedBody = await this.env.BACKUPS.get(grant.key);
          savedBinding = this.env.BACKUPS;
          return new Response("saved");
        }
        if (url.pathname === "/borrow") {
          const refused = [];
          for (const [name, operation] of [
            ["binding", () => savedBinding.head(grant.key)],
            ["body", () => savedBody.arrayBuffer()],
          ]) {
            try {
              await operation();
            } catch {
              refused.push(name);
            }
          }
          return Response.json(refused);
        }
        if (url.pathname === "/transaction")
          return this.env.COUNTERS.getByName("transaction").fetch(request);
        return new Response("unknown fixture route", { status: 404 });
      }
    },
  },
  { r2: ["BACKUPS"], namespaces: { COUNTERS: "Counter" } },
);
export const { fetch, cells, worker_rpc } = app;
