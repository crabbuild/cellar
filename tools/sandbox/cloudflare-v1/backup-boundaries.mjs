// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { createHash } from "node:crypto";
import { DirectoryBackup, DirectoryBackupGateway } from "@cloudflare/sandbox";
import { check, denied, rejects } from "./backup-failures.mjs";

let transfer;
let mutation;

// The real gateway performs its ordinary storage operations. Only the first
// body chunk is interrupted; the SDK and matching shim still own cancellation.
export class TransferAbortGateway extends DirectoryBackupGateway {
  async fetch(request) {
    if (this.ctx.props.mode === "deny") return super.fetch(request);
    check(transfer !== undefined, "missing transfer abort hook");
    if (this.ctx.props.mode === "write") {
      check(request.method === "PUT", "expected a real shim part upload");
      check(Number(request.headers.get("content-length")) > 1, "short part");
      request = new Request(request, {
        body: interrupted(request.body),
        duplex: "half",
      });
      return super.fetch(request);
    }
    const response = await super.fetch(request);
    check(
      this.ctx.props.mode === "read" && response.status === 206,
      "expected a real native archive range",
    );
    return new Response(interrupted(response.body), response);
  }
}

function interrupted(body) {
  return body.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        check(
          chunk instanceof Uint8Array && chunk.length > 1,
          "transfer needs a nonempty native body chunk",
        );
        transfer.chunks++;
        transfer.bytes += chunk.length;
        controller.enqueue(chunk.subarray(0, 1));
        transfer.controller.abort(transfer.reason);
        throw transfer.reason;
      },
    }),
  );
}

export async function cancelBackupTransfers(workspace) {
  check(transfer === undefined, "overlapping transfer abort hooks");
  const path = "/workspace/source/binary";
  const marker = "cancelled restore must preserve this destination";
  const backup = new DirectoryBackup(
    workspace.ctx.container,
    workspace.ctx.exports.TransferAbortGateway,
    { binding: "BACKUPS", prefix: "interrupted/" },
  );
  try {
    for (const kind of ["backup", "restore"]) {
      transfer = {
        controller: new AbortController(),
        reason: new Error(`qualification abort during ${kind} transfer`),
        chunks: 0,
        bytes: 0,
      };
      if (kind === "restore") await workspace.files.writeFile(path, marker);
      const record = await workspace.ctx.storage.get("backup");
      const options = { signal: transfer.controller.signal };
      await rejects(
        () =>
          kind === "backup"
            ? backup.backup({ ...options, dir: "/workspace/source" })
            : new DirectoryBackup(
                workspace.ctx.container,
                workspace.ctx.exports.TransferAbortGateway,
                { binding: "BACKUPS", prefix: "qualified/" },
              ).restore(record, options),
        (error) => error === transfer.reason,
        `${kind} transfer lost the caller's abort reason`,
      );
      check(
        transfer.chunks === 1 && transfer.bytes > 1,
        `${kind} did not enter exactly one native transfer chunk`,
      );
      await denied(workspace.ctx.container);
      if (kind === "restore") {
        const destination = await workspace.files.readFile(path);
        check(
          (await destination.text()) === marker,
          "cancelled restore replaced its destination",
        );
      }
    }
    return Response.json({
      uploadAborted: true,
      downloadAborted: true,
      destinationPreserved: true,
      grantsDenied: true,
    });
  } finally {
    transfer = undefined;
  }
}

const completionFailure = "qualification lost completion acknowledgement";
const deletionFailure = "qualification lost deletion acknowledgement";

// A committed mutation is followed by a failed control response. These hooks
// do not replace/replay storage operations or turn failure into rollback.
export class LostAcknowledgementGateway extends DirectoryBackupGateway {
  async createUpload(name) {
    mutation.creates++;
    return super.createUpload(name);
  }

  async completeUpload(uploadId, parts) {
    mutation.completions++;
    const size = await super.completeUpload(uploadId, parts);
    mutation.committed = { key: this.ctx.props.key, size };
    throw new Error(completionFailure);
  }

  async abortUpload(uploadId) {
    mutation.aborts++;
    return super.abortUpload(uploadId);
  }

  async deleteObject() {
    mutation.deletions++;
    await super.deleteObject();
    throw new Error(deletionFailure);
  }
}

export async function lostBackupAcknowledgements(workspace) {
  check(mutation === undefined, "overlapping mutation observation hooks");
  mutation = { creates: 0, completions: 0, aborts: 0, deletions: 0 };
  const backup = new DirectoryBackup(
    workspace.ctx.container,
    workspace.ctx.exports.LostAcknowledgementGateway,
    { binding: "BACKUPS", prefix: "uncertain/" },
  );
  try {
    await rejects(
      () => backup.backup({ dir: "/workspace/source" }),
      (error) => error.message.includes(completionFailure),
      "failed completion acknowledgement was reported as success",
    );
    check(
      mutation.creates === 1 &&
        mutation.completions === 1 &&
        mutation.aborts === 1,
      "completion response loss replayed a mutation or omitted SDK cleanup",
    );
    check(
      mutation.committed !== undefined,
      "completion did not reach native storage",
    );
    const { key, size } = mutation.committed;
    const object = await workspace.env.BACKUPS.get(key);
    check(
      object !== null && object.size === size && size > 0,
      "failed acknowledgement was treated as object rollback",
    );
    // The fault observer knows the committed key. Production callers cannot
    // infer this record from a failed backup call; reconcile explicitly.
    const bytes = new Uint8Array(await object.arrayBuffer());
    const record = {
      id: key.slice("uncertain/".length, -".tar.zst".length),
      dir: "/workspace/source",
      size,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      format: "tar+zstd/1",
    };
    await denied(workspace.ctx.container);
    await rejects(
      () => backup.delete(record),
      (error) => error.message.includes(deletionFailure),
      "failed deletion acknowledgement was reported as success",
    );
    check(
      mutation.deletions === 1,
      "deletion response loss replayed a mutation",
    );
    check(
      (await workspace.env.BACKUPS.head(key)) === null,
      "real deletion did not commit before acknowledgement failure",
    );
    return Response.json({
      completionUncertain: true,
      committedObjectObserved: true,
      deletionUncertain: true,
      deletedObjectAbsent: true,
      noMutationReplay: true,
    });
  } finally {
    mutation = undefined;
  }
}
