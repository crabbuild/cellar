// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import {
  DirectoryBackup,
  DirectoryBackupGateway,
  SandboxBackupError,
  SandboxFileError,
} from "@cloudflare/sandbox";

let abortAfterUpload;

// Fault injection only: the published method performs the real native R2
// allocation first. The package's grant/revocation/shim exchange stays intact.
export class AbortAfterUploadGateway extends DirectoryBackupGateway {
  async createUpload(name) {
    const id = await super.createUpload(name);
    check(
      typeof abortAfterUpload === "function",
      "missing allocation abort hook",
    );
    abortAfterUpload();
    return id;
  }
}

export function check(condition, message) {
  if (!condition) throw new Error(message);
}

export async function rejects(action, expected, message) {
  let failure;
  try {
    await action();
  } catch (error) {
    failure = error;
  }
  check(failure && expected(failure), message);
}

// Both valid operations distinguish revocation from a wrong method refused by
// a still-live read/write grant. This is the actual container/helper path.
const probeGrant = String.raw`
const http = require("node:http");
function request(method, path) {
  return new Promise((resolve, reject) => {
    const headers = method === "GET" ? { range: "bytes=0-0" } : { "content-length": "1" };
    const request = http.request("http://backups.sandbox.internal" + path, {
      method, agent: false, headers,
    }, (response) => {
      let bytes = 0;
      response.on("data", (chunk) => {
        bytes += chunk.length;
        if (bytes > 4096) response.destroy(new Error("grant probe exceeded its body limit"));
      });
      response.on("error", reject);
      response.on("end", () => resolve(response.statusCode));
    });
    request.setTimeout(5000, () => request.destroy(new Error("grant probe timed out")));
    request.on("error", reject);
    request.end(method === "PUT" ? Buffer.from([1]) : undefined);
  });
}
(async () => {
  const statuses = [await request("GET", "/object"), await request("PUT", "/parts/1")];
  process.stdout.write(statuses.join(","));
})().catch(() => { process.exitCode = 1; });
`;

export async function denied(container) {
  const process = await container.exec(["node", "-e", probeGrant]);
  const output = await process.output();
  check(output.exitCode === 0, "grant observation failed");
  check(
    new TextDecoder().decode(output.stdout) === "403,403",
    "failed operation retained a backup grant",
  );
}

export async function preAbortedBackupOperations(workspace) {
  const record = await workspace.ctx.storage.get("backup");
  const reason = new Error("qualification abort before admission");
  const signal = AbortSignal.abort(reason);
  await rejects(
    () => workspace.backups.backup({ dir: "/workspace/source", signal }),
    (error) => error === reason,
    "pre-aborted backup lost its reason",
  );
  await rejects(
    () => workspace.backups.restore(record, { signal }),
    (error) => error === reason,
    "pre-aborted restore lost its reason",
  );
  return Response.json({ backup: true, restore: true });
}

export async function cancelAllocatedBackup(workspace) {
  const reason = new Error("qualification abort after upload allocation");
  const controller = new AbortController();
  let allocations = 0;
  check(abortAfterUpload === undefined, "overlapping allocation abort hooks");
  abortAfterUpload = () => {
    allocations++;
    controller.abort(reason);
  };
  const backup = new DirectoryBackup(
    workspace.ctx.container,
    workspace.ctx.exports.AbortAfterUploadGateway,
    {
      binding: "BACKUPS",
      prefix: "cancelled/",
    },
  );
  try {
    await rejects(
      () =>
        backup.backup({ dir: "/workspace/source", signal: controller.signal }),
      (error) => error === reason,
      "allocated backup cancellation lost its reason",
    );
    check(
      allocations === 1,
      "cancellation did not follow exactly one real allocation",
    );
    await denied(workspace.ctx.container);
    return Response.json({
      aborted: true,
      allocationObserved: true,
      grantsDenied: true,
    });
  } finally {
    abortAfterUpload = undefined;
  }
}

export async function failedBackupOperations(workspace) {
  const { backups, files, ctx } = workspace;
  const record = await ctx.storage.get("backup");
  const path = "/workspace/source/binary";
  const marker = "failed restore must preserve this destination";
  await files.writeFile(path, marker);
  const unchanged = async () => {
    const response = await files.readFile(path);
    check(
      (await response.text()) === marker,
      "failed restore replaced the destination",
    );
  };
  await rejects(
    () => backups.backup({ dir: "/workspace/source/missing-directory" }),
    (error) => SandboxFileError.is(error) && error.code === "ENOENT",
    "missing backup source did not return the published file error",
  );
  await denied(ctx.container);
  await unchanged();

  const sha256 =
    (record.sha256[0] === "0" ? "1" : "0") + record.sha256.slice(1);
  await rejects(
    () => backups.restore({ ...record, sha256 }),
    (error) =>
      SandboxBackupError.is(error) && error.code === "BACKUP_INTEGRITY",
    "corrupt archive record did not return the published integrity error",
  );
  await denied(ctx.container);
  await unchanged();

  const absent = "00000000-0000-4000-8000-000000000000";
  const id =
    record.id === absent ? "11111111-1111-4111-8111-111111111111" : absent;
  await rejects(
    () => backups.restore({ ...record, id }),
    (error) =>
      SandboxBackupError.is(error) && error.code === "BACKUP_NOT_FOUND",
    "missing archive did not return the published backup error",
  );
  await denied(ctx.container);
  await unchanged();
  return Response.json({
    missingSource: true,
    integrity: true,
    missingArchive: true,
    grantsDenied: true,
    destinationPreserved: true,
  });
}
