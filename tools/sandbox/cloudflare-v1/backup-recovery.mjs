// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { DirectoryBackup, DirectoryBackupGateway } from "@cloudflare/sandbox";
import { check, denied, rejects } from "./backup-failures.mjs";

let lostCreation;
const creationFailure = "qualification lost backup create acknowledgement";

// Keep the published allocation, grant exchange and shim. The caller never
// receives the ID, so the SDK cannot issue its normal compensating abort.
export class LostCreateGateway extends DirectoryBackupGateway {
  async createUpload(name) {
    lostCreation.creates++;
    await super.createUpload(name);
    throw new Error(creationFailure);
  }

  async abortUpload(id) {
    lostCreation.aborts++;
    return super.abortUpload(id);
  }
}

export async function loseBackupCreation(workspace) {
  check(lostCreation === undefined, "overlapping lost create hooks");
  lostCreation = { creates: 0, aborts: 0 };
  try {
    const backup = new DirectoryBackup(
      workspace.ctx.container,
      workspace.ctx.exports.LostCreateGateway,
      { binding: "BACKUPS", prefix: "lost-create/" },
    );
    await rejects(
      () => backup.backup({ dir: "/workspace/source" }),
      (error) => error.message.includes(creationFailure),
      "failed create acknowledgement was reported as success",
    );
    check(
      lostCreation.creates === 1 && lostCreation.aborts === 0,
      "unknown-ID allocation was replayed or manually aborted",
    );
    await denied(workspace.ctx.container);
    return Response.json({
      creationUncertain: true,
      noMutationReplay: true,
      noCompensatingAbort: true,
      grantsDenied: true,
    });
  } finally {
    lostCreation = undefined;
  }
}

// Commit a real part, then withhold its receipt from the matching shim. The
// external harness SIGKILLs the host/runtime here; no finally block, SDK abort
// or graceful native drain may supply the positive recovery evidence.
export class CrashPartGateway extends DirectoryBackupGateway {
  async fetch(request) {
    const response = await super.fetch(request);
    if (this.ctx.props.mode === "write") {
      check(response.status === 200, "crash barrier needs a committed part");
      await new Promise(() => {});
    }
    return response;
  }
}

export async function holdBackupPart(workspace) {
  const backup = new DirectoryBackup(
    workspace.ctx.container,
    workspace.ctx.exports.CrashPartGateway,
    { binding: "BACKUPS", prefix: "crash-part/" },
  );
  await backup.backup({ dir: "/workspace/source" });
  throw new Error("crash barrier unexpectedly returned a backup record");
}
