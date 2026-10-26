// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

function check(value, label) {
  if (!value) throw new Error(label);
}
async function refuses(action, outcome) {
  try {
    await action();
  } catch (error) {
    check(error.outcome === outcome, "file failure provenance");
    return;
  }
  throw new Error("file operation unexpectedly succeeded");
}
export class Sandbox {
  async fetch(request, context) {
    const container = context.container;
    const path = "/tmp/cellar-files/é-?.bin";
    if (request.url.endsWith("/write")) {
      await container.start();
      const setup = await container.execOutput(
        {
          cmd: ["node", "-e", "require('fs').mkdirSync('/tmp/cellar-files')"],
        },
        4096,
      );
      check(setup.exit_code === 0, "create independent test directory");
      await container.writeFile(path, new Uint8Array([0, 255, 42]), {
        mode: 0o640,
      });
      await container.writeFile("/tmp/cellar-files/empty", new Uint8Array());
    }
    try {
      const data = await container.readFile(path, 3);
      check(Array.from(data).join() === "0,255,42", "archive binary bytes");
      check(
        (await container.readFile("/tmp/cellar-files/empty", 0)).length === 0,
        "empty file",
      );
      const metadata = await container.statFile(path);
      check(
        metadata.kind === "file" &&
          metadata.size === 3 &&
          metadata.mode === 0o640,
        "daemon stat metadata",
      );
      const files = await container.listFiles("/tmp/cellar-files", {
        max_bytes: 4096,
        max_entries: 2,
      });
      check(
        files.length === 2 && files.some((file) => file.name === "é-?.bin"),
        "daemon direct listing",
      );
      await refuses(() => container.readFile(path, 2), "unknown");
      await refuses(
        () =>
          container.listFiles("/tmp/cellar-files", {
            max_bytes: 4096,
            max_entries: 1,
          }),
        "unknown",
      );
      await refuses(
        () => container.writeFile("../outside", data),
        "not_committed",
      );
      const independent = await container.execOutput(
        {
          cmd: [
            "node",
            "-e",
            "const fs=require('fs'); process.stdout.write(fs.readFileSync('/tmp/cellar-files/é-?.bin').toString('base64')+':'+(fs.statSync('/tmp/cellar-files/é-?.bin').mode&511).toString(8))",
          ],
        },
        4096,
      );
      check(
        independent.exit_code === 0 &&
          new TextDecoder().decode(independent.stdout) === "AP8q:640",
        "process independently observes bytes and permissions",
      );
      if (request.url.endsWith("/read")) {
        await container.writeFile(
          "/tmp/cellar-files/large",
          new Uint8Array(25000),
        );
        await refuses(
          () => container.readFile("/tmp/cellar-files/large", 3),
          "unknown",
        );
        await refuses(
          () => container.listFiles("/tmp/cellar-files", { max_bytes: 4096 }),
          "unknown",
        );
      }
      return { status: 200, headers: [], body: "b2s=" };
    } finally {
      if (request.url.endsWith("/read")) await container.destroy();
    }
  }
}
export const cells = { Sandbox };
export async function fetch(request, context) {
  const response = await context
    .cell("Sandbox", "portable-files")
    .fetch({ url: request.url, method: "GET", headers: [], body: "" });
  return {
    status: response.status,
    headers: [],
    body: context._encode(response.body),
  };
}
