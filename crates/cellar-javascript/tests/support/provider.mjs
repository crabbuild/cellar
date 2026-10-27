// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

function check(value, label) {
  if (!value) throw new Error(label);
}

async function refuses(action) {
  try {
    await action();
  } catch (error) {
    check(error.outcome === "not_committed", "pre-effect refusal");
    return;
  }
  throw new Error("expected refusal");
}

const ok = () => ({ status: 200, headers: [], body: "b2s=" });

export class Counter {
  async fetch(request, ctx) {
    try {
      return await this.inspect(request, ctx);
    } catch (error) {
      return {
        status: 500,
        headers: [],
        body: ctx._encode(
          JSON.stringify({
            message: error.message,
            code: error.code,
            outcome: error.outcome,
          }),
        ),
      };
    }
  }
  async inspect(request, ctx) {
    const container = ctx.container;
    const backup = ctx.containers.get("backup");
    const bindings = await ctx.containers.list();
    check(
      bindings.map(({ name }) => name).join(",") === "container,backup",
      "declared slots only",
    );
    check(
      bindings.every(
        ({ capabilities }) =>
          capabilities.provider === request.url.split("/").at(-2),
      ),
      "per-slot capabilities",
    );
    check(
      Object.isFrozen(bindings) &&
        Object.isFrozen(bindings[1].capabilities.operations),
      "frozen discovery",
    );
    await refuses(() => ctx.containers.get("missing").running());
    await refuses(() => ctx.containers.get("../escape").running());
    const capabilities = await container.capabilities();
    check(
      capabilities.provider === request.url.split("/").at(-2),
      "selected backend",
    );
    check(
      capabilities.fencing === "local_checks_and_expiry",
      "weaker ownership",
    );
    check(capabilities.max_resources === 0, "no interactive handles");
    check(
      !capabilities.queued_lifecycle && !capabilities.synchronous_running,
      "portable profile",
    );
    const path = "/tmp/é-?.bin";

    if (request.url.endsWith("/expired")) {
      for (const slot of [container, backup]) {
        let refused = false;
        try {
          await slot.execOutput({ cmd: ["child-exit"] }, 64);
        } catch (error) {
          check(error.code === "ResourceExpired", "typed resource expiry");
          check(error.outcome === "not_committed", "expiry precedes command");
          refused = true;
        }
        check(refused, "expired resource must refuse a command");
      }
      return ok();
    }
    if (request.url.endsWith("/destroy")) {
      await container.destroy();
      check(!(await container.running()), "destroyed");
      check(await backup.running(), "default destruction preserves backup");
      await backup.destroy();
      const retired = await container.reconcile();
      check(
        retired.state === "deleted" && retired.generation === "1",
        "retired binding",
      );
      return ok();
    }
    if (request.url.endsWith("/second")) {
      await refuses(() => this.old.running());
      await refuses(() => this.old.reconcile());
      await refuses(() => this.oldBackup.running());
      check(
        (await backup.readFile(path, 3)).join(",") === "1,2,3",
        "backup bytes in later event",
      );
      const current = await container.reconcile();
      check(
        current.state === "running" && current.generation === "1",
        "reconnected binding",
      );
      check(Object.isFrozen(current), "immutable reconciliation");
      check(await container.running(), "same sandbox in next event");
      check(
        (await container.readFile(path, 3)).join(",") === "0,255,42",
        "persistent binary bytes",
      );
      return ok();
    }

    check(
      capabilities.operations.includes("container.reconcile"),
      "reconciliation capability",
    );
    const empty = await container.reconcile();
    check(
      empty.state === "unbound" && empty.generation === null,
      "unbound before create",
    );
    await refuses(() => container.signal(9));
    check(
      (await container.start({ env: [["HELLO", "world"]] })) === "1",
      "generation",
    );
    check(
      (await backup.reconcile()).state === "unbound",
      "independent binding journal",
    );
    check(
      (await backup.start({ env: [["HELLO", "world"]] })) === "1",
      "backup generation",
    );
    await backup.writeFile(path, new Uint8Array([1, 2, 3]), { mode: 0o640 });
    this.old = container;
    this.oldBackup = backup;
    await container.writeFile(path, new Uint8Array([0, 255, 42]), {
      mode: 0o640,
    });
    check(
      (await container.readFile(path, 3)).join(",") === "0,255,42",
      "exact bytes",
    );
    const metadata = await container.statFile(path);
    check(
      metadata.kind === "file" &&
        metadata.size === 3 &&
        metadata.mode === 0o640,
      "metadata",
    );
    const entries = await container.listFiles("/tmp", {
      max_bytes: 4096,
      max_entries: 1,
    });
    check(
      entries.length === 1 && entries[0].name === "é-?.bin",
      "direct children",
    );
    const output = await container.execOutput({ cmd: ["child-exit"] }, 64);
    check(
      output.exit_code === 7 && output.stdout.join(",") === "0,255,42",
      "nonzero exit and stdout",
    );
    check(
      output.stderr.join(",") === "100,105,97,103,110,111,115,116,105,99",
      "stderr",
    );
    return ok();
  }
}

export const cells = { Counter };
export async function fetch(request, ctx) {
  const response = await ctx.cell("COUNTERS", "alice").fetch({
    url: request.url,
    method: "GET",
    headers: [],
    body: "",
  });
  if (response.status !== 200)
    return {
      status: response.status,
      headers: [],
      body: ctx._encode(response.body),
    };
  check(response.text() === "ok", "cell response");
  return ok();
}
