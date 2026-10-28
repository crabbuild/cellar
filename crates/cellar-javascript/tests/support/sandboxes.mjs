// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

let retained;
let original;

function check(value, message) {
  if (!value) throw new Error(message);
}

async function refuses(action) {
  try {
    await action();
  } catch {
    return;
  }
  throw new Error("Expected refusal");
}

export async function fetch(request, ctx) {
  if (request.url.endsWith("/outside")) {
    await refuses(() => ctx.sandboxes.get("build"));
    return { status: 200, headers: [], body: "b2s=" };
  }
  const response = await ctx.cell("COUNTERS", "alice").fetch({
    url: request.url,
    method: "GET",
    headers: [],
    body: "",
  });
  return {
    status: response.status,
    headers: [],
    body: ctx._encode(response.body),
  };
}

export class Counter {
  async fetch(request, ctx) {
    const sandboxes = ctx.sandboxes;
    const path = request.url.split("/").at(-1);
    if (path === "start") {
      retained = sandboxes;
      original = await sandboxes.start("build", "SANDBOXES");
      const repeated = await sandboxes.start("build", "SANDBOXES");
      check(
        original.execution_scope === repeated.execution_scope,
        "Duplicate child",
      );
      check(typeof original.generation === "string", "Lossy generation");
      original = repeated;
      await sandboxes.start("preview", "SANDBOXES");
      const page = await sandboxes.list({ limit: 1 });
      check(
        page.allocations.length === 1 && page.next !== null,
        "Missing bounded page",
      );
      check(
        (await sandboxes.inspect(original)).state === "ready",
        "Child is not ready",
      );
      const output = await sandboxes.execute(
        original,
        "container.exec_output",
        {
          options: { cmd: ["echo", "ok"] },
          max_bytes: 1024,
        },
      );
      check(output.exit_code === 7, "Missing child execution");
      await refuses(() => sandboxes.execute(original, "container.exec", {}));
      await refuses(() =>
        sandboxes.destroy({ ...original, generation: "999" }),
      );
    } else if (path === "borrow") {
      await refuses(() => retained.get("build"));
    } else if (path === "destroy") {
      const current = await sandboxes.get("build");
      check(
        (await sandboxes.destroy(current)).state === "deleted",
        "Deletion missing",
      );
    } else if (path === "recreate") {
      const current = await sandboxes.start("build", "SANDBOXES");
      check(
        current.execution_scope !== original.execution_scope,
        "Reused retired identity",
      );
      await refuses(() => sandboxes.execute(original, "container.running", {}));
    } else if (path === "retire") {
      await sandboxes.retire();
      await refuses(() => sandboxes.start("late", "SANDBOXES"));
    } else if (path === "status") {
      const state = (await sandboxes.parent()).state;
      return {
        status: 200,
        headers: [],
        body: state === "deleted" ? "ZGVsZXRlZA==" : "cGVuZGluZw==",
      };
    } else {
      throw new Error("Unknown route");
    }
    return { status: 200, headers: [], body: "b2s=" };
  }
}

export const cells = { Counter };
