// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import * as module from "./celld/sandbox/index.js";
import {
  durableObjectNamespace,
  workersApplication,
} from "../../../../packages/cellar-cloudflare/index.js";

const app = workersApplication(module, {
  namespaces: { Sandbox: "Sandbox" },
  containers: ["Sandbox"],
});
export const cells = app.cells;
export async function fetch(request, context) {
  const url = new URL(request.url);
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
