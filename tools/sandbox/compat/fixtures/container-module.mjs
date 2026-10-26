// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import * as module from "./celld/container/index.js";
import {
  workersApplication,
  durableObjectNamespace,
} from "../../../../packages/cellar-cloudflare/index.js";

const app = workersApplication(module, {
  namespaces: { MY_CONTAINER: "MyContainer" },
  containers: ["MyContainer"],
});
export const cells = app.cells;
export async function fetch(request, context) {
  const url = new URL(request.url);
  // Harness-only cleanup; the actual application module remains byte-pinned.
  if (url.pathname === "/__destroy") {
    await durableObjectNamespace(context, "MY_CONTAINER")
      .getByName(url.searchParams.get("name"))
      .destroy();
    return { status: 200, headers: [], body: context._encode("destroyed") };
  }
  return app.fetch(request, context);
}
