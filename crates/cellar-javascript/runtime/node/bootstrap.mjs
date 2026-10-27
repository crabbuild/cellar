// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Keep argv small on platforms with per-argument limits. Both the compiled
// runtime and its private configuration arrive over the inherited pipe.
import { readFileSync } from "node:fs";

const exit = process.exit.bind(process);
try {
  const { source, config } = JSON.parse(readFileSync(0, "utf8"));
  // Configuration (including authentication) is a function argument, never part
  // of the module URL, argv, environment or an on-disk bootstrap file.
  const { default: bootstrap } = await import(
    `data:text/javascript;base64,${Buffer.from(source, "utf8").toString("base64")}`
  );
  await bootstrap(config);
} catch {
  // Parse/initialization errors may include private input. Let the native host
  // report its fixed startup failure rather than exposing a Node error stack.
  exit(70);
}
