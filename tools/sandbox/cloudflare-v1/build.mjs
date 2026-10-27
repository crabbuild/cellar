// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { build } from "esbuild";
import { resolve } from "node:path";
import { root, verify } from "./inventory.mjs";
await verify();
for (const fixture of ["fixture", "gateways", "r2"])
  await build({
    absWorkingDir: root,
    entryPoints: [`${fixture}.mjs`],
    outfile: `dist/${fixture}.mjs`,
    bundle: true,
    format: "esm",
    platform: "node",
    target: "node24",
    plugins: [
      {
        name: "cellar-workers",
        setup(builder) {
          builder.onResolve({ filter: /^cloudflare:workers$/ }, () => ({
            path: resolve(
              root,
              "../../../packages/cellar-cloudflare/workers.js",
            ),
          }));
        },
      },
    ],
  });
