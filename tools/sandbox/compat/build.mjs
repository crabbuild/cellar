// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { build } from "esbuild";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { root, verify } from "./inventory.mjs";
await verify(
  JSON.parse(await readFile(resolve(root, "manifest.json"), "utf8")),
);
const output = resolve(root, "dist");
await mkdir(output, { recursive: true });
for (const [fixture, platform] of [
  ["lifetimes", "neutral"],
  ["storage", "neutral"],
  ["callbacks", "neutral"],
  ["rpc", "neutral"],
  ["container", "neutral"],
  ["module", "neutral"],
  ["container-module", "neutral"],
  ["sandbox", "node"],
  ["sandbox-module", "node"],
  ["web-streams", "node"],
  ["web-writable", "node"],
]) {
  for (const format of platform === "node" ? ["esm"] : ["iife", "esm"]) {
    const result = await build({
      absWorkingDir: root,
      entryPoints: [`fixtures/${fixture}.mjs`],
      bundle: true,
      write: false,
      format,
      platform,
      globalName: format === "iife" ? "CellarApp" : undefined,
      target: "es2023",
      metafile: true,
      plugins: [
        {
          name: "cellar-cloudflare-profile",
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
    const name = `${fixture}.${format === "iife" ? "bundle.js" : "mjs"}`;
    await writeFile(resolve(output, name), result.outputFiles[0].contents);
    await writeFile(
      resolve(output, `${name}.inputs.json`),
      JSON.stringify(result.metafile, null, 2) + "\n",
    );
  }
}
