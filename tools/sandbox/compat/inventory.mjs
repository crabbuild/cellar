// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Inventory real published SDK input bytes without executing their code. This
// proves fixture identity and import requirements, not runtime compatibility.
import { build } from "esbuild";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, isAbsolute, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

export const root = dirname(fileURLToPath(import.meta.url));
export const digest = bytes => createHash("sha256").update(bytes).digest("hex");
const json = async path => JSON.parse(await readFile(path, "utf8"));

function within(base, path) {
  const absolute = resolve(base, path);
  const rel = relative(base, absolute);
  if (isAbsolute(path) || rel === ".." || rel.startsWith(`..${sep}`)) {
    throw new Error("Inventory path escapes fixture root");
  }
  return absolute;
}

export async function verifyFiles(files, base = root) {
  for (const file of files) {
    const bytes = await readFile(within(base, file.path));
    if (digest(bytes) !== file.sha256) throw new Error(`Input identity changed: ${file.path}`);
    if (file.git_blob) {
      const gitBlob = createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
      if (gitBlob !== file.git_blob) throw new Error(`Upstream blob changed: ${file.path}`);
    }
  }
}

export async function inputs(base = root) {
  const result = await build({
    absWorkingDir: base,
    entryPoints: ["fixtures/celld/container/index.js", "fixtures/celld/sandbox/index.js"],
    outdir: "inventory-output", bundle: true, write: false, metafile: true,
    platform: "neutral", format: "esm", target: "es2023", logLevel: "silent",
    plugins: [{ name: "inventory-runtime-imports", setup(builder) {
      builder.onResolve({ filter: /^(cloudflare:|node:)/ }, args => ({ path: args.path, external: true }));
    } }],
  });
  const files = await Promise.all(Object.keys(result.metafile.inputs).sort().map(async path => ({
    path: path.split(sep).join("/"), sha256: digest(await readFile(within(base, path))),
  })));
  const externals = [...new Set(Object.values(result.metafile.outputs)
    .flatMap(output => output.imports.filter(item => item.external).map(item => item.path)))].sort();
  return { files, externals };
}

export async function verify(manifest, base = root) {
  if (manifest.format !== 1) throw new Error("Unsupported fixture manifest");
  const lockBytes = await readFile(resolve(base, "package-lock.json"));
  if (digest(lockBytes) !== manifest.lock_sha256) throw new Error("Dependency lock changed");
  const lock = JSON.parse(lockBytes);
  const declared = await json(resolve(base, "package.json"));
  for (const pin of manifest.packages) {
    const installed = await json(resolve(base, `node_modules/${pin.name}/package.json`));
    const locked = lock.packages[`node_modules/${pin.name}`];
    if (declared.dependencies[pin.name] !== pin.version || installed.version !== pin.version || locked?.version !== pin.version ||
        locked?.integrity !== pin.integrity || locked?.resolved !== pin.tarball) {
      throw new Error(`Published package identity changed: ${pin.name}`);
    }
  }
  if (!/^docker\.io\/cloudflare\/sandbox@sha256:[a-f0-9]{64}$/.test(manifest.image.reference) ||
      manifest.image.platform !== "linux/amd64") throw new Error("Invalid sandbox image identity");
  const imageManifest = await readFile(within(base, manifest.image.manifest_path));
  if (manifest.image.reference.split("@")[1] !== `sha256:${digest(imageManifest)}` ||
      JSON.parse(imageManifest).config.digest !== manifest.image.config_digest) {
    throw new Error("Sandbox image manifest changed");
  }
  const sandbox = manifest.packages.find(pin => pin.name === "@cloudflare/sandbox");
  if (manifest.image.tag !== `docker.io/cloudflare/sandbox:${sandbox.version}`) {
    throw new Error("Sandbox package/image version mismatch");
  }
  await verifyFiles(manifest.reference.files, base);
  await verifyFiles(manifest.runtime.files, base);
  const actual = await inputs(base);
  if (JSON.stringify(actual) !== JSON.stringify(manifest.runtime)) {
    throw new Error("Published SDK dependency/import graph changed");
  }
  for (const requirement of manifest.requirements) {
    for (const evidence of requirement.evidence) {
      const text = await readFile(within(base, evidence.path), "utf8");
      if (!text.includes(evidence.token)) throw new Error(`Missing API requirement evidence: ${requirement.id}`);
    }
  }
  return {
    reference: manifest.reference.commit,
    packages: manifest.packages.map(pin => `${pin.name}@${pin.version}`),
    inputs: actual.files.length, externals: actual.externals,
    image: manifest.image.reference,
    conformance: "not_run",
  };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const result = await verify(await json(resolve(root, "manifest.json")));
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
}
