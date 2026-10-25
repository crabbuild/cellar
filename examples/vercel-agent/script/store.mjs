import { resolve } from "node:path";

// Select one backend explicitly. In S3 mode the private development-store
// override must be absent, even if it was inherited from a caller's shell.
export function storeEnvironment(directory, environment = process.env) {
  const env = { ...environment, CELLAR_DURABILITY: "bucket" };
  const mode = env.CELLAR_AI_STORE ?? "local";
  delete env.CELLAR_INTERNAL_DEV_STORE;

  if (mode === "local") {
    env.CELLAR_BUCKET = "cellar-dev";
    env.CELLAR_INTERNAL_DEV_STORE = resolve(directory, "bucket.sqlite");
    return env;
  }
  if (mode !== "s3") throw new Error("CELLAR_AI_STORE must be local or s3");
  if (
    !env.CELLAR_BUCKET ||
    !/^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/.test(env.CELLAR_BUCKET)
  ) {
    throw new Error("S3 mode requires CELLAR_BUCKET to be a bucket name");
  }
  if (!env.S3_ENDPOINT) throw new Error("S3 mode requires S3_ENDPOINT");
  const endpoint = new URL(env.S3_ENDPOINT);
  if (
    !["http:", "https:"].includes(endpoint.protocol) ||
    endpoint.username ||
    endpoint.password ||
    endpoint.search ||
    endpoint.hash ||
    endpoint.pathname !== "/"
  ) {
    throw new Error(
      "S3_ENDPOINT must be an HTTP(S) origin without credentials",
    );
  }
  env.AWS_REGION ??= env.AWS_DEFAULT_REGION ?? "us-east-1";
  return env;
}
