import test from "node:test";
import assert from "node:assert/strict";
import { resolve } from "node:path";
import { storeEnvironment } from "../script/store.mjs";
import { recoveryBucketName } from "./recovery-store.mjs";

test("recovery bucket names stay within S3 limits and isolate scenarios", () => {
  for (const scenario of [
    "external",
    "filesystem",
    "filesystem-read",
    "scenario".repeat(32),
  ]) {
    const first = recoveryBucketName(scenario);
    const second = recoveryBucketName(scenario);
    assert.ok(first.length >= 3 && first.length <= 63, first);
    assert.match(first, /^[a-z0-9][a-z0-9-]*[a-z0-9]$/);
    assert.notEqual(first, second);
    assert.equal(
      storeEnvironment("data", {
        CELLAR_AI_STORE: "s3",
        CELLAR_BUCKET: first,
        S3_ENDPOINT: "http://127.0.0.1:19000",
      }).CELLAR_BUCKET,
      first,
    );
  }
});

test("local mode uses its private development store", () => {
  const env = storeEnvironment("data", {
    CELLAR_INTERNAL_DEV_STORE: "/other/store",
  });
  assert.equal(env.CELLAR_INTERNAL_DEV_STORE, resolve("data/bucket.sqlite"));
  assert.equal(env.CELLAR_DURABILITY, "bucket");
});

test("S3 mode removes an inherited local-store override and preserves credentials", () => {
  const source = {
    CELLAR_AI_STORE: "s3",
    CELLAR_BUCKET: "agent-test",
    S3_ENDPOINT: "http://127.0.0.1:19000",
    AWS_ACCESS_KEY_ID: "test-access",
    AWS_SECRET_ACCESS_KEY: "test-secret",
    CELLAR_INTERNAL_DEV_STORE: "/other/store",
  };
  const env = storeEnvironment("data", source);
  assert.equal(Object.hasOwn(env, "CELLAR_INTERNAL_DEV_STORE"), false);
  assert.equal(env.CELLAR_BUCKET, "agent-test");
  assert.equal(env.AWS_SECRET_ACCESS_KEY, source.AWS_SECRET_ACCESS_KEY);
  assert.equal(env.AWS_REGION, "us-east-1");
  assert.equal(source.CELLAR_INTERNAL_DEV_STORE, "/other/store");
});

test("incomplete or invalid S3 configuration refuses to fall back to local storage", () => {
  for (const value of [
    {},
    { CELLAR_BUCKET: "agent-test" },
    { CELLAR_BUCKET: "../unsafe", S3_ENDPOINT: "http://127.0.0.1:19000" },
    { CELLAR_BUCKET: "agent-test", S3_ENDPOINT: "file:///tmp/store" },
    {
      CELLAR_BUCKET: "agent-test",
      S3_ENDPOINT: "http://user:password@127.0.0.1:19000",
    },
  ]) {
    assert.throws(() =>
      storeEnvironment("data", { CELLAR_AI_STORE: "s3", ...value }),
    );
  }
  assert.throws(() => storeEnvironment("data", { CELLAR_AI_STORE: "unknown" }));
});
