// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import { fromJson, toJson } from "@bufbuild/protobuf";
import { ProfileRegistration } from "./fixtures/profile-generated/javascript/profile_cellar.js";
import { canonical, corpus, schemas } from "./profile/vectors.mjs";

test("the live-profile corpus exercises every generated method and codec", async () => {
  const vectors = await corpus();
  assert.equal(vectors.length, 18);
  assert.deepEqual(new Set(vectors.map(vector => vector.method)), new Set(Object.keys(schemas)));
  for (const vector of vectors) {
    const schema = schemas[vector.method];
    const rule = ProfileRegistration.methods[`cellar.rpc.conformance.v1.Profile/${vector.method}`];
    const input = fromJson(schema, vector.json);
    const bytes = rule.encodeRequest(input);
    assert.deepEqual([...bytes], vector.bytes, vector.id);
    const reply = rule.decodeResponse(bytes);
    assert.deepEqual(canonical(toJson(schema, reply)), vector.canonical, vector.id);
  }
  const frozen = JSON.parse(await readFile(new URL("fixtures/profile-vectors.json", import.meta.url), "utf8"));
  assert.deepEqual(vectors, frozen, "checked-in native vectors are stale");
});
