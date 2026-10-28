// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { setTimeout as delay } from "node:timers/promises";

const program = process.env.CELLAR_FS_HOST;
assert.ok(program, "CELLAR_FS_HOST must name the built cellar-filesystem-agent-example executable");
const entry = fileURLToPath(new URL("./crash-fixture.mjs", import.meta.url));
const root = await mkdtemp(join(tmpdir(), "cellar-filesystem-"));
const children = new Set(), evidence = [];
const supported = ["accepted", "provider", "decision", "tool", "completed", "uncertain-resume", "uncertain-abandon", "uncertain-before-resume", "uncertain-before-abandon"];
const cases = process.env.CELLAR_FS_TEST_CASES?.split(",") ?? supported;
assert.ok(cases.length && cases.every(value => supported.includes(value)), "Select known process cases");

async function until(check, label, timeout = 60_000) {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    const value = await check();
    if (value) return value;
    await delay(50);
  }
  throw new Error(`Timed out: ${label}`);
}

async function start(boundary, generation) {
  let stdout = "", stderr = "";
  const child = spawn(resolve(program), ["--listen", "127.0.0.1:0", "--internal-listen", "127.0.0.1:0"], {
    env: {
      ...process.env, CELLAR_NODE_ENTRY: entry, CELLAR_NODE_EXECUTABLE: process.execPath,
      CELLAR_NODE: `fs-${boundary}-${generation}`, CELLAR_TEST_DATA_DIR: join(root, `${boundary}-${generation}-data`),
      CELLAR_BUCKET: "cellar-dev", CELLAR_INTERNAL_DEV_STORE: join(root, `${boundary}-bucket.sqlite`),
      CELLAR_DURABILITY: "bucket", CELLAR_TOKIO_THREADS: "2", CELLAR_PLACEMENT_WEIGHT: "1",
      CELLAR_WAKER_TICK_MS: "250", CELLAR_REBALANCE_INTERVAL_MS: "0", CELLAR_HANDLER_BUDGET_S: "60",
      CELLAR_SHUTDOWN_TOTAL_MS: "8000", CELLAR_FS_TEST_PAUSE: generation === 1 ? boundary : "",
      CELLAR_FS_TEST_MARKER: join(root, `${boundary}.marker`), CELLAR_FS_TEST_CALLS: join(root, `${boundary}.calls`),
      CELLAR_FS_TEST_COMPLETED: join(root, `${boundary}.completed`), RUST_LOG: "warn",
      CELLAR_FS_TEST_REQUESTS: join(root, `${boundary}.requests`),
      CELLAR_FS_TEST_COMMIT_MARKER: join(root, `${boundary}.committed`),
      CELLAR_FS_TEST_WRITES: join(root, `${boundary}.writes`),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  children.add(child);
  const exited = new Promise(resolve => child.once("exit", (code, signal) => resolve({ code, signal })));
  let spawnError;
  child.on("error", error => { spawnError = error; });
  child.stdout.on("data", chunk => { stdout = (stdout + chunk).slice(-65536); });
  child.stderr.on("data", chunk => { stderr = (stderr + chunk).slice(-65536); });
  const logs = () => `${stdout}\n${stderr}`;
  const alive = () => {
    if (spawnError) throw spawnError;
    if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Host exited: ${logs()}`);
  };
  const addresses = await until(async () => {
    alive();
    const ingress = /cellar listening on (\S+)/.exec(stdout)?.[1];
    const internal = /cellar internal listening on (\S+)/.exec(stdout)?.[1];
    return ingress && internal ? { ingress: `http://${ingress}`, internal: `http://${internal}` } : null;
  }, "host startup");
  await until(async () => {
    alive();
    try { return (await fetch(`${addresses.ingress}/.well-known/cellar/health`)).ok; }
    catch { return false; }
  }, "host readiness");
  return { ...addresses, child, exited, logs, alive };
}

async function readJson(path) {
  try { return JSON.parse(await readFile(path, "utf8")); }
  catch (error) { if (error.code === "ENOENT") return null; throw error; }
}

const post = (url, body) => fetch(url, { method: "POST", body: JSON.stringify(body) });

async function stop(host) {
  assert.ok((await fetch(`${host.internal}/shutdown`, { method: "POST" })).ok);
  const outcome = await Promise.race([host.exited, delay(15_000, undefined, { ref: false }).then(() => { throw new Error(`Shutdown timeout: ${host.logs()}`); })]);
  assert.equal(outcome.code, 0, host.logs());
  children.delete(host.child);
}

async function uncertainControl(boundary) {
  const beforeCommit = boundary.startsWith("uncertain-before-");
  const expectedPhase = beforeCommit ? "decided" : "tool-receipt";
  const first = await start(boundary, 1);
  const prompt = `preserve an uncertain file at ${boundary}`;
  assert.equal((await post(`${first.ingress}/sessions/alpha/turns/one`, { prompt })).status, 202);
  const marker = await until(async () => {
    first.alive();
    return readJson(join(root, `${boundary}.marker`));
  }, `retry exhaustion at ${boundary}`);
  const dispatch = await readJson(join(root, `${boundary}.committed`));
  assert.equal(dispatch.point, beforeCommit ? "not-dispatched" : "committed-reply-lost");
  if (!beforeCommit) assert.equal(marker.version, dispatch.version);
  assert.equal(marker.attempts, 5);
  first.child.kill("SIGKILL");
  await first.exited;
  children.delete(first.child);
  await rm(join(root, `${boundary}-1-data`), { recursive: true, force: true });

  const second = await start(boundary, 2);
  const turnUrl = `${second.ingress}/sessions/alpha/turns/one`;
  const reply = await until(async () => {
    second.alive();
    const value = await fetch(turnUrl);
    return value.ok ? value : null;
  }, "interrupted turn cold recovery");
  const interrupted = await reply.json();
  assert.equal(interrupted.status, "interrupted");
  assert.equal(interrupted.phase, expectedPhase);
  assert.equal(interrupted.step, 0);
  assert.equal(interrupted.attempts, 5);
  assert.equal(interrupted.last_error.outcome, "unknown");
  const snapshot = async () => {
    const reply = await fetch(`${second.ingress}/sessions/alpha/snapshot`);
    assert.ok(reply.ok, second.logs());
    return reply.json();
  };
  const before = await snapshot();
  if (beforeCommit) assert.deepEqual(before, { present: false, receipts: 0, logical: "0", allocated: "0", inodes: "1" });
  else {
    assert.equal(before.text, prompt);
    assert.equal(before.version, dispatch.version);
    assert.equal(before.receipts, 1);
  }
  const same = await post(turnUrl, { prompt });
  assert.equal(same.status, 200);
  assert.deepEqual(await same.json(), interrupted, "duplicate input must not resume an interrupted turn");
  assert.equal((await post(turnUrl, { prompt: "changed", resume: true })).status, 409);
  assert.equal((await post(`${second.ingress}/sessions/alpha/turns/blocked`, { prompt: "other turn" })).status, 409);

  const abandon = boundary.endsWith("-abandon");
  const control = await post(turnUrl, { prompt, [abandon ? "abandon" : "resume"]: true });
  assert.equal(control.status, 200);
  if (abandon) {
    const run = await control.json();
    assert.equal(run.status, "abandoned");
    assert.equal(run.outcome, "unknown");
    assert.equal(run.phase, expectedPhase);
    assert.deepEqual(await snapshot(), before, "abandon must preserve the committed file and receipt");
    const resumed = await post(turnUrl, { prompt, resume: true });
    assert.equal(resumed.status, 200);
    assert.deepEqual(await resumed.json(), run, "an abandoned turn must not restart");
    const followup = `${second.ingress}/sessions/alpha/turns/two`;
    assert.equal((await post(followup, { prompt: "read:/notes.txt" })).status, 202);
    const result = await until(async () => {
      second.alive();
      const value = await (await fetch(followup)).json();
      if (value.status === "interrupted") throw new Error(`Follow-up interrupted: ${JSON.stringify(value)}`);
      return value.status === "completed" ? value : null;
    }, "new turn after abandon");
    if (beforeCommit) assert.match(result.text, /ENOENT/);
    else assert.equal(result.text, prompt);
  } else {
    const resumed = await control.json();
    assert.equal(resumed.status, "pending");
    assert.equal(resumed.attempts, 0);
    const result = await until(async () => {
      second.alive();
      const value = await (await fetch(turnUrl)).json();
      if (value.status === "interrupted") throw new Error(`Resume interrupted: ${JSON.stringify(value)}`);
      return value.status === "completed" ? value : null;
    }, "explicit resume and receipt reconciliation");
    assert.equal(result.step, 2);
    const after = await snapshot();
    assert.equal(after.receipts, 2);
    assert.equal(after.text, prompt);
  }
  const after = await snapshot();
  if (beforeCommit && !abandon) {
    assert.equal(after.present, true);
    assert.equal(typeof after.version, "string");
  } else assert.equal(after.version, before.version, "control/reconciliation repeated the committed file effect");
  let writes;
  try { writes = (await readFile(join(root, `${boundary}.writes`), "utf8")).trim().split("\n"); }
  catch (error) { if (error.code !== "ENOENT") throw error; writes = []; }
  assert.equal(writes.length, beforeCommit && abandon ? 0 : 1, "native file-effect batch count changed");
  const calls = (await readFile(join(root, `${boundary}.requests`), "utf8")).trim().split("\n");
  const one = calls.filter(value => value.includes("/one/"));
  assert.equal(one.filter(value => value.endsWith("/one/0")).length, 1, "saved model decision repeated");
  assert.equal(one.length, abandon ? 1 : 3);
  if (abandon) assert.equal(calls.filter(value => value.includes("/two/")).length, 2);
  assert.equal((await (await fetch(`${second.ingress}/sessions/beta/snapshot`)).json()).present, false);
  await stop(second);
  evidence.push({ boundary, dispatch: dispatch.point, phase: interrupted.phase, attempts: interrupted.attempts, outcome: interrupted.last_error.outcome, before, after, calls, writes });
  console.log(`PASS ${boundary}: ${dispatch.point}, retry exhaustion, SIGKILL/cold recovery, explicit control, ${writes.length} native file-effect batches`);
}

try {
  for (const boundary of cases) {
    if (boundary.startsWith("uncertain-")) {
      await uncertainControl(boundary);
      continue;
    }
    const first = await start(boundary, 1);
    const prompt = `durable file at ${boundary}`;
    // Acceptance may lose its response. Reconciliation always uses the same ID.
    const submission = post(`${first.ingress}/sessions/alpha/turns/one`, { prompt }).catch(() => null);
    const marker = await until(async () => {
      first.alive();
      return readJson(join(root, `${boundary}.marker`));
    }, `checkpoint ${boundary}`);
    assert.equal(marker.point, boundary);
    if (boundary === "provider") {
      const refused = await post(`${first.ingress}/sessions/alpha/turns/one`, { prompt, abandon: true });
      assert.equal(refused.status, 409, "an in-flight workspace step cannot be abandoned");
      const pending = await (await fetch(`${first.ingress}/sessions/alpha/turns/one`)).json();
      assert.equal(pending.status, "pending");
      assert.equal(pending.phase, "model");
    }
    first.child.kill("SIGKILL");
    await first.exited;
    await submission;
    children.delete(first.child);
    await rm(join(root, `${boundary}-1-data`), { recursive: true, force: true });

    // New node identity and empty local storage; the preserved bucket is the
    // only recovery source. No session HTTP request is allowed to wake it.
    const second = await start(boundary, 2);
    if (boundary !== "completed") {
      await until(async () => {
        second.alive();
        try { return (await readFile(join(root, `${boundary}.completed`), "utf8")).includes("completed"); }
        catch (error) { if (error.code === "ENOENT") return false; throw error; }
      }, `alarm-driven cold recovery at ${boundary}`);
    }
    const turnUrl = `${second.ingress}/sessions/alpha/turns/one`;
    const reply = await until(async () => {
      second.alive();
      const reply = await fetch(turnUrl);
      return reply.ok ? reply : null;
    }, "fresh owner admission");
    const run = await reply.json();
    assert.equal(run.status, "completed");
    const snapshot = await (await fetch(`${second.ingress}/sessions/alpha/snapshot`)).json();
    assert.equal(snapshot.text, prompt);
    assert.equal(snapshot.receipts, 2, "write and read must each have one receipt");
    if (marker.version) assert.equal(snapshot.version, marker.version, "recovery repeated a file effect");
    const calls = (await readFile(join(root, `${boundary}.calls`), "utf8")).trim().split("\n");
    assert.equal(calls.length, boundary === "provider" ? 4 : 3, "only uncheckpointed provider work may repeat");
    assert.equal(calls.filter(step => step === "0").length, boundary === "provider" ? 2 : 1);
    assert.equal((await post(turnUrl, { prompt })).status, 200);
    assert.equal((await post(turnUrl, { prompt: "changed" })).status, 409);
    assert.equal((await post(`${second.ingress}/sessions/alpha/turns/bad`, null)).status, 400);
    assert.equal((await (await fetch(`${second.ingress}/sessions/beta/snapshot`)).json()).present, false);

    const followup = `${second.ingress}/sessions/alpha/turns/two`;
    assert.equal((await post(followup, { prompt: "read:/notes.txt" })).status, 202);
    const next = await until(async () => {
      const value = await (await fetch(followup)).json();
      if (value.status === "interrupted") throw new Error(`Follow-up interrupted: ${JSON.stringify(value)}`);
      return value.status === "completed" ? value : null;
    }, "follow-up workspace read");
    assert.equal(next.text, prompt);
    assert.equal((await (await fetch(`${second.ingress}/sessions/alpha/snapshot`)).json()).version, snapshot.version);

    await stop(second);
    evidence.push({ boundary, calls, version: snapshot.version, receipts: snapshot.receipts, followup: next.text });
    console.log(`PASS ${boundary}: SIGKILL, empty-node bucket recovery, atomic file/receipt replay, follow-up workspace`);
  }
  if (process.env.CELLAR_FS_TEST_REPORT) await writeFile(process.env.CELLAR_FS_TEST_REPORT, JSON.stringify({ evidence }, null, 2));
} finally {
  await Promise.all([...children].map(async child => {
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise(resolve => child.once("exit", resolve));
      child.kill("SIGKILL");
      await exited;
    }
  }));
  await rm(root, { recursive: true, force: true });
}
