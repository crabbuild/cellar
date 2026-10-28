#!/usr/bin/env -S node --max-old-space-size=128
// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// A test supervisor around the unmodified production bootstrap and TCP frames.
// Configuration/authentication stay in memory. The relay owns no SDK authority.
import { readFileSync, writeFileSync, existsSync, renameSync } from "node:fs";
import { createServer, connect } from "node:net";
import { createHash } from "node:crypto";
import {
  Worker,
  isMainThread,
  workerData,
  parentPort,
} from "node:worker_threads";

if (isMainThread) {
  const { source, config } = JSON.parse(readFileSync(0, "utf8"));
  const relay = new Worker(new URL(import.meta.url), {
    workerData: {
      port: config.port,
      maximum: Math.min(config.valueLimit * 6 + 4096, config.bridgeLimit),
      operation: process.env.CELLAR_FS_IO_OPERATION,
      occurrence: Number(process.env.CELLAR_FS_IO_OCCURRENCE),
      report: process.env.CELLAR_FS_IO_RELAY_REPORT,
      release: process.env.CELLAR_FS_IO_RELAY_RELEASE,
    },
  });
  relay.on("error", () => process.exit(70));
  relay.on("exit", () => process.exit(70));
  config.port = await new Promise((resolve) => relay.once("message", resolve));
  const { default: bootstrap } = await import(
    `data:text/javascript;base64,${Buffer.from(`${source}\n//# sourceURL=cellar:node-bootstrap\n`).toString("base64")}`
  );
  await bootstrap(config);
} else {
  const config = workerData;
  const operations = new Map();
  const report = {
    format: 1,
    matches: 0,
    held: null,
    cancellations: 0,
    released: false,
  };
  let held, downstream;
  const save = () => {
    writeFileSync(`${config.report}.next`, JSON.stringify(report));
    renameSync(`${config.report}.next`, config.report);
  };
  const fail = () => process.exit(70);
  const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
  function frames(socket, receive) {
    let input = Buffer.alloc(0);
    socket.on("error", fail);
    socket.on("data", (chunk) => {
      input = Buffer.concat([input, chunk]);
      if (input.length > config.maximum * 2 + 16) fail();
      while (input.length >= 8) {
        const metadata = input.readUInt32BE(),
          body = input.readUInt32BE(4);
        if (!metadata || metadata + body > config.maximum) fail();
        if (input.length < 8 + metadata + body) break;
        const frame = input.subarray(0, 8 + metadata + body);
        input = input.subarray(frame.length);
        const value = JSON.parse(frame.subarray(8, 8 + metadata));
        receive(value, frame);
      }
    });
  }
  const listener = createServer((worker) => {
    listener.close();
    downstream = worker;
    const host = connect({ host: "127.0.0.1", port: config.port });
    for (const socket of [worker, host]) socket.setNoDelay(true);
    frames(worker, (value, frame) => {
      if (
        value.type === "operation" &&
        value.name === `filesystem.${config.operation}`
      ) {
        report.matches++;
        operations.set(value.id, {
          invocation: value.invocation,
          ordinal: report.matches,
        });
      }
      host.write(frame);
    });
    frames(host, (value, frame) => {
      const operation =
        value.type === "reply" ? operations.get(value.id) : null;
      if (operation) operations.delete(value.id);
      if (operation?.ordinal === config.occurrence) {
        if (held || report.held || value.invocation !== operation.invocation)
          fail();
        const result = JSON.parse(value.payload);
        if (!result.ok) fail();
        held = Buffer.from(frame);
        report.held = {
          operation: config.operation,
          ordinal: operation.ordinal,
          invocation: value.invocation,
          id: value.id,
          value: result.value,
          frame_sha256: digest(frame),
        };
        save();
        return;
      }
      if (value.type === "cancel" && value.id === report.held?.invocation) {
        report.cancellations++;
        save();
      }
      worker.write(frame);
    });
  });
  listener.on("error", fail);
  listener.listen(0, "127.0.0.1", () =>
    parentPort.postMessage(listener.address().port),
  );
  setInterval(() => {
    if (held && existsSync(config.release)) {
      if (digest(held) !== report.held.frame_sha256) fail();
      downstream.write(held);
      held = null;
      report.released = true;
      save();
    }
  }, 5);
}
