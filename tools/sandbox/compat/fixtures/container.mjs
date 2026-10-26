// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Container } from "@cloudflare/containers";
import { application, check, refuses } from "./adapter.mjs";
class Probe extends Container {
  async inspect(command, nativeContext) {
    const state = await this.getState();
    check(state.status === "stopped", "published Container state");
    const alarm = await this.ctx.storage.getAlarm();
    check(typeof alarm === "number", "published constructor alarm");
    if (command === "ports") {
      const port = Number(await nativeContext.variable("HTTP_PORT"));
      for (const invalid of [0, -1, 65536, 1.5, NaN]) {
        await refuses(() => this.container.getTcpPort(invalid));
      }
      const tcp = this.container.getTcpPort(port);
      if (typeof Request !== "function") {
        const failure = await refuses(() => tcp.fetch("http://container/"));
        check(
          failure.message.includes("web runtime profile"),
          "missing HTTP profile refuses explicitly",
        );
        return { ports: false };
      }
      try {
        await this.startAndWaitForPorts(port, {
          instanceGetTimeoutMS: 1000,
          portReadyTimeoutMS: 1000,
          waitInterval: 25,
        });
        check(
          (await this.getState()).status === "healthy",
          "published readiness persists healthy state",
        );
        check(this.started === 1, "published onStart hook");
        const response = await tcp.fetch(
          "https://untrusted.invalid/path?query=yes",
        );
        check(
          response.status === 302 &&
            response.headers.get("location") === "https://unrelated.invalid",
          "port capability returns redirect without following it",
        );
        check(
          (await response.text()) === "container response",
          "standard response body",
        );
        return { ports: true };
      } finally {
        const monitor = this.monitor;
        await this.container.destroy();
        await monitor;
        // Join the published monitor's native state callback before asking its
        // own stop-event synchronizer to finish the class lifecycle.
        await this.ctx.blockConcurrencyWhile(async () => {});
        await this.syncPendingStoppedEvents();
        check(
          (await this.getState()).status === "stopped",
          "published stop state",
        );
        check(this.stopped === 1, "published onStop hook");
      }
    }
    if (command === "queued-failure") {
      check(
        this.container.start() === undefined,
        "late failure follows void admission",
      );
      const failure = await refuses(() => this.container.monitor());
      check(
        failure.code === "Ambiguous" && failure.outcome === "unknown",
        "submitted startup retains unknown outcome",
      );
      const retry = await refuses(() => this.container.start());
      check(retry === failure, "failed control is not automatically replayed");
      await this.container.destroy();
      check(!this.container.running, "explicit cleanup after unknown startup");
      return { failure: true };
    }
    if (command === "queued") {
      this.queuedPhase = "validation";
      await this.ctx.storage.put("queued-progress", false);
      for (const options of [
        { image: "not-declared" },
        { env: { "BAD=KEY": "value" } },
        { labels: { "too-long-for-a-key": "value" } },
      ]) {
        await refuses(() => this.container.start(options));
      }
      await this.ctx.storage.transaction(async () => {
        await refuses(() => this.container.start());
      });
      this.ctx.storage.transactionSync(() => {
        try {
          this.container.start();
          throw new Error("queued start in inline transaction");
        } catch (error) {
          check(error.outcome === "not_committed", "inline admission refusal");
        }
      });
      await this.ctx.blockConcurrencyWhile(async () => {
        await refuses(() => this.container.start());
      });
      this.queuedPhase = "start";
      const result = this.container.start({
        env: { MODE: "queued" },
        enableInternet: false,
        labels: { app: "compat" },
      });
      check(result === undefined, "start returns void after admission");
      await refuses(() => this.container.start());
      await this.ctx.storage.put("queued-progress", true);
      this.queuedPhase = "stored";
      const observed = this.container.monitor().then(
        () => ({ ok: true }),
        (error) => ({ ok: false, error }),
      );
      await this.container.destroy();
      this.queuedPhase = "destroyed";
      check((await observed).ok, "explicit destroy resolves monitor normally");
      await this.container.monitor();
      check(
        !this.container.running,
        "queued teardown reaches native stopped state",
      );
      this.container.start();
      const marker = new Error("explicit destroy reason");
      const failed = this.container.monitor().then(
        () => ({ ok: true }),
        (error) => ({ ok: false, error }),
      );
      await this.container.destroy(marker);
      this.queuedPhase = "done";
      check(
        (await failed).error === marker,
        "destroy preserves monitor error identity",
      );
      return { queued: true };
    }
    if (command === "queued-progress") {
      return {
        queuedProgress: this.ctx.storage.kv.get("queued-progress") ?? false,
        running: this.container.running,
        phase: this.queuedPhase,
      };
    }
    if (command === "running") {
      check(!this.container.running, "native stopped snapshot");
      await refuses(() =>
        this.ctx.storage.kv.put("container-capability", this.container),
      );
      check(
        this.ctx.storage.kv.get("container-capability") === undefined,
        "container capability is not persisted as data",
      );
      await nativeContext.container.start();
      check(this.container.running, "snapshot observes actual native startup");
      await this.ctx.storage.transaction(async () => {
        check(
          this.container.running,
          "snapshot inside native async transaction",
        );
      });
      this.ctx.storage.transactionSync(() => {
        check(
          this.container.running,
          "snapshot inside native sync transaction",
        );
      });
      await nativeContext.container.destroy();
      check(
        !this.container.running,
        "snapshot observes actual native teardown",
      );
    }
    if (command === "schedule") {
      const schedule = await this.schedule(60, "callback", { value: 42 });
      const result = await this.getSchedule(schedule.taskId);
      check(result.payload.value === 42, "published SQL schedule cursor");
      this.ctx.storage.kv.put("configuration", { tested: true });
    }
    return {
      state: state.status,
      configuration: this.ctx.storage.kv.get("configuration") ?? null,
    };
  }
  callback() {}
  onStart() {
    this.started = (this.started ?? 0) + 1;
  }
  onStop() {
    this.stopped = (this.stopped ?? 0) + 1;
  }
}
const app = application(Probe, true);
export const { cells, fetch } = app;
