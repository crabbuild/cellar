// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Container } from "@cloudflare/containers";
import { application, check, refuses } from "./adapter.mjs";
let releaseCallback;
let lateControl;
class Probe extends Container {
  async inspect(command, nativeContext) {
    const state = await this.getState();
    check(state.status === "stopped", "published Container state");
    if (!this.constructorAlarmChecked) {
      const alarm = await this.ctx.storage.getAlarm();
      check(typeof alarm === "number", "published constructor alarm");
      this.constructorAlarmChecked = true;
    }
    if (command === "idle-alarm") {
      check(!this.container.running, "idle alarm requires a stopped container");
      check(
        Number(
          this.sql`SELECT COUNT(*) AS count FROM container_schedules`[0].count,
        ) === 0,
        "idle alarm has no schedules",
      );
      await this.ctx.blockConcurrencyWhile(async () => {
        await this.alarm({ isRetry: false, retryCount: 0 });
        check(
          (await this.ctx.storage.getAlarm()) === null,
          "published idle alarm clears wake",
        );
      });
      return { idle: true };
    }
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
        this.controlInStartHook = true;
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
        const unavailable = port === 1 ? 2 : 1;
        const failure = await refuses(() =>
          this.waitForPort({ portToCheck: unavailable, retries: 1 }),
        );
        check(
          failure.message !== "error hook failure",
          "readiness preserves original failure",
        );
        check(
          this.errors === 1 && typeof this.lastError === "string",
          "published readiness onError hook",
        );
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
        check(
          this.lastStop.exitCode === 0 && this.lastStop.reason === "exit",
          "explicit destroy stop parameters",
        );
      }
      await this.checkCrashHooks(port);
      return { ports: true };
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
    if (command === "callback-void") {
      const result = await this.ctx.blockConcurrencyWhile(() => {
        this.ctx.storage.kv.put("callback-control", "admitted");
        check(this.container.start() === undefined, "callback start is void");
        // This continuation retains the callback's native token, but runs
        // after its body returned while startup is still awaiting the engine.
        lateControl = new Promise((resolve) => {
          releaseCallback = resolve;
        }).then(() => refuses(() => this.container.signal(15)));
        return "joined";
      });
      check(
        result === "joined" && this.container.running,
        "void startup joined",
      );
      await this.ctx.blockConcurrencyWhile(async () => {
        await this.ctx.storage.transaction(async () => {
          await this.ctx.blockConcurrencyWhile(async () => {
            const refusal = await refuses(() => this.container.signal(15));
            check(
              refusal.outcome === "not_committed",
              "inherited transaction refusal",
            );
          });
        });
        await this.ctx.blockConcurrencyWhile(async () => {
          check(this.container.signal(15) === undefined, "nested block signal");
        });
        await this.container.destroy();
        check(!this.container.running, "callback destroy acknowledges cleanup");
        this.ctx.storage.kv.put("callback-control", "complete");
      });
      return { callback: this.ctx.storage.kv.get("callback-control") };
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
  async checkCrashHooks(port) {
    try {
      await this.startAndWaitForPorts(port, {
        instanceGetTimeoutMS: 1000,
        portReadyTimeoutMS: 1000,
        waitInterval: 25,
      });
      const failedMonitor = this.monitor;
      this.container.signal(9);
      const failure = await refuses(() => failedMonitor);
      check(failure.exitCode === 137, "nonzero monitor exit code");
      await this.ctx.blockConcurrencyWhile(async () => {});
      await this.syncPendingStoppedEvents();
      check(
        this.started === 2 && this.stopped === 2,
        "published crash lifecycle hooks",
      );
      check(
        this.lastStop.exitCode === 137 && this.lastStop.reason === "exit",
        "nonzero onStop parameters",
      );
      check(
        this.ctx.storage.kv.get("last-stop").exitCode === 137,
        "onStop durable state",
      );
      check(
        (await this.getState()).status === "stopped",
        "crash hook state persisted",
      );
    } finally {
      await this.container.destroy();
    }
  }
  callback() {}
  onStart() {
    this.started = (this.started ?? 0) + 1;
    if (this.controlInStartHook) this.container.signal(12);
  }
  onStop(params) {
    this.stopped = (this.stopped ?? 0) + 1;
    this.lastStop = params;
    this.ctx.storage.kv.put("last-stop", params);
  }
  onError(error) {
    this.errors = (this.errors ?? 0) + 1;
    this.lastError = error;
    this.ctx.storage.kv.put("readiness-error", typeof error);
    throw new Error("error hook failure");
  }
}
const app = application(Probe, true);
export const { cells } = app;
export async function fetch(request, context) {
  if (request.url.endsWith("/callback-release")) {
    check(typeof releaseCallback === "function", "callback was entered");
    releaseCallback();
    const error = await lateControl;
    return {
      status: 200,
      headers: [],
      body: context._encode(
        JSON.stringify({ code: error.code, outcome: error.outcome }),
      ),
    };
  }
  return app.fetch(request, context);
}
