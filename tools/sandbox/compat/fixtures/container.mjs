// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Container } from "@cloudflare/containers";
import { application, check } from "./adapter.mjs";
class Probe extends Container {
  async inspect(command) {
    const state = await this.getState();
    check(state.status === "stopped", "published Container state");
    const alarm = await this.ctx.storage.getAlarm();
    check(typeof alarm === "number", "published constructor alarm");
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
}
const app = application(Probe, true);
export const { cells, fetch } = app;
