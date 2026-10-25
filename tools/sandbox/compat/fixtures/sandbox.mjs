// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Sandbox } from "@cloudflare/sandbox";
import { application, check } from "./adapter.mjs";
class Probe extends Sandbox {
  async inspect(command) {
    const state = await this.getState();
    check(state.status === "stopped", "published Sandbox state");
    if (command === "configure") {
      await this.setSandboxName("native-fixture", true);
      await this.setKeepAlive(true);
      await this.setSleepAfter("5m");
      await this.setLabels({ fixture: "native" });
    }
    const name = await this.ctx.storage.get("sandboxName");
    check(
      command !== "read" || name === "native-fixture",
      "published Sandbox metadata recovery",
    );
    return { state: state.status, name: name ?? null };
  }
}
const app = application(Probe, true);
export const { cells, fetch } = app;
