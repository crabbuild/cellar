// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { appendFile, writeFile } from "node:fs/promises";
import { application } from "../dist/application.js";
import { configuredModel } from "../dist/provider.js";
import { configuredObserver, configuredServices } from "../dist/preview.js";
import { toolsModel } from "./tool-fixture.mjs";

const model =
  process.env.CELLAR_AI_TEST_LIVE === "1"
    ? configuredModel()
    : toolsModel(process.env.CELLAR_AI_FILESYSTEM === "1");
let tools = 0;
const stream = model.doStream.bind(model);
model.doStream = async (...args) => {
  await appendFile(process.env.CELLAR_AI_CALLS, "model\n");
  return stream(...args);
};
export default application(
  model,
  async (point) => {
    if (point === "tool") tools++;
    if (point === "completed")
      await appendFile(process.env.CELLAR_AI_COMPLETIONS, "completed\n");
    const pause = process.env.CELLAR_AI_PAUSE_AT;
    if (
      point === pause ||
      (point === "tool" &&
        ((pause === "filesystem" && tools === 1) ||
          (pause === "filesystem-read" && tools === 5)))
    ) {
      await writeFile(process.env.CELLAR_AI_MARKER, point);
      await new Promise(() => {});
    }
  },
  configuredObserver(),
  configuredServices(),
);
