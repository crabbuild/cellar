// Fault controls belong to this test entry, never the production app.ts module.
import { appendFile, writeFile } from "node:fs/promises";
import { application } from "../dist/application.js";
import { configuredModel, fixtureModel } from "../dist/provider.js";

const model =
  process.env.CELLAR_AI_TEST_LIVE === "1" ? configuredModel() : fixtureModel();
const generate = model.doGenerate.bind(model);
model.doGenerate = async (...args) => {
  if (process.env.CELLAR_AI_PAUSE_AT === "accepted")
    await new Promise(() => {});
  await appendFile(process.env.CELLAR_AI_CALLS, "model\n");
  if (process.env.CELLAR_AI_PAUSE_AT === "provider") {
    await writeFile(process.env.CELLAR_AI_MARKER, "provider");
    await new Promise(() => {});
  }
  return generate(...args);
};
let checkpoints = 0;
export default application(model, async (point) => {
  if (point === "completed")
    await appendFile(process.env.CELLAR_AI_COMPLETIONS, "completed\n");
  if (point === "model") checkpoints++;
  if (
    point === process.env.CELLAR_AI_PAUSE_AT ||
    (point === "model" &&
      checkpoints === 2 &&
      process.env.CELLAR_AI_PAUSE_AT === "final-model")
  ) {
    await writeFile(process.env.CELLAR_AI_MARKER, point);
    await new Promise(() => {});
  }
});
