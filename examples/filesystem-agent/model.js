// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

/** Deterministic model fixture for running the durable boundaries without an
 * API key. Replace this adapter with a provider using the original ctx SDK. */
export const scriptedFilesystemModel = {
  configuration: "filesystem-agent-scripted-model-v1",
  async decide({ prompt, observations, step }) {
    const refused = observations.find(value => value.result.error);
    if (refused) return { type: "complete", text: `File tool refused: ${refused.result.error.code} ${refused.result.error.errno ?? ""}` };
    if (prompt.startsWith("read:")) {
      return step === 0
        ? { type: "tool", id: "read", request: { operation: "read", path: prompt.slice(5).trim() } }
        : { type: "complete", text: observations.at(-1).result.text ?? "Read binary data; inspect the saved tool result." };
    }
    if (step === 0) return { type: "tool", id: "write", request: { operation: "write", path: "/notes.txt", content: prompt } };
    if (step === 1) return { type: "tool", id: "read", request: { operation: "read", path: "/notes.txt" } };
    return { type: "complete", text: "Saved notes.txt and verified its recorded read result." };
  },
};
