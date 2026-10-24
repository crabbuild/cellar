// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import type { Decision, Model } from "./provider.js";

// Read the provider's real stream while withholding the complete decision from
// ToolLoopAgent until its Cellar checkpoint has passed storage.sync().
export async function streamDecision(
  model: Model,
  params: Parameters<Model["doGenerate"]>[0],
  preview: (text: string) => void,
): Promise<Decision> {
  const { stream } = await model.doStream(params);
  const reader = stream.getReader();
  const content: Decision["content"] = [];
  const blocks = new Map<
    string,
    Extract<Decision["content"][number], { type: "text" | "reasoning" }>
  >();
  let finish:
    | Extract<
        Awaited<ReturnType<typeof reader.read>>["value"],
        { type: "finish" }
      >
    | undefined;
  let bytes = 0;

  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value.type === "error") throw new Error("provider stream failed");
      if (value.type === "text-start" || value.type === "reasoning-start") {
        const block = {
          type:
            value.type === "text-start"
              ? ("text" as const)
              : ("reasoning" as const),
          text: "",
          providerMetadata: value.providerMetadata,
        };
        blocks.set(value.id, block);
        content.push(block);
      } else if (
        value.type === "text-delta" ||
        value.type === "reasoning-delta"
      ) {
        const block = blocks.get(value.id);
        if (!block) throw new Error("provider stream block missing");
        bytes += Buffer.byteLength(value.delta);
        if (bytes > 400 * 1024)
          throw new Error("provider stream exceeds journal budget");
        block.text += value.delta;
        if (value.providerMetadata)
          block.providerMetadata = {
            ...block.providerMetadata,
            ...value.providerMetadata,
          };
        if (value.type === "text-delta")
          preview(
            content
              .filter((item) => item.type === "text")
              .map((item) => item.text)
              .join(""),
          );
      } else if (value.type === "text-end" || value.type === "reasoning-end") {
        const block = blocks.get(value.id);
        if (!block) throw new Error("provider stream block missing");
        if (value.providerMetadata)
          block.providerMetadata = {
            ...block.providerMetadata,
            ...value.providerMetadata,
          };
        if (
          value.type === "text-end" &&
          value.citations &&
          block.type === "text"
        )
          block.citations = value.citations;
      } else if (value.type === "tool-call") {
        if (value.providerExecuted)
          throw new Error(
            "provider-executed tools need a separate durability contract",
          );
        content.push(value);
      } else if (value.type === "finish") {
        finish = value;
      } else if (
        [
          "file",
          "reasoning-file",
          "tool-result",
          "tool-approval-request",
          "custom",
        ].includes(value.type)
      ) {
        throw new Error("unsupported provider content in text/tool agent");
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
  if (!finish) throw new Error("provider stream ended without a finish event");
  return {
    content,
    finishReason: finish.finishReason,
    usage: finish.usage,
    warnings: [],
  };
}
