// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { fixtureModel } from "../dist/provider.js";

// A provider fixture only; ToolLoopAgent owns parsing, validation and execution.
export function toolsModel(filesystem = false) {
  const model = fixtureModel();
  const baseline = model.doGenerate;
  model.doGenerate = async (params) => {
    const result = await baseline(params);
    const replies = params.prompt
      .filter((message) => message.role === "tool")
      .flatMap((message) => message.content);
    const names = new Set(replies.map((part) => part.toolName));
    const call = (toolName, input) => ({
      type: "tool-call",
      toolName,
      toolCallId: `call-${toolName}`,
      input: JSON.stringify(input),
    });
    if (filesystem) {
      const original = replies.find(
        (part) => part.toolCallId === "call-readFile",
      )?.output?.value;
      result.content = !names.has("writeFile")
        ? [
            call("writeFile", {
              path: "/workspace.txt",
              content: "first contents",
            }),
          ]
        : !names.has("readFile")
          ? [
              call("readFile", { path: "/workspace.txt" }),
              call("listFiles", { path: "/" }),
              call("searchFiles", { path: "/", pattern: "first" }),
            ]
          : !names.has("editFile")
            ? [
                call("editFile", {
                  path: "/workspace.txt",
                  inode: original.inode,
                  version: original.version,
                  offset: "0",
                  delete_bytes: "5",
                  insert: "other",
                }),
                {
                  ...call("editFile", {
                    path: "/workspace.txt",
                    inode: original.inode,
                    version: original.version,
                    offset: "0",
                    delete_bytes: "5",
                    insert: "wrong",
                  }),
                  toolCallId: "call-stale-edit",
                },
                call("renameFile", {
                  path: "/workspace.txt",
                  destination: "/result.txt",
                }),
              ]
            : !names.has("deleteFile")
              ? [
                  {
                    ...call("readFile", { path: "/result.txt" }),
                    toolCallId: "call-read-renamed",
                  },
                  call("deleteFile", { path: "/result.txt" }),
                ]
              : [
                  {
                    type: "text",
                    text: "Read, listed, searched, edited, renamed and deleted the cell file. The stale edit was refused.",
                  },
                ];
      result.finishReason = {
        unified: names.has("deleteFile") ? "stop" : "tool-calls",
      };
      return result;
    }
    result.content = !names.size
      ? [
          call("calculate", { operation: "multiply", a: 24, b: 7 }),
          call("lookup", { topic: "durability" }),
          call("increment", { amount: 13 }),
          call("saveNote", {
            title: "Recovery",
            content:
              "Model decisions and local tool receipts are saved atomically.",
          }),
          call("createTicket", {
            title: "Verify recovery",
            priority: "normal",
          }),
        ]
      : !names.has("readState")
        ? [call("readState", {})]
        : [
            {
              type: "text",
              text: "## Saved results\n\n24 × 7 is **168**. The counter is **13**. The note and one ticket are saved.\n\n| Tool | Result |\n| --- | --- |\n| Counter | 13 |\n| Calculation | 168 |",
            },
          ];
    result.finishReason = {
      unified: names.has("readState") ? "stop" : "tool-calls",
    };
    return result;
  };
  return model;
}
