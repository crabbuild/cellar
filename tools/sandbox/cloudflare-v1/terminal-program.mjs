// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Passed as one literal `node -e` argument. No application text is interpolated
// into shell source; the real image supplies Node, stty and its Linux terminal.
export const terminalProgram = String.raw`
const readline = require("node:readline");
const { execFileSync } = require("node:child_process");
const input = readline.createInterface({ input: process.stdin, terminal: false });

function emit(stage) {
  const [rows, cols] = execFileSync("stty", ["size"], {
    stdio: ["inherit", "pipe", "inherit"],
  }).toString().trim().split(/\s+/).map(Number);
  const observed = {
    stage,
    pid: process.pid,
    tty: !!process.stdin.isTTY && !!process.stdout.isTTY,
    rows,
    cols,
  };
  process.stdout.write("cellar-pty:" + JSON.stringify(observed) + "\n");
}

process.stderr.write("pty-stderr\n");
emit("initial");
input.on("line", (line) => {
  emit(line);
  if (line === "done") {
    input.close();
    process.stdin.pause();
  }
});
`.trim();
