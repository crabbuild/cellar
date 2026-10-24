// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

const $ = (id) => document.getElementById(id);
let history;
try {
  history = JSON.parse(localStorage.getItem("cellar-pi-tasks") ?? "[]");
} catch {
  history = [];
}
if (!Array.isArray(history)) history = [];
history = history
  .filter(
    (x) =>
      x && /^[a-zA-Z0-9_-]{1,64}$/.test(x.id) && typeof x.prompt === "string",
  )
  .slice(0, 20);
let current,
  stream,
  pollTimer,
  revision = 0,
  busy = false;
let activity = new Set(),
  liveDeltas = 0,
  readController;
function remember() {
  try {
    localStorage.setItem(
      "cellar-pi-tasks",
      JSON.stringify(history.slice(0, 20)),
    );
  } catch {
    /* A task URL still works without local storage. */
  }
  renderHistory();
}
function renderHistory() {
  $("history").replaceChildren();
  if (!history.length) {
    const p = document.createElement("p");
    p.className = "history-empty";
    p.textContent = "Your tasks will appear here.";
    $("history").append(p);
  }
  for (const item of history) {
    const button = document.createElement("button");
    button.className = item.id === current?.id ? "active" : "";
    const title = document.createElement("strong");
    title.textContent = item.prompt;
    const meta = document.createElement("small");
    meta.textContent = `${item.status ?? "pending"} · ${item.id.slice(-8)}`;
    button.append(title, meta);
    button.onclick = () => select(item);
    $("history").append(button);
  }
}
function notice(text = "") {
  $("notice").textContent = text;
  $("notice").hidden = !text;
}
function connection(text, online = true) {
  $("connection").textContent = text;
  $("connection-dot").classList.toggle("offline", !online);
}
function record(text) {
  if (activity.has(text)) return;
  activity.add(text);
  $("timeline").replaceChildren(
    ...[...activity].slice(-7).map((value) => {
      const li = document.createElement("li");
      li.textContent = value;
      return li;
    }),
  );
}
function setStatus(label, saved = false) {
  $("task-status").textContent = label;
  $("task-status").classList.toggle("saved", saved);
}
function disconnect() {
  revision++;
  readController?.abort();
  stream?.close();
  stream = undefined;
  clearTimeout(pollTimer);
}
function reset() {
  disconnect();
  current = undefined;
  busy = false;
  activity = new Set();
  liveDeltas = 0;
  $("empty").hidden = false;
  $("conversation-body").hidden = true;
  $("tool-receipt").hidden = true;
  $("task-form").hidden = false;
  $("prompt").disabled = false;
  $("run").disabled = false;
  $("prompt").value = "";
  $("task-title").textContent = "Start a new task";
  $("task-id").textContent = "—";
  $("attempt").textContent = "—";
  for (const id of ["count", "steps", "receipts", "deltas"])
    $(id).textContent = "0";
  $("count-caption").textContent = "No saved increment yet";
  $("answer").textContent = "";
  $("cursor").hidden = true;
  $("timeline").replaceChildren();
  record("Waiting for a task");
  setStatus("Ready");
  notice();
  window.history.replaceState(null, "", "/");
  renderHistory();
  connection("Ready");
}
function receipt(count, replayed = false, receipts = 1) {
  $("count").textContent = count;
  $("count-caption").textContent = "Increment committed";
  $("receipts").textContent = receipts;
  $("tool-receipt").hidden = false;
  $("tool-copy").textContent =
    `increment → ${count}${replayed ? " · restored receipt" : ""}`;
  record(
    replayed ? "Saved tool receipt reused" : "Tool effect and receipt saved",
  );
}
function progress(e) {
  if (!current || current.status === "completed" || current.status === "failed")
    return;
  if (e.type === "recovering") {
    $("answer").textContent = "";
    $("preview-label").textContent = "Recovering saved work";
    $("cursor").hidden = true;
    setStatus("Recovering");
    record("Worker interrupted · recovering");
    return;
  }
  if (e.attempt) $("attempt").textContent = e.attempt;
  if (e.type === "attempt") {
    $("answer").textContent = "";
    record(`Attempt ${e.attempt} started`);
  }
  if (e.type === "model") {
    $("answer").textContent = "";
    $("preview-label").textContent = "Waiting for the model";
    setStatus("Running");
    record(`Model response ${(e.step ?? 0) + 1} started`);
  }
  if (e.type === "text") {
    $("answer").textContent = e.text;
    $("preview-label").textContent = "Streaming · not saved yet";
    $("cursor").hidden = false;
    setStatus("Streaming");
  }
  if (e.type === "checkpoint") {
    $("steps").textContent = Math.max(
      Number($("steps").textContent),
      e.step + 1,
    );
    $("answer").textContent = e.text;
    $("preview-label").textContent = e.replayed
      ? "Saved response restored"
      : "Response checkpoint saved";
    $("cursor").hidden = true;
    record(`Model response ${e.step + 1} ${e.replayed ? "restored" : "saved"}`);
  }
  if (e.type === "tool") receipt(e.count, e.replayed);
  if (e.deltaEvents > liveDeltas) liveDeltas = e.deltaEvents;
  $("deltas").textContent = liveDeltas;
  if (e.type === "completed" || e.type === "failed") {
    $("cursor").hidden = true;
    $("preview-label").textContent = "Checking durable result…";
  }
}
async function reconcile(id, generation) {
  try {
    const reply = await fetch(`/api/tasks/${id}`, {
      signal: readController.signal,
    });
    if (generation !== revision) return;
    if (reply.ok) {
      const value = await reply.json();
      if (generation !== revision) return;
      current.prompt = value.prompt;
      $("prompt-copy").textContent = value.prompt;
      if (!history.some((item) => item.id === current.id)) {
        history.unshift(current);
        history = history.slice(0, 20);
      }
      current.status = value.status;
      $("attempt").textContent = value.attempts;
      $("steps").textContent = value.modelSteps;
      $("receipts").textContent = value.toolReceipts;
      $("count").textContent = value.count;
      if (value.toolReceipts) receipt(value.count, false, value.toolReceipts);
      if (value.status === "completed" || value.status === "failed") {
        const assistant = value.messages
          .filter((m) => m.role === "assistant")
          .at(-1);
        $("answer").textContent =
          assistant?.content
            .filter((p) => p.type === "text")
            .map((p) => p.text)
            .join("\n") ?? "";
        $("preview-label").textContent =
          value.status === "completed"
            ? "Final answer · saved"
            : "Task stopped";
        $("cursor").hidden = true;
        setStatus(
          value.status === "completed" ? "Completed" : "Failed",
          value.status === "completed",
        );
        record(
          value.status === "completed"
            ? "Final transcript saved"
            : "Retry limit reached",
        );
        if (value.status === "failed")
          notice(
            "This task exhausted its attempts. Start a new task to try again.",
          );
        connection("Saved result");
        remember();
        stream?.close();
        return;
      }
      notice();
      remember();
    } else if (reply.status !== 404)
      connection("Reconnecting to worker", false);
  } catch {
    if (generation === revision) connection("Reconnecting to worker", false);
  }
  if (generation === revision)
    pollTimer = setTimeout(() => reconcile(id, generation), 1200);
}
function select(item) {
  reset();
  current = item;
  activity.clear();
  $("timeline").replaceChildren();
  $("empty").hidden = true;
  $("conversation-body").hidden = false;
  $("task-form").hidden = true;
  $("task-title").textContent = "Task conversation";
  $("task-id").textContent = item.id;
  $("task-id").title = item.id;
  $("prompt-copy").textContent = item.prompt;
  $("answer").textContent = "";
  $("preview-label").textContent = "Loading saved state";
  setStatus("Connecting");
  window.history.replaceState(
    null,
    "",
    `/?task=${encodeURIComponent(item.id)}`,
  );
  renderHistory();
  // Always reload from Cellar, including a previously completed browser entry.
  current.status = "pending";
  stream = new EventSource(`/api/tasks/${item.id}/events`);
  stream.addEventListener("open", () => connection("Stream connected"));
  stream.addEventListener("progress", (event) => {
    try {
      progress(JSON.parse(event.data));
    } catch {
      notice("Could not read a preview. The saved result will still load.");
    }
  });
  stream.onerror = () => {
    connection("Stream reconnecting", false);
    $("preview-label").textContent =
      "Connection interrupted · restoring saved state";
    $("answer").textContent = "";
    $("cursor").hidden = true;
  };
  readController = new AbortController();
  const generation = revision;
  void reconcile(item.id, generation);
}
$("task-form").onsubmit = async (event) => {
  event.preventDefault();
  if (busy) return;
  const prompt = $("prompt").value.trim();
  if (!prompt) return;
  busy = true;
  const item = { id: `task-${crypto.randomUUID()}`, prompt, status: "pending" };
  history.unshift(item);
  history = history.slice(0, 20);
  remember();
  select(item);
  record("Sending task");
  try {
    const reply = await fetch(`/api/tasks/${item.id}`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ prompt }),
    });
    if (!reply.ok) throw new Error("acceptance unavailable");
    if (current?.id === item.id && current.status === "pending") {
      record("Task accepted and saved");
      setStatus("Running");
    }
  } catch {
    if (current?.id === item.id) {
      notice(
        "Acceptance was interrupted. Retry this same task; it will not create a second increment.",
      );
      const retry = document.createElement("button");
      retry.className = "secondary";
      retry.textContent = "Retry same task";
      retry.onclick = async () => {
        retry.disabled = true;
        try {
          const reply = await fetch(`/api/tasks/${item.id}`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ prompt }),
          });
          if (!reply.ok) throw new Error();
          notice();
        } catch {
          retry.disabled = false;
        }
      };
      $("notice").append(retry);
    }
  } finally {
    busy = false;
  }
};
$("toggle-tasks").onclick = () => {
  const open = $("task-sidebar").classList.toggle("mobile-open");
  $("toggle-tasks").setAttribute("aria-expanded", String(open));
};
$("new-task").onclick = $("new-task-top").onclick = () => {
  reset();
  $("prompt").focus();
};
$("example").onclick = () => {
  $("prompt").value =
    "Add 7 to the counter, then explain in a few sentences what changed and what the final value is.";
  $("prompt").focus();
};
$("restart").onclick = async () => {
  $("restart").disabled = true;
  connection("Restarting worker", false);
  record("Worker restart requested");
  try {
    const reply = await fetch("/api/restart", { method: "POST" });
    if (!reply.ok) throw new Error();
    connection("Worker restarted");
    record("Replacement worker ready");
  } catch {
    notice(
      "Worker restart failed. Check the local demo process, then reconnect.",
    );
  } finally {
    $("restart").disabled = false;
  }
};
async function start() {
  const task = new URLSearchParams(location.search).get("task");
  reset();
  try {
    const config = await (await fetch("/api/config")).json();
    $("provider").textContent = `${config.provider} / ${config.model}`;
    $("mode-label").textContent =
      config.provider === "fixture"
        ? "Deterministic fixture"
        : "Live model stream";
  } catch {
    connection("Demo unavailable", false);
  }
  if (task && /^[a-zA-Z0-9_-]{1,64}$/.test(task))
    select(
      history.find((x) => x.id === task) ?? {
        id: task,
        prompt: "Loading task…",
      },
    );
}
void start();
