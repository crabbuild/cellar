// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

const element = id => document.getElementById(id);
const endpoint = element("endpoint");
const socketStatus = element("socket-status");
const sseStatus = element("sse-status");
const time = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  fractionalSecondDigits: 3,
  hour12: false,
});
let socket = null;
let feed = null;
let countRequest = null;

function status(node, message, active = false) {
  node.textContent = message;
  node.dataset.active = String(active);
}

function log(channel, label, payload, kind = "received") {
  const list = element(`${channel}-log`);
  const entry = document.createElement("li");
  entry.className = "log-entry";
  entry.dataset.kind = kind;

  const meta = document.createElement("div");
  meta.className = "entry-meta";
  const title = document.createElement("span");
  title.className = "entry-label";
  title.textContent = label;
  const timestamp = document.createElement("time");
  const now = new Date();
  timestamp.dateTime = now.toISOString();
  timestamp.textContent = time.format(now);
  meta.append(title, timestamp);

  // Incoming data is text, never HTML. Bound the log so a long session does not
  // keep an unlimited number of received messages in the DOM.
  const body = document.createElement("pre");
  body.className = "entry-payload";
  body.textContent = payload;
  entry.append(meta, body);
  list.append(entry);

  while (list.children.length > 100) {
    list.firstElementChild.remove();
  }

  element(`${channel}-empty`).hidden = true;
  list.parentElement.scrollTop = list.parentElement.scrollHeight;
}

function socketControls() {
  const active = socket !== null && socket.readyState !== WebSocket.CLOSED;
  const open = socket?.readyState === WebSocket.OPEN;
  element("connect").disabled = active;
  element("disconnect").disabled = !active;
  element("send").disabled = !open;
  element("binary").disabled = !open;
  endpoint.disabled = active;
}

async function refreshCount() {
  // Prefer the newest read if several echoes arrive close together.
  countRequest?.abort();
  const request = new AbortController();
  countRequest = request;

  try {
    const response = await fetch("/rooms/demo/state", {
      signal: request.signal,
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const state = await response.json();
    element("room-count").textContent = state.messages;
    element("count-error").hidden = true;
  } catch (error) {
    if (error.name !== "AbortError") {
      element("count-error").textContent =
        `Could not read the room count: ${error.message}. Try Refresh count.`;
      element("count-error").hidden = false;
    }
  }
}

element("connect").addEventListener("click", () => {
  const path = endpoint.value;
  const url = new URL(path, location.href);
  url.protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const connection = new WebSocket(url);
  connection.binaryType = "arraybuffer";
  socket = connection;
  status(socketStatus, "Connecting…");
  socketControls();

  connection.addEventListener("open", () => {
    if (socket !== connection) {
      return;
    }

    status(socketStatus, "Connected", true);
    socketControls();
    log("socket", "CONNECTED", path, "connection");
    element("message").focus();
  });

  connection.addEventListener("message", event => {
    const binary = typeof event.data !== "string";
    const payload = binary
      ? JSON.stringify(Array.from(new Uint8Array(event.data)))
      : event.data;
    log("socket", binary ? "← RECEIVED BINARY" : "← RECEIVED TEXT", payload);

    if (path.startsWith("/rooms/")) {
      void refreshCount();
    }
  });

  connection.addEventListener("error", () => {
    log(
      "socket",
      "CONNECTION ERROR",
      "Check that the example server is running.",
      "connection",
    );
  });

  connection.addEventListener("close", event => {
    log(
      "socket",
      "DISCONNECTED",
      `Code ${event.code}${event.reason ? ` · ${event.reason}` : ""}`,
      "connection",
    );

    if (socket === connection) {
      socket = null;
      status(socketStatus, "Disconnected");
      socketControls();
    }
  });
});

element("disconnect").addEventListener("click", () => {
  socket?.close(1000, "Closed from the browser");
  status(socketStatus, "Closing…");
  socketControls();
});

function send(data) {
  if (socket?.readyState !== WebSocket.OPEN) {
    return;
  }

  socket.send(data);
  const binary = typeof data !== "string";
  log(
    "socket",
    binary ? "→ SENT BINARY" : "→ SENT TEXT",
    binary ? JSON.stringify(Array.from(data)) : data,
    "sent",
  );
}

element("message-form").addEventListener("submit", event => {
  event.preventDefault();
  send(element("message").value);
});

element("binary").addEventListener("click", () => {
  send(new Uint8Array([0, 128, 255]));
});

element("refresh-count").addEventListener("click", refreshCount);

function finishFeed(message) {
  feed?.close();
  feed = null;
  status(sseStatus, message);
  element("start-feed").disabled = false;
  element("stop-feed").disabled = true;
}

element("start-feed").addEventListener("click", () => {
  // EventSource parses SSE record boundaries even when HTTP chunks split or
  // combine records. Named events need addEventListener, not just onmessage.
  const source = new EventSource("/events");
  feed = source;
  status(sseStatus, "Connecting…");
  element("start-feed").disabled = true;
  element("stop-feed").disabled = false;

  source.addEventListener("open", () => {
    if (feed !== source) {
      return;
    }

    status(sseStatus, "Receiving", true);
  });

  source.addEventListener("progress", event => {
    if (feed !== source) {
      return;
    }

    log("sse", `PROGRESS · ID ${event.lastEventId}`, event.data);
  });

  source.addEventListener("done", event => {
    if (feed !== source) {
      return;
    }

    log("sse", "DONE", event.data);
    // This sample is finite. Close explicitly to prevent EventSource from
    // reconnecting after EOF and replaying the same three progress events.
    finishFeed("Complete");
  });

  source.addEventListener("error", () => {
    if (feed !== source) {
      return;
    }

    log(
      "sse",
      "FEED INTERRUPTED",
      "Check the server, then start the feed again.",
      "connection",
    );
    finishFeed("Interrupted");
  });
});

element("stop-feed").addEventListener("click", () => {
  finishFeed("Stopped");
});

for (const channel of ["socket", "sse"]) {
  element(`clear-${channel}`).addEventListener("click", () => {
    element(`${channel}-log`).replaceChildren();
    element(`${channel}-empty`).hidden = false;
  });
}

// Navigating away should release both native connections rather than leave a
// client attempting to reconnect or an unused socket waiting for more frames.
window.addEventListener("pagehide", () => {
  socket?.close(1000, "Page closed");
  socket = null;
  status(socketStatus, "Disconnected");
  socketControls();
  finishFeed("Stopped");
  countRequest?.abort();
});

void refreshCount();
