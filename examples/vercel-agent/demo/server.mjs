import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import {join} from "node:path";
import {ticketStore} from "./tickets.mjs";

const idPattern = /^[a-zA-Z0-9_-]{1,64}$/;
const types = new Set([
  "attempt",
  "model",
  "text",
  "checkpoint",
  "tool",
  "completed",
  "failed",
]);
function asset(path) {
  if (path === '/') return ['index.html', 'text/html'];
  if (!/^\/assets\/[a-zA-Z0-9_.-]+\.(js|css|woff2?)$/.test(path)) return;
  const type = path.endsWith('.js') ? 'text/javascript' : path.endsWith('.css') ? 'text/css' : path.endsWith('.woff2') ? 'font/woff2' : 'font/woff';
  return [path.slice(1), type];
}
async function body(req, limit = 192 * 1024) {
  let bytes = 0;
  const chunks = [];
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > limit) throw new Error("request too large");
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString("utf8");
}
function json(res, status, value) {
  res.writeHead(status, {
    "content-type": "application/json",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(value));
}
export function createDemoServer({ backend, token, restart, model, provider, directory }) {
  const tickets = ticketStore(directory ? join(directory, "tickets.sqlite") : ":memory:");
  const tasks = new Map();
  let clients = 0;
  function task(id) {
    if (!tasks.has(id)) {
      if (tasks.size >= 128) {
        const victim = [...tasks].find(([, value]) => !value.clients.size);
        if (!victim) throw new Error("preview capacity reached");
        tasks.delete(victim[0]);
      }
      tasks.set(id, {
        clients: new Set(),
        state: { type: "waiting", attempt: 0, text: "", deltaEvents: 0 },
      });
    }
    return tasks.get(id);
  }
  function send(res, event, data) {
    // Disconnect slow readers instead of buffering unbounded preview text.
    if (res.writableLength > 256 * 1024) {
      res.destroy();
      return;
    }
    res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
  }
  function reset() {
    for (const item of tasks.values()) {
      item.state = { type: "recovering", attempt: 0, text: "", deltaEvents: 0 };
      for (const res of item.clients) send(res, "progress", item.state);
    }
  }
  const server = createServer(async (req, res) => {
    try {
      const origin = `http://127.0.0.1:${server.address().port}`;
      if (req.headers.host !== new URL(origin).host)
        return json(res, 403, { error: "Use the local demo URL" });
      if (req.headers.origin && req.headers.origin !== origin)
        return json(res, 403, { error: "Origin rejected" });
      const path = new URL(req.url, origin).pathname;
      if (path === "/internal/tickets" && req.method === "POST") {
        if (req.headers.authorization !== `Bearer ${token()}`) return json(res, 403, {error: "Tool service rejected"});
        const value = JSON.parse(await body(req, 4096));
        if (req.headers.authorization !== `Bearer ${token()}`) return json(res, 403, {error: "Worker replaced"});
        const result = tickets.create(req.headers['idempotency-key'], value);
        return json(res, result.status, result.result);
      }
      if (path === "/api/tickets" && req.method === "GET") return json(res, 200, tickets.stats());
      if (path === "/internal/events" && req.method === "POST") {
        if (req.headers.authorization !== `Bearer ${token()}`)
          return json(res, 403, { error: "Observer rejected" });
        const e = JSON.parse(await body(req));
        // A restart can rotate the token while the request body is arriving.
        if (req.headers.authorization !== `Bearer ${token()}`)
          return json(res, 403, { error: "Observer replaced" });
        if (
          !idPattern.test(e.task) ||
          !types.has(e.type) ||
          !Number.isInteger(e.attempt) ||
          e.attempt < 1 ||
          (e.text !== undefined &&
            (typeof e.text !== "string" || e.text.length > 32000)) ||
          (e.step !== undefined &&
            (!Number.isInteger(e.step) || e.step < 0 || e.step > 15)) ||
          (e.tool !== undefined && (typeof e.tool !== "string" || !["increment", "calculate", "lookup", "saveNote", "readState", "createTicket"].includes(e.tool))) ||
          (e.count !== undefined &&
            (!Number.isSafeInteger(e.count) || e.count < 0))
        )
          return json(res, 400, { error: "Invalid event" });
        const item = task(e.task);
        if (e.attempt < item.state.attempt)
          return json(res, 202, { ignored: true });
        if (e.attempt > item.state.attempt || e.type === "model")
          item.state = { ...item.state, text: "", tool: undefined };
        // Explicit field selection keeps private/internal data out of the stream.
        item.state = {
          ...item.state,
          type: e.type,
          attempt: e.attempt,
          ...(e.step !== undefined ? { step: e.step } : {}),
          ...(e.text !== undefined ? { text: e.text } : {}),
          ...(e.count !== undefined ? { count: e.count } : {}),
          ...(e.tool !== undefined ? {tool: e.tool} : {}),
          replayed: e.replayed === true,
          deltaEvents: item.state.deltaEvents + (e.type === "text" ? 1 : 0),
        };
        for (const client of item.clients) send(client, "progress", item.state);
        return json(res, 202, { accepted: true });
      }
      if (path === "/api/config" && req.method === "GET")
        return json(res, 200, { provider, model });
      if (path === "/api/restart" && req.method === "POST") {
        if (req.headers.origin !== origin)
          return json(res, 403, { error: "Origin required" });
        await restart(reset);
        return json(res, 200, { restarted: true });
      }
      const match = /^\/api\/tasks\/([a-zA-Z0-9_-]{1,64})(\/events)?$/.exec(
        path,
      );
      if (match?.[2] && req.method === "GET") {
        if (clients >= 64)
          return json(res, 503, { error: "Too many stream connections" });
        const item = task(match[1]);
        clients++;
        item.clients.add(res);
        res.writeHead(200, {
          "content-type": "text/event-stream",
          "cache-control": "no-cache, no-transform",
          connection: "keep-alive",
          "x-accel-buffering": "no",
        });
        send(res, "progress", item.state);
        const heartbeat = setInterval(() => {
          if (!res.write(": keepalive\n\n")) res.destroy();
        }, 15000);
        req.on("close", () => {
          clearInterval(heartbeat);
          item.clients.delete(res);
          clients--;
        });
        return;
      }
      if (match && !match[2] && ["GET", "POST"].includes(req.method)) {
        if (req.method === "POST" && req.headers.origin !== origin)
          return json(res, 403, { error: "Origin required" });
        const target = backend();
        if (!target)
          return json(res, 503, {
            error: "Worker is recovering. Reconnecting automatically.",
          });
        const response = await fetch(`${target}/agents/${match[1]}`, {
          method: req.method,
          ...(req.method === "POST" ? { body: await body(req, 40000) } : {}),
          signal: AbortSignal.timeout(65000),
        });
        res.writeHead(response.status, {
          "content-type": "application/json",
          "cache-control": "no-store",
        });
        res.end(await response.text());
        return;
      }
      const resource = asset(path);
      if (resource && req.method === "GET") {
        const [file, type] = resource;
        const data = await readFile(
          new URL(`../ui/dist/${file}`, import.meta.url),
        );
        res.writeHead(200, {
          "content-type": `${type}; charset=utf-8`,
          "cache-control": "no-store",
          "content-security-policy":
            "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; font-src 'self'; object-src 'none'; form-action 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'",
          "x-content-type-options": "nosniff",
        });
        res.end(data);
        return;
      }
      json(res, 404, { error: "Not found" });
    } catch {
      if (!res.headersSent)
        json(res, 503, {
          error:
            "Worker unavailable or request invalid. Retry with the same task ID.",
        });
      else res.end();
    }
  });
  return {
    server,
    close: async () => {
      for (const item of tasks.values())
        for (const res of item.clients) res.end();
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
      tickets.close();
    },
  };
}
