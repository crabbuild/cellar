import { DatabaseSync } from "node:sqlite";
import { createHash } from "node:crypto";

// An independent downstream service, deliberately outside Cellar transactions.
// Its idempotency record and effect live in the same SQLite write transaction.
export function ticketStore(path) {
  const db = new DatabaseSync(path);
  db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
    CREATE TABLE IF NOT EXISTS tickets (operation TEXT PRIMARY KEY, request TEXT NOT NULL, result TEXT NOT NULL, requests INTEGER NOT NULL)`);
  return {
    create(operation, value) {
      if (
        !/^[a-f0-9]{64}$/.test(operation ?? "") ||
        !value ||
        typeof value.title !== "string" ||
        !value.title.trim() ||
        value.title.length > 120 ||
        !["low", "normal", "high"].includes(value.priority) ||
        Object.keys(value).some((key) => !["title", "priority"].includes(key))
      )
        return { status: 400, result: { error: "Invalid ticket" } };
      const request = JSON.stringify({
        title: value.title,
        priority: value.priority,
      });
      db.exec("BEGIN IMMEDIATE");
      try {
        const prior = db
          .prepare("SELECT request, result FROM tickets WHERE operation = ?")
          .get(operation);
        if (prior && prior.request !== request) {
          db.exec("ROLLBACK");
          return { status: 409, result: { error: "Idempotency key conflict" } };
        }
        const result = prior
          ? JSON.parse(prior.result)
          : {
              id: `ticket-${createHash("sha256").update(operation).digest("hex").slice(0, 16)}`,
              title: value.title,
              priority: value.priority,
            };
        if (prior)
          db.prepare(
            "UPDATE tickets SET requests = requests + 1 WHERE operation = ?",
          ).run(operation);
        else
          db.prepare("INSERT INTO tickets VALUES (?, ?, ?, 1)").run(
            operation,
            request,
            JSON.stringify(result),
          );
        db.exec("COMMIT");
        return { status: prior ? 200 : 201, result };
      } catch (error) {
        db.exec("ROLLBACK");
        throw error;
      }
    },
    stats: () => ({
      ...db
        .prepare(
          "SELECT COUNT(*) AS effects, COALESCE(SUM(requests), 0) AS requests FROM tickets",
        )
        .get(),
    }),
    close: () => db.close(),
  };
}
