import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { access, readFile, readdir, rm } from "node:fs/promises";
import { join, relative } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { promisify } from "node:util";
import { storeEnvironment } from "../script/store.mjs";

const run = promisify(execFile);

// Inspect only application rows, without opening a writer or reading the
// model journal's private replay metadata. These observations never feed the
// replacement worker: it must recover its own database from the bucket.
export async function workerState(directory, { filesystem = false } = {}) {
  const cells = [];
  async function visit(path) {
    for (const item of await readdir(path, { withFileTypes: true })) {
      if (item.name.startsWith(".")) continue;
      const file = join(path, item.name);
      if (item.isDirectory()) await visit(file);
      else if (item.name === "db.sqlite") {
        if (!relative(directory, file).startsWith("DurableAgent:")) continue;
        const db = new DatabaseSync(file, { readOnly: true });
        try {
          const rows = db
            .prepare(
              "SELECT k, v FROM __sys_cell_kv WHERE k = 'count' OR k LIKE 'note:%' ORDER BY k",
            )
            .all();
          const files = filesystem
            ? db
                .prepare(
                  `
            SELECT hex(e.name) AS name_hex, CAST(i.id AS TEXT) AS inode,
              CAST(i.version AS TEXT) AS version, CAST(i.size AS TEXT) AS size,
              (SELECT substr(data, 1, min(i.size, 4096)) FROM __sys_fs_chunks
                WHERE inode=i.id AND chunk_index=0) AS prefix
            FROM __sys_fs_entries e JOIN __sys_fs_inodes i ON i.id=e.inode
            WHERE e.parent=1 AND i.kind=0 ORDER BY e.name LIMIT 32
          `,
                )
                .all()
                .map((row) => ({
                  ...row,
                  prefix:
                    row.prefix === null
                      ? null
                      : Buffer.from(row.prefix).toString("base64"),
                }))
            : undefined;
          cells.push({
            cell: relative(directory, file).split(/[\\/]/)[0],
            count: JSON.parse(rows.find((row) => row.k === "count")?.v ?? "0"),
            notes: rows
              .filter((row) => row.k.startsWith("note:"))
              .map((row) => JSON.parse(row.v)),
            ...(filesystem ? { files } : {}),
          });
        } finally {
          db.close();
        }
      }
    }
  }
  await visit(directory);
  return cells.sort((a, b) => a.cell.localeCompare(b.cell));
}

// Every scenario owns a new bucket. Leave S3 buckets intact for inspection;
// only temporary worker files are removed by the process harness.
export async function recoveryStore(directory, scenario) {
  const remote = process.env.CELLAR_AI_STORE === "s3";
  const bucket = `cellar-agent-${scenario}-${randomUUID()}`;
  const env = storeEnvironment(directory, {
    ...process.env,
    ...(remote ? { CELLAR_BUCKET: bucket } : {}),
  });

  async function s3(operation, args = []) {
    try {
      const { stdout } = await run(
        process.env.CELLAR_AI_AWS ?? "aws",
        [
          "--endpoint-url",
          env.S3_ENDPOINT,
          "--region",
          env.AWS_REGION,
          "s3api",
          operation,
          "--bucket",
          bucket,
          "--output",
          "json",
          ...args,
        ],
        {
          env: { ...env, AWS_PAGER: "", AWS_EC2_METADATA_DISABLED: "true" },
          timeout: 30000,
          maxBuffer: 4 * 1024 * 1024,
        },
      );
      return stdout ? JSON.parse(stdout) : {};
    } catch {
      // CLI diagnostics can include credential-bearing environment details.
      throw new Error(
        `S3 ${operation} failed; check the private store configuration`,
      );
    }
  }

  if (remote) {
    await s3(
      "create-bucket",
      env.AWS_REGION === "us-east-1"
        ? []
        : [
            "--create-bucket-configuration",
            `LocationConstraint=${env.AWS_REGION}`,
          ],
    );
  }

  return {
    env,
    backend: remote ? "s3" : "local",
    async inspect() {
      if (!remote) return { backend: "local" };
      await assert.rejects(access(join(directory, "bucket.sqlite")), {
        code: "ENOENT",
      });
      const listing = await s3("list-objects-v2");
      const objects = (listing.Contents ?? []).map((object) => ({
        key: object.Key,
        bytes: object.Size,
        etag: object.ETag,
      }));
      const segments = objects.filter(
        (object) =>
          object.key.startsWith("cells/DurableAgent:") &&
          object.key.includes("/ltx/"),
      );
      assert.ok(
        segments.length > 0,
        "S3 bucket must contain real cell LTX objects",
      );
      const ownership = [];
      for (const object of objects.filter(
        (object) =>
          object.key.startsWith("cells/DurableAgent:") &&
          object.key.endsWith("/own.json"),
      )) {
        const file = join(directory, `ownership-${randomUUID()}.json`);
        try {
          await s3("get-object", ["--key", object.key, file]);
          const { node, epoch } = JSON.parse(await readFile(file, "utf8"));
          ownership.push({ key: object.key, node, epoch });
        } finally {
          await rm(file, { force: true });
        }
      }
      const sample = join(directory, `segment-${randomUUID()}.ltx`);
      let sha256;
      try {
        await s3("get-object", ["--key", segments[0].key, sample]);
        const bytes = await readFile(sample);
        assert.equal(bytes.length, segments[0].bytes);
        sha256 = createHash("sha256").update(bytes).digest("hex");
      } finally {
        await rm(sample, { force: true });
      }
      return {
        backend: "s3",
        endpoint: env.S3_ENDPOINT,
        bucket,
        localBucketAbsent: true,
        objects,
        ownership,
        downloadedSegment: { key: segments[0].key, sha256 },
      };
    },
  };
}
