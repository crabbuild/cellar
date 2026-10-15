import { test, expect, type Page } from "@playwright/test"

const resource = {
  tenant: "team",
  application: "orders",
  id: "a".repeat(32),
  name: "order-events",
  family: "order.changed",
}
const revision = {
  source: "Order:one",
  incarnation: "b".repeat(32),
  lane: "c".repeat(64),
  dispatch_generation: "7",
  next_sequence: "9007199254740993",
  settled_through: "1",
}
const identity = {
  resource: resource.id,
  name: "projection",
  id: "d".repeat(32),
  generation: "e".repeat(32),
}
const subscription = {
  identity,
  state: "active",
  required: true,
  checkpoint: "1",
  earliest: "1",
  end: "9007199254740994",
  retry_at_ms: "0",
  lease: null,
  observation: {
    observed_ms: String(Date.now()),
    lag_records: "9007199254740992",
    retained_lag_records: "9007199254740992",
    oldest_retained_age_ms: "120000",
    history_expired: false,
    lease_active: false,
  },
}
const lane = {
  revision,
  resource,
  status: "blocked",
  pending_rows: "9007199254740993",
  pending_bytes: "18014398509481986",
  retry_at_ms: "0",
  attempts: "2",
  last_error: "Grant revoked",
  seal_state: null,
  blocked_reason: "Forbidden",
  oldest_pending_age_ms: "120000",
}
const app = (named = false) => ({
  name: "orders",
  revision: "r1",
  version: "v1",
  primary: true,
  crons: [],
  classes: ["Order"],
  bindings: [
    {
      name: "EVENTS",
      kind: "event_stream",
      identity: named ? "" : resource.id,
      scope: named ? null : "__EventStream:one",
      schedules: [],
      event: {
        resource: { ...resource, id: named ? null : resource.id },
        read: true,
        administer: true,
        publish: true,
        subscriptions: ["projection"],
      },
    },
  ],
})
const pageOf = (items: unknown[]) => ({
  items,
  next_cursor: null,
  coverage: "complete",
  warnings: [],
  observed_at: Date.now(),
})
const cell = {
  scope: "Order:one",
  class: "Order",
  kind: "application",
  owner: "node-one",
  epoch: 1,
  observed_at: Date.now(),
  error: null,
}
const observation = {
  observed_ms: String(Date.now()),
  retention_frontier: "0",
  protected_records: "1",
  protected_bytes: "128",
  retention_blocked_bytes: "0",
  active_leases: "0",
  expired_subscriptions: "0",
  oldest_retained_age_ms: "120000",
  sqlite_allocated_bytes: "4096",
  sqlite_used_bytes: "2048",
}
const node = {
  id: "node-one",
  generation: "gen-1",
  address: "127.0.0.1:9876",
  protocol: 1,
  lease_current: true,
  lease_expires_at: Date.now() + 60000,
  load: {
    sampled_ms: Date.now(),
    resident_cells: 1,
    owned_cells: 1,
    rss_bytes: 2048,
  },
}

type Action = { kind: string; [key: string]: unknown }
async function setup(
  page: Page,
  options: {
    named?: boolean
    grants?: string[]
    unknown?: boolean
    lost?: boolean
    sealOnly?: boolean
  } = {}
) {
  const requests: {
    path: string
    body: { operation_id: string; resource_id?: string; action: Action }
  }[] = []
  await page.route("**/api/v1/**", async (route) => {
    const url = new URL(route.request().url())
    const path = decodeURIComponent(url.pathname)
    let result: unknown
    if (path.endsWith("/session"))
      result = {
        user: "builder",
        fleets: [{ id: "main", name: "Events preview", environment: "Test" }],
        grants: {
          main: options.grants ?? [
            "metadata_read",
            "data_read",
            "data_write",
            "operate",
          ],
        },
      }
    else if (path.endsWith("/applications"))
      result = pageOf([app(options.named)])
    else if (path.endsWith("/applications/orders")) result = app(options.named)
    else if (path.endsWith("/resources"))
      result = { records: [{ resource, state: "active" }], cursor: null }
    else if (path.endsWith("/event-metrics"))
      result = {
        observed_at: Date.now(),
        generation: "gen-1",
        metrics: {
          active_waiters: "1",
          inflight_dispatches: "2",
          counters: [
            {
              name: "cellar_event_appended_records_total",
              value: "18446744073709551615",
            },
          ],
          histograms: [
            {
              name: "cellar_event_ack_duration_seconds",
              count: "5",
              sum_ms: "42",
              buckets: [
                { upper_ms: 10, count: "4" },
                { upper_ms: null, count: "5" },
              ],
            },
          ],
        },
      }
    else if (path.endsWith("/nodes/node-one")) result = node
    else if (path.endsWith("/cells")) result = pageOf([cell])
    else if (path.endsWith("/cells/Order:one")) result = cell
    else if (path.endsWith("/event-state"))
      result = {
        format: 1,
        scope: cell.scope,
        sampled_ms: String(Date.now()),
        gauges: Object.fromEntries(
          [
            "sources",
            "outbox_rows",
            "outbox_bytes",
            "outbox_oldest_age_ms",
            "streams",
            "retired_streams",
            "stream_rows",
            "stream_bytes",
            "protected_records",
            "protected_bytes",
            "retention_blocked_bytes",
            "active_leases",
            "subscriptions",
            "expired_subscriptions",
            "subscription_lag_records",
            "subscription_retained_lag_records",
            "subscription_oldest_age_ms",
          ].map((key) => [key, "0"])
        ),
      }
    else if (path.endsWith("/action") || path.endsWith("/outbox")) {
      const body = route.request().postDataJSON()
      requests.push({ path, body })
      const action = body.action
      if (action.kind === "inspect")
        result = {
          result: {
            incarnation: revision.incarnation,
            pending_rows: options.sealOnly ? "0" : lane.pending_rows,
            pending_bytes: options.sealOnly ? "0" : lane.pending_bytes,
            lanes: [
              options.sealOnly
                ? {
                    ...lane,
                    pending_rows: "0",
                    pending_bytes: "0",
                    oldest_pending_age_ms: null,
                    status: "sealed",
                    seal_state: "blocked",
                  }
                : lane,
            ],
            next: action.cursor ? null : "lane-page-2",
            observation: {
              ...observation,
              oldest_pending_age_ms: options.sealOnly ? null : "120000",
              blocked_lanes: [{ reason: "Forbidden", lanes: "1" }],
            },
          },
        }
      else if (action.kind === "describe")
        result = {
          result: {
            resource,
            revision: "1",
            earliest: "1",
            end: "2",
            rows: "1",
            bytes: "128",
            observation,
          },
        }
      else if (action.kind === "subscriptions")
        result = {
          result: {
            resource: resource.id,
            records: [subscription],
            cursor: null,
          },
        }
      else if (action.kind === "sources")
        result = {
          result: {
            resource: resource.id,
            records: [{ ...revision, accepted_through: "1", sealed: false }],
            cursor: null,
          },
        }
      else if (action.kind === "read")
        result = {
          result: {
            resource: resource.id,
            records: [
              {
                offset: "1",
                event: {
                  family: resource.family,
                  source: cell.scope,
                  id: "event-1",
                  key: "order-1",
                  ...(action.payload ? { payload: { amount: 42 } } : {}),
                },
              },
            ],
            earliest: "1",
            end: "2",
            next: "2",
            cursor: "v1.cursor",
          },
        }
      else if (action.kind === "audit")
        result = {
          result: {
            records: [
              {
                sequence: "1",
                receipt: { action: "pause", reason: "maintenance" },
              },
            ],
            next: "1",
            has_more: false,
          },
        }
      else if (action.kind === "retry" || action.kind === "change") {
        if (options.lost) {
          await route.abort("connectionreset")
          return
        }
        result = {
          result: options.unknown
            ? undefined
            : { operation: body.operation_id, action },
          failure: options.unknown ? "Reply lost after dispatch" : undefined,
          operation: {
            id: body.operation_id,
            status: options.unknown ? "outcome_unknown" : "succeeded",
          },
        }
      } else if (action.kind === "status")
        result = {
          result: {
            operation: action.operation,
            state: "confirmed",
            operator: "dashboard-builder",
          },
        }
      else result = { result: {} }
    } else result = pageOf([])
    await route.fulfill({ json: result })
  })
  await page.goto("/")
  await page.getByLabel("Dashboard access token").fill("fixture-token")
  await page.getByRole("button", { name: "Connect to fleet console" }).click()
  return requests
}

async function open(page: Page, path: string) {
  await page.goto(path)
  await page.getByLabel("Dashboard access token").fill("fixture-token")
  await page.getByRole("button", { name: "Connect to fleet console" }).click()
}

test("event navigation resolves named bindings and opens subscriptions", async ({
  page,
}) => {
  await setup(page, { named: true })
  await page.getByRole("link", { name: "Subscriptions", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await expect(
    page.getByText("Resolve order-events", { exact: true })
  ).toBeVisible()
  await page
    .getByRole("button", { name: "Inspect stream", exact: true })
    .click()
  await expect(
    page.getByRole("tab", { name: "Subscriptions" })
  ).toHaveAttribute("data-state", "active")
  await expect(
    page.getByRole("button", { name: "projection", exact: true })
  ).toBeVisible()
  await expect(
    page.getByText("9,007,199,254,740,992 / 9,007,199,254,740,992")
  ).toBeVisible()
})

test("outbox retry pins the inspected revision and can reconcile an unknown result", async ({
  page,
}) => {
  const calls = await setup(page, { unknown: true })
  await open(page, "/fleets/main/cells/Order%3Aone?tab=outbox")
  await expect(
    page.getByText("9,007,199,254,740,993", { exact: true }).first()
  ).toBeVisible()
  await expect(page.getByRole("button", { name: "Retry lane" })).toBeVisible()
  await page.screenshot({
    path: "../.cellar/event-console-outbox.png",
    fullPage: true,
  })
  await page.getByRole("button", { name: "Retry lane" }).click()
  await expect(
    page.getByRole("button", { name: "Apply change" })
  ).toBeDisabled()
  await page
    .getByLabel("Reason", { exact: true })
    .fill("Restore destination permissions")
  await page.getByRole("button", { name: "Apply change" }).click()
  await expect(page.getByText("Outcome needs verification")).toBeVisible()
  const writes = calls.filter((call) => call.body.action.kind === "retry")
  expect(writes).toHaveLength(1)
  expect(writes[0].body.action.expected).toEqual(revision)
  expect(writes[0].body.operation_id).toMatch(/^[a-f0-9]{32}$/)
  await page.getByRole("button", { name: "Check receipt" }).click()
  await expect(
    page.getByText("The owning cell confirmed the durable receipt.")
  ).toBeVisible()
  expect(
    calls.filter((call) => call.body.action.kind === "retry")
  ).toHaveLength(1)
})

test("lost browser response retains the operation ID and never resends", async ({
  page,
}) => {
  const calls = await setup(page, { lost: true })
  await open(page, "/fleets/main/cells/Order%3Aone?tab=outbox")
  await page.getByRole("button", { name: "Retry lane" }).click()
  await page.getByLabel("Reason", { exact: true }).fill("Check a blocked lane")
  await page.getByRole("button", { name: "Apply change" }).click()
  await expect(page.getByText("Outcome needs verification")).toBeVisible()
  await page.getByRole("button", { name: "Check receipt" }).click()
  await expect(
    page.getByText("The owning cell confirmed the durable receipt.")
  ).toBeVisible()
  const write = calls.find((call) => call.body.action.kind === "retry")!
  expect(
    calls.find((call) => call.body.action.kind === "status")!.body.action
      .operation
  ).toBe(write.body.operation_id)
  expect(
    calls.filter((call) => call.body.action.kind === "retry")
  ).toHaveLength(1)
})

test("event payloads require an explicit request and browsing is non-consuming", async ({
  page,
}) => {
  const calls = await setup(page)
  await open(page, "/fleets/main/streams/orders/EVENTS?view=events")
  await page.getByRole("button", { name: "Inspect event" }).click()
  await expect(
    page.getByText("Metadata only. Request payloads", { exact: false })
  ).toBeVisible()
  await expect(page.getByText('"amount"', { exact: false })).toHaveCount(0)
  await page.keyboard.press("Escape")
  expect(
    calls
      .filter((call) => call.body.action.kind === "read")
      .every((call) => call.body.action.payload === false)
  ).toBeTruthy()
  await page
    .getByRole("button", { name: "Include payloads for this page" })
    .click()
  await page.getByRole("button", { name: "Inspect event" }).click()
  await expect(page.getByText('"amount"', { exact: false })).toBeVisible()
  const reads = calls.filter((call) => call.body.action.kind === "read")
  expect(reads.at(-1)?.body.action.payload).toBe(true)
  expect(
    reads.every(
      (call) =>
        call.body.action.limit === 25 && call.body.action.max_bytes === 262144
    )
  ).toBeTruthy()
  expect(
    calls.some((call) =>
      ["consume", "ack", "publish"].includes(call.body.action.kind)
    )
  ).toBeFalsy()
})

test("subscription reset validates exact offsets and pins generation", async ({
  page,
}) => {
  const calls = await setup(page)
  await open(page, "/fleets/main/streams/orders/EVENTS?view=subscriptions")
  await expect(
    page.getByRole("combobox", { name: "Manage projection" })
  ).toBeVisible()
  await page.screenshot({
    path: "../.cellar/event-console-subscriptions.png",
    fullPage: true,
  })
  await page.getByRole("combobox", { name: "Manage projection" }).click()
  await page.getByRole("option", { name: "Reset checkpoint…" }).click()
  await page.getByLabel("Next event offset").fill("9223372036854775808")
  await page
    .getByLabel("Reason", { exact: true })
    .fill("Replay retained events")
  await expect(
    page.getByRole("button", { name: "Apply change" })
  ).toBeDisabled()
  await page.getByLabel("Next event offset").fill("9007199254740993")
  await page.getByRole("button", { name: "Apply change" }).click()
  await expect(page.getByText("Change confirmed")).toBeVisible()
  const write = calls.find((call) => call.body.action.kind === "change")!
  expect(write.body.resource_id).toBe(resource.id)
  expect(write.body.action.expected).toEqual(identity)
  expect(write.body.action.action).toEqual({
    kind: "reset",
    cursor: { resource: resource.id, partition: 0, next: "9007199254740993" },
  })
})

test("read-only inspectors cannot change subscriptions or retry lanes", async ({
  page,
}) => {
  const calls = await setup(page, { grants: ["metadata_read", "data_read"] })
  await open(page, "/fleets/main/streams/orders/EVENTS?view=subscriptions")
  await expect(
    page.getByRole("combobox", { name: "Manage projection" })
  ).toBeDisabled()
  await open(page, "/fleets/main/cells/Order%3Aone?tab=outbox")
  await expect(page.getByRole("button", { name: "Retry lane" })).toBeDisabled()
  expect(
    calls.some((call) => ["retry", "change"].includes(call.body.action.kind))
  ).toBeFalsy()
})

test("node metrics pin generation and preserve u64 precision", async ({
  page,
}) => {
  await setup(page)
  const observed = page.waitForRequest((request) =>
    request.url().includes("event-metrics?generation=gen-1")
  )
  await open(page, "/fleets/main/nodes/node-one?tab=events")
  await observed
  await expect(
    page.getByText("18,446,744,073,709,551,615", { exact: true })
  ).toBeVisible()
  await expect(
    page.getByText("Host-lifetime counters reset", { exact: false })
  ).toBeVisible()
})

test("outbox pages stay bounded and mobile layouts do not overflow", async ({
  page,
}) => {
  const calls = await setup(page)
  await page.setViewportSize({ width: 390, height: 844 })
  await open(page, "/fleets/main/cells/Order%3Aone?tab=outbox")
  await page.getByRole("button", { name: "Next lanes" }).click()
  await expect
    .poll(
      () =>
        calls.filter((call) => call.body.action.kind === "inspect").at(-1)?.body
          .action.cursor
    )
    .toBe("lane-page-2")
  expect(
    calls
      .filter((call) => call.body.action.kind === "inspect")
      .every((call) => call.body.action.limit === 25)
  ).toBeTruthy()
  await expect(page.getByRole("button", { name: "Retry lane" })).toBeVisible()
  await page.screenshot({
    path: "../.cellar/event-console-mobile.png",
    fullPage: true,
  })
  const overflow = await page.evaluate(
    () => document.documentElement.scrollWidth > window.innerWidth
  )
  expect(overflow).toBeFalsy()
})

test("stream workspace remains usable in dark mode on narrow screens", async ({
  page,
}) => {
  await setup(page)
  await open(page, "/fleets/main/streams/orders/EVENTS?view=subscriptions")
  await page.getByRole("radio", { name: "Dark theme" }).click()
  await page.setViewportSize({ width: 390, height: 844 })
  await expect(
    page.getByRole("combobox", { name: "Manage projection" })
  ).toBeVisible()
  await expect(page.locator("html")).toHaveClass(/dark/)
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth > innerWidth)
  ).toBeFalsy()
  await page.screenshot({
    path: "../.cellar/event-console-dark-mobile.png",
    fullPage: true,
  })
})

test("seal-only outbox work can be retried without pending event rows", async ({
  page,
}) => {
  const calls = await setup(page, { sealOnly: true })
  await open(page, "/fleets/main/cells/Order%3Aone?tab=outbox")
  await expect(page.getByRole("button", { name: "Retry lane" })).toBeEnabled()
  await page.getByRole("button", { name: "Retry lane" }).click()
  await expect(
    page.getByText("Seal delivery is blocked; this retry includes the seal.")
  ).toBeVisible()
  await page
    .getByLabel("Reason", { exact: true })
    .fill("Retry seal delivery after restoring grant")
  await page.getByRole("button", { name: "Apply change" }).click()
  await expect(page.getByText("Change confirmed")).toBeVisible()
  expect(
    calls.filter((call) => call.body.action.kind === "retry")
  ).toHaveLength(1)
})
