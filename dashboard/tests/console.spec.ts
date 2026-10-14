import { test, expect } from "@playwright/test"

const now = Date.now()
const pageOf = (items: unknown[]) => ({
  items,
  next_cursor: null,
  observed_at: now,
  coverage: "complete",
  warnings: [],
})
const apps = [
  {
    name: "commerce-api",
    version: "deploy-a46b293f",
    revision: "rev-1",
    primary: true,
    classes: ["Customer", "Order", "__SQLDatabase"],
    crons: ["*/5 * * * *"],
    bindings: [
      {
        name: "DATABASE",
        kind: "sql",
        identity: "commerce-data",
        scope: "__SQLDatabase:abc",
        schedules: [],
      },
      {
        name: "CACHE",
        kind: "kv",
        identity: "commerce-cache",
        scope: "__KV:def",
        schedules: [],
      },
      {
        name: "EMAILS",
        kind: "queue",
        identity: "email-delivery",
        scope: "__Queue:ghi",
        schedules: [],
      },
      {
        name: "CHECKOUT",
        kind: "workflow",
        identity: "checkout",
        scope: null,
        schedules: [],
      },
    ],
  },
]
const nodes = [
  "node-vancouver-01",
  "node-vancouver-02",
  "node-vancouver-03",
].map((id, i) => ({
  id,
  generation: `gen-${i}`,
  address: `10.0.1.${i + 1}:9000`,
  lease_current: true,
  lease_expires_at: now + 60000,
  protocol: 7,
  load: {
    sampled_ms: now,
    owned_cells: 148 + 32 * i,
    resident_cells: 24 + 5 * i,
    rss_bytes: 168000000 + 46000000 * i,
    in_use_bytes: null,
    cpu_percent_x100: 1200 + i * 200,
    pressured: false,
    restoring: 0,
    rebalance_paused: false,
    draining: false,
  },
}))

test.beforeEach(async ({ page }) => {
  await page.route("**/api/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname
    let data: unknown
    if (path === "/api/v1/session")
      data = {
        user: "builder",
        fleets: [
          {
            id: "main",
            name: "Commerce production",
            environment: "Production",
          },
        ],
        grants: {
          main: [
            "metadata_read",
            "data_read",
            "data_write",
            "operate",
            "configure",
          ],
        },
      }
    else if (path.endsWith("/nodes")) data = pageOf(nodes)
    else if (path.includes("/nodes/"))
      data = nodes.find((n) => path.endsWith(n.id))
    else if (path.endsWith("/applications")) data = pageOf(apps)
    else if (path.endsWith("/applications/commerce-api")) data = apps[0]
    else if (path.endsWith("/cells"))
      data = pageOf([
        {
          scope: "Customer:alpha",
          class: "Customer",
          kind: "application",
          owner: nodes[0].id,
          epoch: 18,
          observed_at: now,
          error: null,
        },
      ])
    else if (
      path.endsWith("/cells/Customer%3Aalpha") ||
      path.endsWith("/cells/Customer:alpha")
    )
      data = {
        scope: "Customer:alpha",
        class: "Customer",
        kind: "application",
        owner: nodes[0].id,
        epoch: 18,
        observed_at: now,
        error: null,
      }
    else if (path.endsWith("/operations"))
      data = { items: [], coverage: "latest_100" }
    else if (path.endsWith("/resources/action"))
      data = {
        result: { results: [{ columns: ["name"], rows: [["customers"]] }] },
        operation: { status: "succeeded" },
      }
    else
      return route.fulfill({
        status: 404,
        json: { error: `No test fixture for ${path}` },
      })
    await route.fulfill({ json: data })
  })
  await page.goto("/")
  await page
    .getByLabel("Dashboard access token")
    .fill("test-browser-only-token-for-fixture")
  await page.getByRole("button", { name: "Connect to fleet console" }).click()
  await expect(
    page.getByRole("heading", { name: "Fleet overview", exact: true })
  ).toBeVisible()
})

test("overview, cells and node deep links stay inside the selected fleet", async ({
  page,
}) => {
  await expect(
    page.getByRole("link", { name: /node-vancouver-01/ })
  ).toBeVisible()
  await page.getByRole("link", { name: "Cells", exact: true }).click()
  await page.getByRole("link", { name: /Customer:alpha/ }).click()
  await expect(page).toHaveURL(/\/fleets\/main\/cells\/Customer%3Aalpha/)
  await page
    .getByRole("link", { name: "node-vancouver-01", exact: true })
    .click()
  await expect(page).toHaveURL(/\/fleets\/main\/nodes\/node-vancouver-01/)
  await expect(
    page.getByRole("button", { name: "Run signed probe" })
  ).toBeVisible()
})

test("SQL workspace confirms the selected target before submitting", async ({
  page,
}) => {
  await page.getByRole("link", { name: "SQL databases", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Query mode").click()
  await page
    .getByRole("option", { name: "Read and write", exact: true })
    .click()
  await page.getByRole("button", { name: "Run statement" }).click()
  await expect(page.getByRole("alertdialog")).toContainText("Fleet: main")
  await expect(page.getByRole("alertdialog")).toContainText(
    "commerce-api / DATABASE"
  )
  await page.getByRole("button", { name: "Confirm operation" }).click()
  await expect(
    page.getByRole("cell", { name: "customers", exact: true })
  ).toBeVisible()
})

test("cell bindings separate matching namespaces from application resources and stay usable on narrow screens", async ({
  page,
}, testInfo) => {
  const namespaces = [
    "Customer",
    "Order",
    "Alarm",
    "Async",
    "Vector",
    "Room",
    "Supervisor",
    "Account",
    "Outbox",
  ]
  const application = {
    ...apps[0],
    bindings: [
      ...namespaces.map((identity) => ({
        name: identity === "Customer" ? "CUSTOMERS" : identity.toUpperCase(),
        kind: "durable_object_namespace",
        identity,
        scope: null,
        schedules: [],
      })),
      ...apps[0].bindings,
      {
        name: "BLOBS",
        kind: "blob_bucket",
        identity: "commerce-files",
        scope: null,
        schedules: [],
      },
      {
        name: "DEAD",
        kind: "queue",
        identity: "dead-letter",
        scope: "__Queue:dead",
        schedules: [],
      },
    ],
  }
  await page.route("**/api/v1/fleets/main/applications?*", (route) =>
    route.fulfill({ json: pageOf([application]) })
  )
  await page.getByRole("link", { name: "Cells", exact: true }).click()
  await page.getByRole("link", { name: /Customer:alpha/ }).click()
  const bindings = page.getByRole("region", {
    name: "commerce-api bindings",
    exact: true,
  })
  const matching = bindings.getByRole("region", {
    name: "Bindings for this cell",
    exact: true,
  })
  await expect(matching).toContainText("CUSTOMERS")
  await expect(matching).toContainText("Cell class")
  await expect(matching.getByRole("listitem")).toHaveCount(1)
  await expect(bindings).toContainText("15 bindings")
  await expect(
    bindings.getByRole("region", { name: "Storage", exact: true })
  ).toContainText("SQL database")
  await expect(
    bindings.getByRole("region", { name: "Storage", exact: true })
  ).toContainText("Blob bucket")
  await expect(
    bindings.getByRole("region", { name: "Background work", exact: true })
  ).toContainText("CHECKOUT")
  await expect(
    bindings.getByRole("link", { name: "Open CHECKOUT workflow workspace" })
  ).toHaveAttribute("href", "/fleets/main/workflows/commerce-api/CHECKOUT")
  const disclosure = bindings.getByRole("button", {
    name: "Other cell namespaces 8",
  })
  await expect(disclosure).toHaveAttribute("aria-expanded", "false")
  await expect(bindings.getByText("durable_object_namespace")).toHaveCount(0)
  await page.setViewportSize({ width: 1440, height: 1000 })
  await page.screenshot({
    path: testInfo.outputPath("cell-bindings-desktop.png"),
    fullPage: true,
  })
  await page.setViewportSize({ width: 390, height: 844 })
  await disclosure.focus()
  await page.keyboard.press("Enter")
  await expect(disclosure).toHaveAttribute("aria-expanded", "true")
  await expect(
    bindings
      .getByRole("list", { name: "Cell namespaces" })
      .getByRole("listitem")
  ).toHaveCount(8)
  await expect(bindings.getByText("SUPERVISOR", { exact: true })).toBeVisible()
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth)
  ).toBeLessThanOrEqual(390)
  await page.screenshot({
    path: testInfo.outputPath("cell-bindings-mobile.png"),
    fullPage: true,
  })
  await bindings
    .getByRole("link", { name: "Open DATABASE sql database workspace" })
    .click()
  await expect(page).toHaveURL(/\/fleets\/main\/sql\/commerce-api\/DATABASE/)
  await expect(
    page.getByRole("button", { name: "Run statement" })
  ).toBeVisible()
})

test("backing resource cells show their matching binding once with the correct workspace link", async ({
  page,
}) => {
  const cell = {
    scope: "__SQLDatabase:abc",
    class: "__SQLDatabase",
    kind: "sql",
    owner: nodes[0].id,
    epoch: 18,
    observed_at: now,
    error: null,
  }
  await page.route("**/api/v1/fleets/main/cells*", (route) =>
    route.fulfill({ json: pageOf([cell]) })
  )
  await page.route("**/api/v1/fleets/main/cells/*", (route) =>
    route.fulfill({ json: cell })
  )
  await page.getByRole("link", { name: "Cells", exact: true }).click()
  await page.getByRole("link", { name: /__SQLDatabase:abc/ }).click()
  const matching = page.getByRole("region", {
    name: "Bindings for this cell",
    exact: true,
  })
  await expect(matching).toContainText("Backing cell")
  await expect(
    matching.getByRole("link", { name: "Open DATABASE sql database workspace" })
  ).toBeVisible()
  await expect(
    page.getByRole("link", { name: "Open DATABASE sql database workspace" })
  ).toHaveCount(1)
  await expect(
    page.getByRole("region", { name: "Storage", exact: true })
  ).toContainText("CACHE")
})

test("desktop and narrow layouts have no page overflow", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1000 })
  await page.screenshot({
    path: testInfo.outputPath("overview-desktop.png"),
    fullPage: true,
  })
  await page.setViewportSize({ width: 390, height: 844 })
  await expect(
    page.getByRole("heading", { name: "Fleet overview", exact: true })
  ).toBeVisible()
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth)
  ).toBeLessThanOrEqual(390)
  await page.screenshot({
    path: testInfo.outputPath("overview-mobile.png"),
    fullPage: true,
  })
  await page.getByRole("button", { name: "Toggle Sidebar" }).click()
  await expect(
    page.getByRole("link", { name: "Key-value", exact: true })
  ).toBeVisible()
})

test("read-only SQL submits the enforced mode and displays returned columns", async ({
  page,
}) => {
  await page.getByRole("link", { name: "SQL databases", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  const sent = page.waitForRequest((request) =>
    request.url().endsWith("/resources/action")
  )
  await page.getByRole("button", { name: "Run statement" }).click()
  expect((await sent).postDataJSON().command.readonly).toBe(true)
  await expect(page.getByRole("alertdialog")).toHaveCount(0)
  await expect(
    page.getByRole("columnheader", { name: "name", exact: true })
  ).toBeVisible()
  await expect(
    page.getByRole("cell", { name: "customers", exact: true })
  ).toBeVisible()
})

test("KV continuation retains the queried prefix and values decode as UTF-8", async ({
  page,
}) => {
  let listCalls = 0
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.action === "kv_list") {
      listCalls++
      expect(command.prefix).toBe("sessions/")
      if (listCalls === 2) expect(command.after).toBe("sessions/1")
      await route.fulfill({
        json: {
          result: {
            keys: [
              {
                name: `sessions/${listCalls}`,
                metadata: null,
                expiration: null,
              },
            ],
            complete: listCalls === 2,
          },
        },
      })
    } else
      await route.fulfill({
        json: {
          result: {
            found: true,
            tag: "bytes",
            value: [104, 101, 108, 108, 111],
            metadata: '{"source":"test"}',
            expiration: null,
          },
        },
      })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Key prefix").fill("sessions/")
  await page.getByRole("button", { name: "List keys" }).click()
  await page.getByLabel("Key prefix").fill("edited-but-not-submitted")
  await page.getByRole("button", { name: "Next 100 keys" }).click()
  await page.getByRole("button", { name: "sessions/2", exact: true }).click()
  await expect(page.getByLabel("Key", { exact: true })).toHaveValue(
    "sessions/2"
  )
  await expect(page.locator(".json-view").first()).toContainText("hello")
  await expect(
    page.getByRole("button", { name: "Download value" })
  ).toBeVisible()
})

test("workflow controls identify the exact instance and confirm mutations", async ({
  page,
}) => {
  await page.route("**/resources/action", async (route) => {
    const request = route.request().postDataJSON()
    if (request.command.action === "workflow_list")
      await route.fulfill({
        json: {
          result: pageOf([
            {
              instanceId: "order-42",
              status: "waiting",
              createdMs: now,
              generation: "run-1",
            },
          ]),
        },
      })
    else
      await route.fulfill({
        json: {
          result: {
            instanceId: request.command.instance,
            status: "paused",
            generation: "run-1",
          },
        },
      })
  })
  await page.getByRole("link", { name: "Workflows", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "Load instances" }).click()
  await page.getByRole("button", { name: "order-42", exact: true }).click()
  await expect(page.getByLabel("Instance ID")).toHaveValue("order-42")
  await page.getByRole("button", { name: "Pause", exact: true }).click()
  await expect(page.getByRole("alertdialog")).toContainText("order-42")
  const sent = page.waitForRequest((r) => r.url().endsWith("/resources/action"))
  await page.getByRole("button", { name: "Confirm operation" }).click()
  const payload = (await sent).postDataJSON()
  expect(payload.application).toBe("commerce-api")
  expect(payload.binding).toBe("CHECKOUT")
  expect(payload.command).toEqual({
    action: "workflow",
    instance: "order-42",
    expected_generation: "run-1",
    command: { operation: "pause" },
  })
})

test("node controls discover capabilities and send the selected generation", async ({
  page,
}) => {
  await page.route("**/nodes/*/action", async (route) => {
    const body = route.request().postDataJSON()
    expect(body.generation).toBe("gen-0")
    await route.fulfill({
      json: {
        result:
          body.action === "state"
            ? {
                generation: "gen-0",
                authoritative: true,
                fleet_ready: true,
                draining: false,
                observed_at: now,
                capabilities: { balancing: true, reload: false },
                deployment: { version: "deploy-a46b293f", generation: 1 },
              }
            : {
                requested_paused: true,
                propagation: "awaiting_lease_publication",
              },
      },
    })
  })
  await page.getByRole("link", { name: /node-vancouver-01/ }).click()
  await expect(
    page.getByRole("button", { name: "Pause balancing", exact: true })
  ).toBeDisabled()
  await page
    .getByRole("button", { name: "Inspect live state", exact: true })
    .click()
  await expect(
    page.getByRole("button", { name: "Reload deployment", exact: true })
  ).toBeDisabled()
  await page
    .getByRole("button", { name: "Pause balancing", exact: true })
    .click()
  await expect(page.getByRole("alertdialog")).toContainText(
    "every donor in the fleet"
  )
  await expect(page.getByRole("alertdialog")).toContainText("Generation: gen-0")
  const request = page.waitForRequest((r) => r.url().endsWith("/action"))
  await page.getByRole("button", { name: "Confirm node action" }).click()
  expect((await request).postDataJSON().action).toBe("pause_balancing")
  await expect(page.locator(".json-view")).toContainText(
    "awaiting_lease_publication"
  )
})

test("cell eviction captures its owner generation and epoch before dispatch", async ({
  page,
}) => {
  let submitted: unknown
  await page.route(
    "**/api/v1/fleets/main/nodes/node-vancouver-01/action",
    async (route) => {
      submitted = route.request().postDataJSON()
      await route.fulfill({
        json: { result: { completed: true, outcome: "evicted" } },
      })
    }
  )
  await page.getByRole("link", { name: "Cells", exact: true }).click()
  await page.getByRole("link", { name: /Customer:alpha/ }).click()
  await page.getByRole("button", { name: "Evict cell", exact: true }).click()
  await expect(page.getByRole("alertdialog")).toContainText("Epoch: 18")
  await expect(page.getByRole("alertdialog")).toContainText(
    "Node generation: gen-0"
  )
  expect(submitted).toBeUndefined()
  await page.getByRole("button", { name: "Confirm eviction" }).click()
  await expect
    .poll(() => submitted)
    .toMatchObject({
      generation: "gen-0",
      action: { evict: { scope: "Customer:alpha", epoch: 18 } },
    })
})

test("SQL explorer reads schema and retains migration pagination target", async ({
  page,
}) => {
  const commands: Record<string, unknown>[] = []
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    commands.push(command)
    await route.fulfill({
      json: {
        result:
          command.action === "sql_migrations"
            ? {
                items: [
                  {
                    id: command.after ? 101 : 100,
                    name: "add_customers.sql",
                    applied_at: "2026-10-13 10:00:00",
                  },
                ],
                exists: true,
                table: command.table,
                next_cursor: command.after ? null : 100,
              }
            : {
                results: [
                  {
                    columns: ["name", "type", "sql"],
                    rows: [
                      [
                        "customers",
                        "table",
                        "CREATE TABLE customers(id INTEGER)",
                      ],
                    ],
                  },
                ],
              },
      },
    })
  })
  await page.getByRole("link", { name: "SQL databases", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "Browse schema" }).click()
  await expect(
    page.getByRole("cell", { name: "customers", exact: true })
  ).toBeVisible()
  expect(commands[0].readonly).toBe(true)
  await page.getByRole("button", { name: "Load migration history" }).click()
  await expect(
    page.getByRole("cell", { name: "add_customers.sql" })
  ).toBeVisible()
  await page
    .getByLabel("Migration history table")
    .fill("__sys_sql_migrations_other")
  await page.getByRole("button", { name: "Next migrations" }).click()
  await expect
    .poll(() => commands.at(-1))
    .toEqual({
      action: "sql_migrations",
      table: "__sys_sql_migrations",
      after: 100,
    })
})

test("node-owned cells show observed alarms and preserve continuation", async ({
  page,
}) => {
  const requests: unknown[] = []
  await page.route("**/nodes/*/action", async (route) => {
    const body = route.request().postDataJSON()
    requests.push(body)
    await route.fulfill({
      json: {
        result: {
          authoritative: true,
          observed_at: now,
          next_cursor: body.action.cells.after ? null : "Customer:alpha",
          items: [
            {
              scope: body.action.cells.after
                ? "Customer:beta"
                : "Customer:alpha",
              epoch: 18,
              phase: "resident",
              active_requests: 0,
              alarm_at: now + 60000,
              alarm_firing: false,
            },
          ],
        },
      },
    })
  })
  await page.getByRole("link", { name: /node-vancouver-01/ }).click()
  await page.getByRole("button", { name: "Inspect owned cells" }).click()
  await expect(
    page.getByRole("link", { name: "Customer:alpha", exact: true })
  ).toBeVisible()
  await expect(
    page.getByRole("columnheader", { name: "Alarm", exact: true })
  ).toBeVisible()
  await page.getByRole("button", { name: "Next owned cells" }).click()
  await expect(
    page.getByRole("link", { name: "Customer:beta", exact: true })
  ).toBeVisible()
  expect(requests.at(-1)).toMatchObject({
    generation: "gen-0",
    action: { cells: { after: "Customer:alpha", limit: 100 } },
  })
  await page.screenshot({
    path: "test-results/node-owned-cells.png",
    fullPage: true,
  })
})

test("cron edits retain their observed revision and distinguish saved from adopted", async ({
  page,
}) => {
  let configuration = {
    application: "commerce-api",
    deployment_revision: "deploy-observed",
    prefix: "deploy/commerce-api/v1",
    revision: "cron-observed",
    expressions: ["*/5 * * * *"],
    primary: true,
  }
  const writes: Record<string, unknown>[] = []
  await page.route("**/applications/commerce-api/cron**", async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname.endsWith("/nodes")) {
      await route.fulfill({
        json: pageOf([
          {
            node: nodes[0].id,
            generation: "gen-0",
            status: "adopted",
            observed_revision: "cron-saved",
            observed_at: now,
            error: null,
          },
          {
            node: nodes[1].id,
            generation: "gen-1",
            status: "pending",
            observed_revision: "cron-observed",
            observed_at: now,
            error: null,
          },
        ]),
      })
    } else if (route.request().method() === "POST") {
      const body = route.request().postDataJSON()
      writes.push(body)
      configuration = {
        ...configuration,
        revision: "cron-saved",
        expressions: body.expressions,
      }
      await route.fulfill({
        json: {
          configuration,
          propagation: "pending",
          operation: { status: "accepted" },
        },
      })
    } else await route.fulfill({ json: configuration })
  })
  await page.getByRole("link", { name: "Cron schedules", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "Edit schedules" }).click()
  await page.getByLabel("Cron expressions", { exact: true }).fill("0 * * * *")
  // A background refresh cannot silently replace the revision being edited.
  configuration = { ...configuration, revision: "cron-new-observation" }
  await page.getByRole("button", { name: "Refresh configuration" }).click()
  await expect(
    page.getByText("Schedule revision: cron-new-observation")
  ).toBeVisible()
  await page.getByRole("button", { name: "Save schedules" }).click()
  expect(writes).toHaveLength(0)
  await expect(page.getByRole("alertdialog")).toContainText("deploy-observed")
  await page.getByRole("button", { name: "Confirm schedule change" }).click()
  await expect.poll(() => writes.length).toBe(1)
  expect(writes[0]).toMatchObject({
    deployment_revision: "deploy-observed",
    expected_revision: "cron-observed",
    expressions: ["0 * * * *"],
  })
  await expect(
    page.getByText("Schedule saved. Check node adoption below.")
  ).toBeVisible()
  await page.getByRole("button", { name: "Check node adoption" }).click()
  await expect(
    page.getByRole("row", { name: new RegExp(nodes[0].id) })
  ).toContainText("adopted")
  await expect(
    page.getByRole("row", { name: new RegExp(nodes[1].id) })
  ).toContainText("pending")
  await page.evaluate(() => window.scrollTo(0, 0))
  await page.screenshot({
    path: "test-results/cron-configuration.png",
    fullPage: true,
  })
})

test("node stop waits for durable joined proof after a lost acknowledgment", async ({
  page,
}) => {
  let stopBody: Record<string, unknown> | null = null
  let joined = false
  await page.route("**/nodes/*/action", async (route) => {
    const body = route.request().postDataJSON()
    if (body.action === "state") {
      await route.fulfill({
        json: {
          result: {
            generation: "gen-0",
            authoritative: true,
            fleet_ready: true,
            draining: false,
            observed_at: now,
            capabilities: { balancing: true, reload: true, stop: true },
            deployment: null,
          },
        },
      })
    } else {
      stopBody = body
      await route.abort("connectionreset")
    }
  })
  await page.route("**/nodes/*/stops/**", async (route) => {
    await route.fulfill({
      json: {
        receipt: {
          phase: joined ? "complete" : "draining_ingress",
          requested_mode: "handoff",
          observed_at: now,
          domain_joined: joined,
          error: null,
          proof: joined
            ? {
                mode: "handoff",
                ingress_drained: true,
                application_work_drained: true,
                pending_application_events: 0,
                cells_drained: true,
                pending_dispatches: 0,
                log_quiesced: true,
                local_durability_closed: true,
                clean_reload_prepared: false,
                drain_timed_out: false,
                deadline_elapsed: false,
              }
            : null,
        },
      },
    })
  })
  await page.getByRole("link", { name: /node-vancouver-01/ }).click()
  await expect(
    page.getByRole("button", { name: "Drain and stop node", exact: true })
  ).toBeDisabled()
  await page
    .getByRole("button", { name: "Inspect live state", exact: true })
    .click()
  await page
    .getByRole("button", { name: "Drain and stop node", exact: true })
    .click()
  expect(stopBody).toBeNull()
  await expect(page.getByRole("alertdialog")).toContainText("Generation: gen-0")
  await page.getByRole("button", { name: "Confirm node stop" }).click()
  await expect.poll(() => stopBody).not.toBeNull()
  expect(stopBody).toMatchObject({
    generation: "gen-0",
    action: { stop: { mode: "handoff" } },
  })
  await expect(
    page.getByText("Stop acknowledgment was not confirmed")
  ).toBeVisible()
  await expect(
    page.getByText("draining ingress", { exact: true })
  ).toBeVisible()
  await expect(
    page.getByText("The node has not yet published its final drain proof.")
  ).toBeVisible()
  joined = true
  await page.getByRole("button", { name: "Refresh receipt" }).click()
  await expect(page.getByText("complete", { exact: true })).toBeVisible()
  await expect(
    page
      .getByRole("region", { name: "Node stop proof" })
      .getByText("Confirmed", { exact: true })
  ).toHaveCount(6)
  await expect(
    page.getByRole("button", { name: "Drain and stop node", exact: true })
  ).toBeDisabled()
})

test("keyboard cancellation returns focus without issuing a write", async ({
  page,
}) => {
  let writes = 0
  await page.route("**/resources/action", async (route) => {
    writes += 1
    await route.fulfill({ json: { error: "No write should be submitted" } })
  })
  await page.getByRole("link", { name: "SQL databases", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Query mode").click()
  await page
    .getByRole("option", { name: "Read and write", exact: true })
    .click()
  const run = page.getByRole("button", { name: "Run statement" })
  await run.focus()
  await page.keyboard.press("Enter")
  await expect(page.getByRole("alertdialog")).toBeVisible()
  await expect(
    page.getByRole("button", { name: "Cancel", exact: true })
  ).toBeFocused()
  await page.keyboard.press("Tab")
  await expect(
    page.getByRole("button", { name: "Confirm operation" })
  ).toBeFocused()
  for (let index = 0; index < 6; index += 1) {
    await page.keyboard.press("Tab")
    await expect
      .poll(() =>
        page
          .getByRole("alertdialog")
          .evaluate((dialog) => dialog.contains(document.activeElement))
      )
      .toBe(true)
  }
  await page.keyboard.press("Escape")
  await expect(page.getByRole("alertdialog")).not.toBeVisible()
  await expect(run).toBeFocused()
  expect(writes).toBe(0)
})

test("workflow definition and execution explorer preserve versions across paged restarts", async ({
  page,
}) => {
  const definition = (version: string) => ({
    version,
    nodes: [
      { id: "start", label: "Order received", kind: "start" },
      {
        id: "prepare",
        label: "Prepare order",
        kind: "activity",
        step: "prepare",
        description: "Validate items and reserve inventory.",
      },
      { id: "decision", label: "Approval required?", kind: "decision" },
      {
        id: "approve",
        label: "Wait for approval",
        kind: "wait",
        step: "approval",
      },
      {
        id: "publish",
        label: "Publish receipt",
        kind: "activity",
        step: "publish",
      },
    ],
    edges: [
      { from: "start", to: "prepare" },
      { from: "prepare", to: "decision" },
      { from: "decision", to: "approve", label: "yes" },
      { from: "decision", to: "publish", label: "no" },
      { from: "approve", to: "publish" },
    ],
  })
  const app = structuredClone(apps[0])
  Object.assign(app.bindings[3], { definition: definition("checkout-v2") })
  await page.route("**/applications/commerce-api", (route) =>
    route.fulfill({ json: app })
  )
  await page.route("**/resources/action", async (route) => {
    const request = route.request().postDataJSON()
    const cursor = request.command.command.cursor
    const event = (eventId: number, type: string, fields = {}) => ({
      eventId,
      timestamp: now + eventId * 100,
      instanceId: request.command.instance,
      generation: "run-1",
      type,
      ...fields,
    })
    await route.fulfill({
      json: {
        result: {
          instanceId: request.command.instance,
          cursor,
          done: false,
          definitions: cursor
            ? {
                "run-2": {
                  definition: definition("checkout-v2"),
                  revision: "digest-two",
                },
              }
            : {
                "run-1": {
                  definition: definition("checkout-v1"),
                  revision: "digest-one",
                },
              },
          events: cursor
            ? [
                event(5, "workflow_completed", {
                  output: { receipt: "receipt-42" },
                }),
                event(6, "workflow_queued", { generation: "run-2" }),
                event(7, "step_started", {
                  generation: "run-2",
                  stepName: "prepare",
                  count: 1,
                  input: "[REDACTED]",
                }),
              ]
            : [
                event(1, "workflow_started", { params: { order: 42 } }),
                event(2, "step_started", {
                  stepName: "prepare",
                  count: 1,
                  input: { items: 3 },
                }),
                event(3, "attempt_errored", {
                  stepName: "prepare",
                  count: 1,
                  attempt: 1,
                  error: { message: "Inventory busy" },
                }),
                event(4, "step_completed", {
                  stepName: "prepare",
                  count: 1,
                  attempt: 2,
                  output: { reserved: true },
                }),
              ],
        },
      },
    })
  })
  await page.getByRole("link", { name: "Workflows", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await expect(page.getByText("checkout-v2", { exact: true })).toBeVisible()
  await page
    .getByRole("button", { name: "Prepare order activity · prepare" })
    .click()
  await expect(
    page.getByText("Validate items and reserve inventory.")
  ).toBeVisible()
  await expect(
    page.getByText("Outgoing paths: Approval required?")
  ).toBeVisible()
  await page.getByLabel("Instance ID").fill("order-42")
  await page.getByRole("button", { name: "Load history", exact: true }).click()
  const explorer = page.getByLabel("Workflow execution explorer")
  await explorer.getByRole("button", { name: /prepare step completed/ }).click()
  await expect(explorer.getByLabel("Step inspector")).toContainText(
    '"reserved": true'
  )
  await expect(explorer.getByLabel("Step inspector")).toContainText(
    '"items": 3'
  )
  await explorer.getByRole("tab", { name: "Run definition" }).click()
  await expect(explorer.getByText("checkout-v1", { exact: true })).toBeVisible()
  await explorer
    .getByRole("button", { name: "Prepare order step completed" })
    .click()
  await expect(explorer).toContainText('"reserved": true')
  await expect(explorer).toContainText('"items": 3')
  await explorer.getByRole("button", { name: "Next history page" }).click()
  await explorer.getByLabel("Execution generation").selectOption("run-2")
  await expect(explorer.getByText("checkout-v2", { exact: true })).toBeVisible()
  await explorer.getByRole("tab", { name: "Execution path" }).click()
  await explorer.getByRole("button", { name: /prepare step started/ }).click()
  await expect(explorer.getByLabel("Step inspector")).toContainText(
    "[REDACTED]"
  )
  await explorer.getByLabel("Execution generation").selectOption("run-1")
  await explorer.getByRole("button", { name: /workflow completed/ }).click()
  await expect(explorer.getByLabel("Step inspector")).toContainText(
    "receipt-42"
  )
  await expect(explorer).toContainText("7 loaded events")
  await page.getByLabel("Instance ID").fill("order-99")
  await page.getByRole("button", { name: "Load history", exact: true }).click()
  await expect(explorer).toContainText("order-99 · 4 loaded events")
  await expect(explorer).not.toContainText("order-42")
  await explorer.getByRole("button", { name: /prepare step completed/ }).click()
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth
    )
  ).toBeTruthy()
  await explorer.screenshot({ path: "test-results/workflow-execution.png" })
  await page
    .getByLabel("Workflow definition graph")
    .screenshot({ path: "test-results/workflow-definition.png" })
  await page.getByRole("radio", { name: "Dark theme", exact: true }).click()
  await expect(page.locator("html")).toHaveClass("dark")
  await explorer.screenshot({
    path: "test-results/workflow-execution-dark.png",
  })
  await page.setViewportSize({ width: 390, height: 844 })
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth
    )
  ).toBeTruthy()
})

test("theme preferences follow the system, persist and keep text readable", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1000 })
  await page.getByRole("radio", { name: "System theme", exact: true }).click()
  for (const theme of ["dark", "light"] as const) {
    await page.emulateMedia({ colorScheme: theme })
    await expect(page.locator("html")).toHaveClass(theme)
    const contrasts = await page.evaluate(() => {
      const root = getComputedStyle(document.documentElement)
      const luminance = (name: string) => {
        const hex = root.getPropertyValue(name).trim().replace("#", "")
        const channels = [0, 2, 4].map((offset) => {
          const c = parseInt(hex.slice(offset, offset + 2), 16) / 255
          return c <= 0.04045 ? c / 12.92 : ((c + 0.055) / 1.055) ** 2.4
        })
        return (
          channels[0] * 0.2126 + channels[1] * 0.7152 + channels[2] * 0.0722
        )
      }
      return [
        ["--foreground", "--background"],
        ["--card-foreground", "--card"],
        ["--muted-foreground", "--card"],
        ["--muted-foreground", "--muted"],
        ["--primary-foreground", "--primary"],
        ["--primary-foreground", "--primary-hover"],
        ["--info", "--card"],
        ["--info", "--info-muted"],
        ["--success", "--success-muted"],
        ["--warning", "--warning-muted"],
        ["--destructive", "--destructive-muted"],
      ].map(([foreground, background]) => {
        const a = luminance(foreground),
          b = luminance(background)
        return {
          foreground,
          background,
          ratio: (Math.max(a, b) + 0.05) / (Math.min(a, b) + 0.05),
        }
      })
    })
    for (const pair of contrasts) {
      expect(
        pair.ratio,
        `${theme}: ${pair.foreground} on ${pair.background}`
      ).toBeGreaterThanOrEqual(4.5)
    }
    await page.screenshot({
      path: testInfo.outputPath(`overview-${theme}.png`),
      fullPage: true,
    })
  }
  await page.getByRole("radio", { name: "Dark theme", exact: true }).click()
  await expect(page.locator("html")).toHaveClass("dark")
  await page.keyboard.press("d")
  await expect(page.locator("html")).toHaveClass("dark")
  await page.reload()
  await expect(page.locator("html")).toHaveClass("dark")
  await expect(page.locator("html")).toHaveCSS("color-scheme", "dark")
})

test("mobile navigation closes after selection and retains usable controls", async ({
  page,
}, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 })
  await page
    .getByRole("button", { name: "Toggle Sidebar", exact: true })
    .click()
  const navigation = page.getByRole("navigation", { name: "Fleet navigation" })
  await navigation.getByRole("link", { name: "Nodes", exact: true }).click()
  await expect(
    page.getByRole("heading", { name: "Nodes", exact: true })
  ).toBeVisible()
  await expect(page.getByRole("dialog")).not.toBeVisible()
  const filter = page.getByLabel("Filter this page")
  await filter.fill("vancouver-02")
  await expect(
    page.getByRole("link", { name: /node-vancouver-02/ })
  ).toBeVisible()
  await expect(
    page.getByRole("link", { name: /node-vancouver-01/ })
  ).not.toBeVisible()
  await page.getByRole("radio", { name: "Dark theme", exact: true }).click()
  for (const width of [390, 320]) {
    await page.setViewportSize({ width, height: 844 })
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth)
    ).toBeLessThanOrEqual(width)
    for (const name of [
      "Toggle Sidebar",
      "Light theme",
      "System theme",
      "Dark theme",
    ]) {
      const bounds = await page
        .getByRole(name === "Toggle Sidebar" ? "button" : "radio", {
          name,
          exact: true,
        })
        .boundingBox()
      expect(bounds?.height).toBeGreaterThanOrEqual(44)
      expect(bounds?.width).toBeGreaterThanOrEqual(44)
    }
  }
  await page.setViewportSize({ width: 390, height: 844 })
  await page.screenshot({
    path: testInfo.outputPath("nodes-mobile-dark.png"),
    fullPage: true,
  })
})

test("resource filtering has a recovery path and keyboard-visible navigation", async ({
  page,
}, testInfo) => {
  const link = page
    .getByRole("navigation", { name: "Fleet navigation" })
    .getByRole("link", { name: "SQL databases", exact: true })
  await link.focus()
  await page.keyboard.press("Tab")
  await page.keyboard.press("Shift+Tab")
  await expect(link).toBeFocused()
  expect(
    await link.evaluate((element) => getComputedStyle(element).boxShadow)
  ).not.toBe("none")
  await page.keyboard.press("Enter")
  await expect(link).toHaveAttribute("aria-current", "page")
  await page.getByLabel("Filter resources on this page").fill("does-not-exist")
  await expect(
    page.getByText("No matching resources", { exact: true })
  ).toBeVisible()
  await page.getByRole("button", { name: "Clear filter", exact: true }).click()
  await expect(page.getByRole("link", { name: "Open workspace" })).toBeVisible()
  await page.getByLabel("Filter resources on this page").fill("commerce-api")
  await expect(page.getByRole("link", { name: "Open workspace" })).toBeVisible()
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await expect(
    page.getByRole("heading", { name: "Key-value", exact: true })
  ).toBeVisible()
  await expect(page.getByLabel("Filter resources on this page")).toHaveValue("")
  await link.click()
  await expect(
    page.getByRole("heading", { name: "SQL databases", exact: true })
  ).toBeVisible()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await expect(
    page.getByRole("heading", { name: "DATABASE", exact: true })
  ).toBeVisible()
  await page.getByRole("radio", { name: "Dark theme", exact: true }).click()
  await page.screenshot({
    path: testInfo.outputPath("sql-workspace-dark.png"),
    fullPage: true,
  })
})
