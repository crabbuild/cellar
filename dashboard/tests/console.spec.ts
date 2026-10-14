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
  await expect(page.getByLabel("Exact key")).toHaveValue("sessions/2")
  await expect(page.getByLabel("Value preview")).toHaveText("hello")
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
  await expect(page.locator(".schema-object > summary")).toContainText(
    "customers"
  )
  expect(commands[0].readonly).toBe(true)
  await page.locator(".schema-migrations > summary").click()
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
    if (route.request().postDataJSON().command.readonly === false) writes += 1
    await route.fulfill({ json: { result: { results: [] } } })
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

const sqlSchema = {
  results: [
    {
      columns: ["name", "type", "sql"],
      rows: [
        ["customers", "table", "CREATE TABLE customers(id INTEGER, name TEXT)"],
      ],
    },
    {
      columns: ["table", "name", "type", "pk"],
      rows: [
        ["customers", "id", "INTEGER", 1],
        ["customers", "name", "TEXT", 0],
      ],
    },
  ],
}

test("SQL tabs, schema completion, selection shortcuts, query plans and session history", async ({
  page,
}) => {
  const runs: { sql: string; readonly: boolean }[] = []
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.sql.includes("SELECT name, type, sql FROM sqlite_schema"))
      return route.fulfill({ json: { result: sqlSchema } })
    runs.push(command)
    return route.fulfill({
      json: {
        result: {
          results: [{ columns: ["id"], rows: [[runs.length]] }],
          duration: 1,
          count: 1,
        },
      },
    })
  })
  await page.getByRole("link", { name: "SQL databases", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  const editor = page.getByLabel("SQL statement", { exact: true })
  await editor.fill("SELECT * FROM cust")
  await editor.press("Control+Space")
  await page.getByRole("option", { name: "customers", exact: true }).click()
  await expect(editor).toHaveText("SELECT * FROM customers")
  await editor.fill("SELECT customers.na")
  await editor.press("Control+Space")
  await page.getByRole("option", { name: "name", exact: true }).click()
  await expect(editor).toHaveText("SELECT customers.name")
  const full = "SELECT id FROM customers;\nSELECT name FROM customers;"
  await editor.fill(full)
  await editor.press("ArrowUp")
  await editor.press("Home")
  await editor.press("Shift+End")
  await editor.press("ControlOrMeta+Enter")
  await expect
    .poll(() => runs.at(-1))
    .toMatchObject({ sql: "SELECT id FROM customers;", readonly: true })
  await expect(page.getByRole("cell", { name: "1", exact: true })).toBeVisible()
  await editor.press("ControlOrMeta+Shift+Enter")
  await expect
    .poll(() => runs.at(-1))
    .toMatchObject({ sql: full, readonly: true })
  await expect(page.getByRole("cell", { name: "2", exact: true })).toBeVisible()
  await page.getByRole("button", { name: "New query tab" }).click()
  await editor.fill("SELECT 42;")
  await page.getByRole("button", { name: "Explain query", exact: true }).click()
  await expect
    .poll(() => runs.at(-1))
    .toMatchObject({ sql: "EXPLAIN QUERY PLAN SELECT 42;", readonly: true })
  await page.getByRole("tab", { name: "Query 1", exact: true }).click()
  await expect.poll(() => editor.innerText()).toBe(full)
  await expect(page.getByRole("cell", { name: "2", exact: true })).toBeVisible()
  await page.getByRole("tab", { name: "Query 2", exact: true }).click()
  await expect(editor).toHaveText("SELECT 42;")
  await expect(page.getByRole("cell", { name: "3", exact: true })).toBeVisible()
  const handle = page.getByRole("separator", {
    name: "Resize SQL editor and results",
  })
  const before = await editor.boundingBox()
  await handle.focus()
  await handle.press("ArrowDown")
  await expect
    .poll(async () => (await editor.boundingBox())!.height)
    .not.toBe(before!.height)
  await page.getByRole("button", { name: "History (3)" }).click()
  const history = page.getByRole("region", { name: "Session query history" })
  await expect(history).toContainText("EXPLAIN QUERY PLAN SELECT 42;")
  await history
    .getByRole("button", { name: /EXPLAIN QUERY PLAN SELECT 42;/ })
    .click()
  await expect(
    page.getByRole("tab", { name: "History query" })
  ).toHaveAttribute("aria-selected", "true")
  await expect(editor).toHaveText("EXPLAIN QUERY PLAN SELECT 42;")
  expect(runs).toHaveLength(3)
})

test("SQL loaded results sort, filter, inspect full values and export exact bytes", async ({
  page,
}) => {
  const value = 'a long value, with "quotes"\nand a newline'
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    await route.fulfill({
      json: {
        result: command.sql.includes(
          "SELECT name, type, sql FROM sqlite_schema"
        )
          ? sqlSchema
          : {
              results: [
                {
                  columns: ["id", "name", "note"],
                  rows: [
                    [10, "Beta", null],
                    [2, "Alpha", value],
                    [30, "Other", "Alpha"],
                  ],
                },
              ],
              count: 1,
              duration: 2,
            },
      },
    })
  })
  await page.getByRole("link", { name: "SQL databases", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "Run statement" }).click()
  await page.getByRole("button", { name: "Sort by id", exact: true }).click()
  await expect(
    page.locator(".result-table-scroll tbody tr").first()
  ).toContainText("2Alpha")
  await page.getByLabel("Filter column").click()
  await page.getByRole("option", { name: "name", exact: true }).click()
  await page.getByLabel("Filter loaded rows").fill("Alpha")
  await expect(page.locator(".result-table-scroll tbody tr")).toHaveCount(1)
  await page.getByRole("button", { name: value, exact: true }).click()
  await expect(page.getByRole("dialog").locator("pre")).toHaveText(value)
  await page.getByRole("dialog").getByRole("button", { name: "Close" }).click()
  const csv = page.waitForEvent("download")
  await page.getByRole("button", { name: "CSV", exact: true }).click()
  const fs = await import("node:fs/promises")
  const csvDownload = await csv
  expect(await fs.readFile((await csvDownload.path())!, "utf8")).toBe(
    '"id","name","note"\r\n"2","Alpha","a long value, with ""quotes""\nand a newline"\r\n'
  )
  const json = page.waitForEvent("download")
  await page.getByRole("button", { name: "JSON", exact: true }).click()
  expect(
    JSON.parse(await fs.readFile((await (await json).path())!, "utf8"))
  ).toEqual({ columns: ["id", "name", "note"], rows: [[2, "Alpha", value]] })
})

test("KV drafts load value metadata expiry and preserve them through reviewed conditional saves", async ({
  page,
}) => {
  const expiration = Date.now() + 86400000
  let inspected = {
    found: true,
    revision: "5",
    tag: "json",
    value: Array.from(Buffer.from('{"n":1}')),
    metadata: '{"source":"SDK"}',
    expiration,
  }
  const commands: Record<string, unknown>[] = []
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    commands.push(command)
    if (command.action === "kv_edit") {
      expect(route.request().postDataJSON().expected_identity).toBe(
        "commerce-cache"
      )
      expect(command.edit.condition).toEqual({ revision: "5" })
      expect(command.edit.expiration).toEqual({ mode: "preserve" })
      expect(command.edit.tag).toBe("json")
      expect(command.edit.metadata).toBe('{"source":"SDK"}')
      inspected = {
        ...inspected,
        revision: "6",
        value: Array.from(Buffer.from(command.edit.value_base64, "base64")),
      }
      return route.fulfill({
        json: {
          result: { ok: true, conflict: false },
          operation: { status: "succeeded" },
        },
      })
    }
    return route.fulfill({ json: { result: inspected } })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Exact key").fill("sessions/customer")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await expect(page.getByLabel("Value preview")).toHaveText('{"n":1}')
  await page.getByRole("button", { name: "Edit value", exact: true }).click()
  await expect(page.getByLabel("Metadata (JSON)")).toHaveValue(
    '{\n  "source": "SDK"\n}'
  )
  await expect(page.getByLabel("Expiration", { exact: true })).toContainText(
    "Preserve current expiry"
  )
  const value = page.getByLabel("Value", { exact: true })
  await value.fill("{broken")
  await expect(
    page.getByRole("button", { name: "Review changes" })
  ).toBeDisabled()
  await value.fill('{"n":2}')
  await page.getByRole("button", { name: "Format JSON" }).click()
  await expect.poll(() => value.innerText()).toBe('{\n  "n": 2\n}')
  await page.getByRole("button", { name: "Review changes" }).click()
  await expect(page.getByRole("alertdialog")).toContainText(
    "Before · revision 5"
  )
  await expect(page.getByRole("alertdialog")).toContainText('{"n":1}')
  await expect(page.getByRole("alertdialog")).toContainText(
    new Date(expiration).toISOString()
  )
  expect(commands.filter((c) => c.action === "kv_edit")).toHaveLength(0)
  await page.getByRole("button", { name: "Cancel", exact: true }).click()
  await expect.poll(() => value.innerText()).toBe('{\n  "n": 2\n}')
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await expect
    .poll(() => page.getByLabel("Value preview").innerText())
    .toBe('{\n  "n": 2\n}')
  expect(inspected.expiration).toBe(expiration)
  expect(commands.filter((c) => c.action === "kv_edit")).toHaveLength(1)
})

test("KV conflicts keep the draft and require comparison before saving a new revision", async ({
  page,
}) => {
  let reads = 0
  const writes: {
    action: string
    edit: { condition: unknown; metadata: string; value_base64: string }
  }[] = []
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.action === "kv_get") {
      reads++
      return route.fulfill({
        json: {
          result: {
            found: true,
            revision: reads === 1 ? "9" : "10",
            tag: "text",
            value: Array.from(
              Buffer.from(
                reads === 1
                  ? "initial"
                  : writes.length === 2
                    ? "my draft"
                    : "another writer"
              )
            ),
            metadata: reads === 1 ? '{"source":"old"}' : '{"source":"new"}',
            expiration: null,
          },
        },
      })
    }
    writes.push(command)
    return writes.length === 1
      ? route.fulfill({
          status: 409,
          json: {
            error: "This key changed or expired after it was inspected.",
          },
        })
      : route.fulfill({
          json: { result: { ok: true }, operation: { status: "succeeded" } },
        })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Exact key").fill("key")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await page.getByRole("button", { name: "Edit value" }).click()
  await page.getByLabel("Value", { exact: true }).fill("my draft")
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await expect(page.getByRole("alert")).toContainText(
    "Key changed while you edited"
  )
  await expect(page.getByLabel("Value", { exact: true })).toHaveText("my draft")
  await expect(
    page.getByRole("button", { name: "Review changes" })
  ).toBeDisabled()
  expect(writes).toHaveLength(1)
  await page.getByRole("button", { name: "Compare latest" }).click()
  await expect(page.locator(".kv-conflict")).toContainText("another writer")
  await page.getByRole("button", { name: "Use latest as baseline" }).click()
  await expect(page.getByLabel("Metadata (JSON)")).toHaveValue(
    '{\n  "source": "new"\n}'
  )
  await expect(page.getByLabel("Value", { exact: true })).toHaveText("my draft")
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await expect(page.getByLabel("Value preview")).toHaveText("my draft")
  expect(writes[1].edit.condition).toEqual({ revision: "10" })
  expect(writes[1].edit.metadata).toBe('{"source":"new"}')
  expect(Buffer.from(writes[1].edit.value_base64, "base64").toString()).toBe(
    "my draft"
  )
})

test("KV binary bytes, expiry modes, deletion and unsaved draft navigation are explicit", async ({
  page,
}) => {
  const writes: Record<string, unknown>[] = []
  let found = true
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.action === "kv_list")
      return route.fulfill({
        json: {
          result: {
            keys: [
              { name: "binary", expiration: null, metadata: null },
              { name: "other", expiration: null, metadata: null },
            ],
            complete: true,
          },
        },
      })
    if (command.action === "kv_get")
      return route.fulfill({
        json: {
          result: found
            ? {
                found: true,
                revision: String(11 + writes.length),
                tag: "bytes",
                value: [0, 255, 1],
                metadata: "null",
                expiration: null,
              }
            : { found: false, revision: null },
        },
      })
    writes.push(command)
    if (command.action === "kv_delete_if") found = false
    return route.fulfill({
      json: { result: { ok: true }, operation: { status: "succeeded" } },
    })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "List keys", exact: true }).click()
  await page.getByRole("button", { name: "binary", exact: true }).click()
  await expect(page.getByLabel("Value preview")).toHaveText("AP8B")
  const download = page.waitForEvent("download")
  await page.getByRole("button", { name: "Download value" }).click()
  const fs = await import("node:fs/promises")
  expect(
    Array.from(await fs.readFile((await (await download).path())!))
  ).toEqual([0, 255, 1])
  await page.getByLabel("Value view").click()
  await page.getByRole("option", { name: "Text", exact: true }).click()
  await expect(page.getByRole("alert")).toContainText("not valid UTF-8")
  await expect(page.getByLabel("Value preview")).toHaveText("AP8B")
  await page.getByRole("button", { name: "Edit value", exact: true }).click()
  await page.getByLabel("Value", { exact: true }).fill("AP8C")
  await page.getByRole("button", { name: "other", exact: true }).click()
  await expect(page.getByRole("alertdialog")).toContainText(
    "Discard unsaved draft?"
  )
  await page.getByRole("button", { name: "Keep editing" }).click()
  await expect(page.getByLabel("Value", { exact: true })).toHaveText("AP8C")
  await page.getByLabel("Expiration", { exact: true }).click()
  await page.getByRole("option", { name: "Expire at", exact: true }).click()
  await page.getByLabel("Expiration date and time").fill("2099-04-05T12:30")
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await expect(page.getByLabel("Value preview")).toBeVisible()
  const timestamp = await page.evaluate(() =>
    new Date("2099-04-05T12:30:00").getTime()
  )
  expect(writes[0]).toMatchObject({
    action: "kv_edit",
    edit: {
      value_base64: "AP8C",
      metadata: "null",
      tag: "bytes",
      condition: { revision: "11" },
      expiration: { mode: "at", timestamp_ms: timestamp },
    },
  })
  await page.getByRole("button", { name: "Delete key", exact: true }).click()
  await expect(page.getByRole("alertdialog")).toContainText("binary")
  await page
    .getByRole("button", { name: "Delete inspected key", exact: true })
    .click()
  await expect(
    page.getByRole("button", { name: "Create this key" })
  ).toBeVisible()
  expect(writes[1]).toEqual({
    action: "kv_delete_if",
    key: "binary",
    condition: { revision: "12" },
  })
  await page.getByRole("button", { name: "Create this key" }).click()
  await page.getByLabel("Value", { exact: true }).fill("new value")
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await expect.poll(() => writes.length).toBe(3)
  expect(writes[2]).toMatchObject({
    action: "kv_edit",
    edit: {
      condition: { revision: null },
      expiration: { mode: "never" },
      value_base64: Buffer.from("new value").toString("base64"),
    },
  })
})

test("KV ignores a stale read and bounds large values", async ({ page }) => {
  let release: (() => void) | undefined
  let oldStarted = false
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.key === "slow") {
      oldStarted = true
      await new Promise<void>((resolve) => {
        release = resolve
      })
    }
    await route.fulfill({
      json: {
        result:
          command.key === "large"
            ? {
                found: true,
                valueOmitted: true,
                size: 2000000,
                revision: "3",
                metadata: '{"source":"SDK"}',
                expiration: null,
                tag: "bytes",
              }
            : {
                found: true,
                revision: "2",
                value: Array.from(Buffer.from(command.key)),
                metadata: null,
                expiration: null,
                tag: "text",
              },
      },
    })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Exact key").fill("slow")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await expect.poll(() => oldStarted).toBe(true)
  await page.getByRole("button", { name: "New key", exact: true }).click()
  await page.getByLabel("Key", { exact: true }).fill("new-draft")
  release!()
  await expect(page.getByLabel("Key", { exact: true })).toHaveValue("new-draft")
  await page.getByRole("button", { name: "Discard draft", exact: true }).click()
  await page
    .getByRole("alertdialog")
    .getByRole("button", { name: "Discard draft" })
    .click()
  await page.getByLabel("Exact key").fill("large")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await expect(
    page.getByRole("heading", { name: "Value exceeds the inspection limit" })
  ).toBeVisible()
  await expect(page.getByRole("heading", { name: "Edit key" })).toHaveCount(0)
  await expect(page.getByRole("button", { name: "Edit value" })).toHaveCount(0)
  await expect(page.locator(".kv-metadata")).toContainText('"source": "SDK"')
})

test("KV text drafts preserve UTF-8 BOM bytes and respect read-only grants", async ({
  page,
}) => {
  let sent: Record<string, unknown> | undefined
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.action === "kv_edit") sent = command
    await route.fulfill({
      json: {
        result:
          command.action === "kv_edit"
            ? { ok: true }
            : {
                found: true,
                revision: "1",
                value: [239, 187, 191, 65],
                metadata: null,
                expiration: null,
                tag: "text",
              },
      },
    })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Exact key").fill("bom")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await page.getByRole("button", { name: "Edit value" }).click()
  await expect(
    page.getByRole("button", { name: "Review changes" })
  ).toBeDisabled()
  await page.getByLabel("Metadata (JSON)").fill("null")
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await expect
    .poll(() => sent)
    .toMatchObject({ edit: { value_base64: "77u/QQ==", metadata: "null" } })
  await page.getByRole("button", { name: "Sign out" }).click()
  await page.route("**/session", (route) =>
    route.fulfill({
      json: {
        user: "viewer",
        fleets: [
          {
            id: "main",
            name: "Commerce production",
            environment: "Production",
          },
        ],
        grants: { main: ["metadata_read", "data_read"] },
      },
    })
  )
  await page
    .getByLabel("Dashboard access token")
    .fill("read-only-test-fixture-token")
  await page.getByRole("button", { name: "Connect to fleet console" }).click()
  await page.getByLabel("Exact key").fill("bom")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await expect(page.getByRole("button", { name: "Edit value" })).toBeDisabled()
  await expect(
    page.getByRole("button", { name: "Delete key", exact: true })
  ).toBeDisabled()
  await expect(
    page.getByRole("button", { name: "New key", exact: true })
  ).toBeDisabled()
})

test("KV conflict recovery refuses an oversized latest value", async ({
  page,
}) => {
  let reads = 0
  let writes = 0
  await page.route("**/resources/action", async (route) => {
    const command = route.request().postDataJSON().command
    if (command.action === "kv_edit") {
      writes++
      return route.fulfill({
        status: 409,
        json: { error: "Another writer changed this key." },
      })
    }
    reads++
    return route.fulfill({
      json: {
        result:
          reads === 1
            ? {
                found: true,
                revision: "1",
                tag: "text",
                value: [65],
                metadata: null,
                expiration: null,
              }
            : {
                found: true,
                revision: "2",
                tag: "bytes",
                valueOmitted: true,
                size: 2000000,
                metadata: null,
                expiration: null,
              },
      },
    })
  })
  await page.getByRole("link", { name: "Key-value", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Exact key").fill("key")
  await page.getByRole("button", { name: "Read key", exact: true }).click()
  await page.getByRole("button", { name: "Edit value" }).click()
  await page.getByLabel("Value", { exact: true }).fill("my draft")
  await page.getByRole("button", { name: "Review changes" }).click()
  await page.getByRole("button", { name: "Save changes" }).click()
  await page.getByRole("button", { name: "Compare latest" }).click()
  await expect(
    page.getByRole("button", { name: "Use latest as baseline" })
  ).toBeDisabled()
  await expect(
    page.getByRole("button", { name: "Review changes" })
  ).toBeDisabled()
  await expect(page.getByLabel("Value", { exact: true })).toHaveText("my draft")
  await expect(page.locator(".kv-conflict")).toContainText("Value omitted")
  expect(writes).toBe(1)
})

test("queue details preserve binary payloads and expose lease and provenance snapshots", async ({
  page,
}) => {
  const reads: unknown[] = []
  await page.route("**/resources/action", async (route) => {
    reads.push(route.request().postDataJSON().command)
    await route.fulfill({
      json: {
        result: {
          observedAt: now,
          messages: [
            {
              id: "message-binary",
              bodyBase64: "/wAB",
              contentType: "bytes",
              size: 3,
              enqueuedAt: now - 120000,
              visibleAt: now - 1000,
              attempts: 2,
              deliveryAttempts: 3,
              state: "leased",
              leasedUntil: now + 30000,
              deadLetterSource: "source-queue",
              transferTarget: null,
              lease: { active: true, expiresAt: now + 30000, generation: 7 },
              provenance: {
                sourceQueue: "source-queue",
                transferId: "transfer-123",
                kind: null,
                targetQueue: null,
              },
            },
          ],
        },
      },
    })
  })
  await page.getByRole("link", { name: "Queues", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "Peek", exact: true }).click()
  await page
    .getByRole("button", { name: "message-binary", exact: true })
    .click()
  const panel = page.getByRole("dialog", { name: "Message details" })
  await expect(panel).toContainText("2.0 min")
  await expect(panel).toContainText("Active at observation")
  await expect(panel).toContainText("transfer-123")
  await expect(panel).toContainText("source-queue")
  await expect(panel).toContainText(
    "Earlier source-queue attempt history is not retained"
  )
  await panel.getByRole("tab", { name: "Base64", exact: true }).click()
  await expect(panel).toContainText("/wAB")
  const downloadEvent = page.waitForEvent("download")
  await panel.getByRole("button", { name: "Download bytes" }).click()
  const stream = await (await downloadEvent).createReadStream()
  const chunks: Buffer[] = []
  for await (const chunk of stream!) chunks.push(chunk)
  expect([...Buffer.concat(chunks)]).toEqual([255, 0, 1])
  expect(reads).toEqual([{ action: "queue", operation: "peek", limit: 100 }])
  await page.keyboard.press("Escape")
  await expect(panel).not.toBeVisible()
  await expect(
    page.getByRole("button", { name: "message-binary", exact: true })
  ).toBeFocused()
  await page.setViewportSize({ width: 390, height: 844 })
  await page
    .getByRole("button", { name: "message-binary", exact: true })
    .click()
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth
    )
  ).toBeTruthy()
  await panel.screenshot({ path: "test-results/queue-message-mobile.png" })
})

test("workflow pins, duration evidence, bounded search and comparisons stay tied to their runs", async ({
  page,
}) => {
  const definition = (version: string) => ({
    version,
    nodes: [
      {
        id: "prepare",
        label: version === "v1" ? "Prepare" : "Prepare updated",
        kind: "activity",
        step: "prepare",
      },
      ...(version === "v2"
        ? [{ id: "approve", label: "Approve", kind: "wait", step: "approval" }]
        : []),
    ],
    edges: version === "v2" ? [{ from: "prepare", to: "approve" }] : [],
  })
  const event = (
    id: number,
    type: string,
    generation: string,
    fields = {}
  ) => ({
    eventId: id,
    timestamp: now + id * 100,
    instanceId: "order-42",
    generation,
    type,
    ...fields,
  })
  const events = [
    event(1, "step_started", "run-1", {
      stepName: "prepare",
      count: 1,
      input: { items: 3 },
    }),
    event(2, "step_completed", "run-1", {
      stepName: "prepare",
      count: 1,
      output: { receipt: "first" },
    }),
    event(3, "step_started", "run-2", {
      stepName: "prepare",
      count: 2,
      input: "[REDACTED]",
    }),
    event(4, "step_started", "run-2", {
      stepName: "approval",
      durationMs: 60000,
    }),
  ]
  await page.route("**/resources/action", async (route) => {
    const body = route.request().postDataJSON().command,
      command = body.command
    if (command.operation === "history_search")
      return route.fulfill({
        json: {
          result: {
            instanceId: body.instance,
            scope: `__Workflow:${body.instance}`,
            events: command.cursor ? [events[2]] : [],
            scanned: 64,
            next_cursor: command.cursor ? null : 64,
            at_end: !!command.cursor,
            done: false,
          },
        },
      })
    await route.fulfill({
      json: {
        result: {
          instanceId: body.instance,
          scope: `__Workflow:${body.instance}`,
          cursor: command.cursor,
          done: true,
          events:
            body.instance === "other"
              ? [
                  { ...events[0], instanceId: "other", input: { other: true } },
                  {
                    ...events[1],
                    instanceId: "other",
                    output: { receipt: "other" },
                  },
                ]
              : events,
          definitions: {
            "run-1": { definition: definition("v1"), revision: "one" },
            "run-2": { definition: definition("v2"), revision: "two" },
          },
        },
      },
    })
  })
  await page.getByRole("link", { name: "Workflows", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByLabel("Instance ID").fill("order-42")
  await page.getByRole("button", { name: "Load history", exact: true }).click()
  const explorer = page.getByLabel("Workflow execution explorer").first(),
    inspector = explorer.getByLabel("Step inspector")
  await explorer
    .getByLabel("Execution generation", { exact: true })
    .selectOption("run-1")
  await explorer.getByRole("button", { name: /prepare step completed/ }).click()
  await expect(inspector).toContainText("100 ms")
  await explorer.getByRole("button", { name: "Pin step details" }).click()
  await explorer
    .getByLabel("Execution generation", { exact: true })
    .selectOption("run-2")
  await explorer.getByRole("button", { name: /approval step started/ }).click()
  await expect(inspector).toContainText('"receipt": "first"')
  await expect(inspector).toContainText("run-1")
  await explorer.getByRole("button", { name: "Unpin details" }).click()
  await expect(inspector).toContainText("Duration not recorded")
  await expect(inspector).not.toContainText("1.0 min")
  await explorer
    .getByLabel("Search retained history", { exact: true })
    .fill("secret")
  await explorer
    .getByRole("button", { name: "Search history", exact: true })
    .click()
  await expect(explorer).toContainText("No matches in this scanned page")
  await explorer
    .getByRole("button", { name: "Search next history page" })
    .click()
  await explorer.getByRole("button", { name: "#3 · prepare" }).click()
  await expect(inspector).toContainText("[REDACTED]")
  await explorer.getByRole("tab", { name: "Compare runs and versions" }).click()
  const comparison = page.getByLabel("Workflow comparison", { exact: true })
  await comparison.getByLabel("Left execution generation").selectOption("run-1")
  await comparison
    .getByLabel("Right execution generation")
    .selectOption("run-2")
  await comparison
    .getByRole("tab", { name: "Definition comparison", exact: true })
    .click()
  await expect(comparison.getByLabel("Definition changes")).toContainText(
    "1 added nodes"
  )
  await expect(comparison.getByLabel("Definition changes")).toContainText(
    "1 changed nodes"
  )
  await expect(
    comparison.getByLabel("Left definition comparison")
  ).toContainText("v1")
  await expect(
    comparison.getByLabel("Right definition comparison")
  ).toContainText("v2")
  await comparison.getByLabel("Comparison instance").fill("other")
  await comparison.getByRole("button", { name: "Load comparison run" }).click()
  await expect(comparison).toContainText("Right · other")
  await comparison
    .getByRole("tab", { name: "Run comparison", exact: true })
    .click()
  await expect(comparison.getByLabel("Right run comparison")).toContainText(
    '"other": true'
  )
  await expect(comparison.getByLabel("Left run comparison")).not.toContainText(
    '"other": true'
  )
  await page.setViewportSize({ width: 1440, height: 1000 })
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth
    )
  ).toBeTruthy()
  await comparison.screenshot({ path: "test-results/workflow-comparison.png" })
  await page.getByRole("radio", { name: "Dark theme", exact: true }).click()
  await comparison.screenshot({
    path: "test-results/workflow-comparison-dark.png",
  })
  await page.setViewportSize({ width: 390, height: 844 })
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth
    )
  ).toBeTruthy()
})

test("cron execution history keeps adoption, acceptance and interruption distinct", async ({
  page,
}) => {
  const queries: string[] = []
  const configuration = {
    application: "commerce-api",
    prefix: "deploy/current",
    deployment_revision: "deploy-current",
    revision: "cron-current",
    expressions: ["*/5 * * * *"],
    primary: true,
  }
  await page.route("**/applications/commerce-api/cron**", async (route) => {
    const url = new URL(route.request().url())
    if (url.pathname.endsWith("/nodes"))
      return route.fulfill({
        json: pageOf([
          {
            node: nodes[0].id,
            status: "adopted",
            observed_revision: "cron-current",
            observed_at: now,
          },
        ]),
      })
    if (!url.pathname.endsWith("/executions"))
      return route.fulfill({ json: configuration })
    queries.push(url.search)
    await route.fulfill({
      json: {
        items: url.searchParams.has("before")
          ? []
          : [
              {
                id: 3,
                expression: "*/5 * * * *",
                scheduled_at: now - 1000,
                started_at: now - 500,
                ended_at: null,
                duration_ms: null,
                status: "started",
                attempt: 1,
                target: "application",
                node: "n1",
                runtime_generation: 2,
                retry_requested: false,
                configuration: {
                  prefix: "deploy/current",
                  deployment_version: "v2",
                  schedule_revision: "cron-current",
                },
              },
              {
                id: 2,
                expression: "0 * * * *",
                scheduled_at: now - 10000,
                started_at: now - 9000,
                ended_at: now - 8000,
                duration_ms: 1000,
                status: "workflow_accepted",
                attempt: 1,
                target: "workflow",
                workflow_name: "Checkout",
                workflow_binding: "CHECKOUT",
                workflow_instance: "cron-instance",
                node: "n1",
                runtime_generation: 1,
                retry_requested: false,
                configuration: {
                  prefix: "deploy/old",
                  deployment_version: "v1",
                  schedule_revision: null,
                },
              },
              {
                id: 1,
                expression: "0 * * * *",
                scheduled_at: now - 20000,
                started_at: now - 19000,
                ended_at: now - 18000,
                duration_ms: 1000,
                status: "failed",
                attempt: 2,
                target: "application",
                node: "n1",
                runtime_generation: 1,
                retry_requested: true,
                error: "handler failed",
                background_errors: 1,
              },
            ],
        next_cursor: url.searchParams.has("before") ? null : "1",
        observed_at: now,
        retention_limit: 256,
        active_alarm: now + 30000,
      },
    })
  })
  await page.getByRole("link", { name: "Cron schedules", exact: true }).click()
  await page.getByRole("link", { name: "Open workspace" }).click()
  await page.getByRole("button", { name: "Load execution history" }).click()
  await expect(page.locator("main")).toContainText("Completion not recorded")
  await expect(page.locator("main")).toContainText("Workflow accepted")
  await expect(page.locator("main")).toContainText("Current configuration")
  await expect(page.locator("main")).toContainText(
    "Earlier or unrecorded configuration"
  )
  await page
    .getByRole("button", { name: "0 * * * *", exact: true })
    .last()
    .click()
  const panel = page.getByRole("dialog", { name: "Schedule execution" })
  await expect(panel).toContainText("handler failed")
  await expect(panel).toContainText("Yes; inspect later attempts")
  await page.keyboard.press("Escape")
  await page
    .getByRole("button", { name: "0 * * * *", exact: true })
    .first()
    .click()
  await expect(
    panel.getByRole("link", { name: "Inspect workflow execution →" })
  ).toHaveAttribute("href", /\?instance=cron-instance$/)
  await page.keyboard.press("Escape")
  await page.getByRole("button", { name: "Earlier executions" }).click()
  await expect(page.locator("main")).toContainText("No recorded attempts")
  await expect(page.locator("main")).toContainText(
    "Empty history is not evidence"
  )
  expect(queries).toEqual(["?limit=50", "?limit=50&before=1"])
})
