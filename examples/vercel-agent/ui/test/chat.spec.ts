import { test, expect, type Page } from "@playwright/test";

async function setup(page: Page) {
  const requests: {
    id: string;
    prompt: string;
    previous: string | null;
    updatedAt?: number;
  }[] = [];
  let done = false,
    rejectNext = false;
  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/api/config")
      return route.fulfill({ json: { provider: "fixture", model: "ui-test" } });
    if (path === "/api/restart")
      return route.fulfill({ json: { restarted: true } });
    if (path === "/api/conversations") {
      const parents = new Set(requests.map((item) => item.previous));
      return route.fulfill({
        json: {
          chats: requests
            .filter((item) => !parents.has(item.id))
            .map((item) => ({
              task: item.id,
              title: item.prompt.slice(0, 48),
              prompt: item.prompt,
              previous: item.previous,
              updatedAt: item.updatedAt ?? 1000,
            })),
        },
      });
    }
    if (path.endsWith("/events"))
      return route.fulfill({
        contentType: "text/event-stream",
        body: 'event: progress\ndata: {"type":"text","attempt":1,"step":0,"text":"A **live draft**"}\n\n',
      });
    const id = path.split("/").at(-1)!;
    if (request.method() === "POST") {
      const body = request.postDataJSON();
      requests.push({ id, ...body });
      if (rejectNext) {
        rejectNext = false;
        return route.fulfill({
          status: 503,
          json: { error: "Worker is recovering" },
        });
      }
      return route.fulfill({
        status: done ? 200 : 202,
        json: { status: done ? "completed" : "pending" },
      });
    }
    const saved = requests.find((value) => value.id === id);
    if (!saved)
      return route.fulfill({ status: 404, json: { error: "Task not found" } });
    const messages = [
      { role: "user", content: saved.prompt },
      ...(done
        ? [
            {
              role: "assistant",
              content: [
                {
                  type: "text",
                  text: '## Verified result\n\nThe answer is **168**.\n\n| Operation | Result |\n| --- | --- |\n| Multiplication | 168 |\n\n<script>alert("unsafe")</script>',
                },
              ],
            },
          ]
        : []),
    ];
    return route.fulfill({
      json: {
        status: done ? "completed" : "pending",
        prompt: saved.prompt,
        previous: saved.previous,
        updatedAt: saved.updatedAt ?? 1000,
        attempts: 1,
        modelSteps: done ? 1 : 0,
        toolReceipts: 0,
        count: 0,
        receipts: [],
        messages,
        usage: done
          ? [{ inputTokens: { total: 10 }, outputTokens: { total: 20 } }]
          : [],
      },
    });
  });
  return {
    requests,
    complete: () => {
      done = true;
    },
    rejectNext: () => {
      rejectNext = true;
    },
  };
}

test("conversation order follows saved updates, not selection or polling", async ({
  page,
}, info) => {
  const server = await setup(page);
  server.requests.push(
    {
      id: "newer",
      prompt: "Newer conversation",
      previous: null,
      updatedAt: 2000,
    },
    {
      id: "older",
      prompt: "Older conversation",
      previous: null,
      updatedAt: 1000,
    },
  );
  server.complete();
  await page.clock.install();
  await page.goto("/");
  const navigation = page.getByRole("navigation", { name: "Conversations" });
  const open = async () => {
    if (info.project.name === "mobile")
      await page.getByRole("button", { name: "Open conversations" }).click();
  };
  await open();
  await expect(navigation.getByRole("button")).toHaveText([
    "Newer conversation",
    "Older conversation",
  ]);
  await navigation.getByRole("button", { name: "Older conversation" }).click();
  await expect(page).toHaveURL(/task=older/);
  await expect(
    page.getByRole("heading", { name: "Verified result" }),
  ).toBeVisible();
  await open();
  await expect(navigation.getByRole("button")).toHaveText([
    "Newer conversation",
    "Older conversation",
  ]);
  await page.clock.fastForward(6000);
  await expect(navigation.getByRole("button")).toHaveText([
    "Newer conversation",
    "Older conversation",
  ]);
  // A real saved update reorders the list even if the server array is unsorted.
  server.requests[1].updatedAt = 3000;
  await page.clock.fastForward(6000);
  await expect(navigation.getByRole("button")).toHaveText([
    "Older conversation",
    "Newer conversation",
  ]);
  await page.reload();
  await open();
  await expect(navigation.getByRole("button")).toHaveText([
    "Older conversation",
    "Newer conversation",
  ]);
  expect(server.requests).toHaveLength(2);
});

test("server conversation history restores without browser storage or a turn URL", async ({
  page,
}, info) => {
  const server = await setup(page);
  server.requests.push({
    id: "restored-turn",
    prompt: "Saved conversation from the bucket",
    previous: null,
  });
  server.complete();
  await page.goto("/?task=restored-turn");
  await expect(
    page.getByRole("heading", { name: "Verified result" }),
  ).toBeVisible();
  if (info.project.name === "mobile")
    await page.getByRole("button", { name: "Open conversations" }).click();
  await expect(
    page
      .getByRole("navigation", { name: "Conversations" })
      .getByRole("button", { name: "Saved conversation from the bucket" }),
  ).toBeVisible();
  await page.evaluate(() => localStorage.clear());
  await page.goto("/");
  if (info.project.name === "mobile")
    await page.getByRole("button", { name: "Open conversations" }).click();
  await page
    .getByRole("navigation", { name: "Conversations" })
    .getByRole("button", { name: "Saved conversation from the bucket" })
    .click();
  await expect(page).toHaveURL(/task=restored-turn/);
  await expect(
    page.getByRole("heading", { name: "Verified result" }),
  ).toBeVisible();
  expect(server.requests).toHaveLength(1);
});

test("streamed Markdown, durable completion, reload, and follow-up preserve turn identity", async ({
  page,
}) => {
  const server = await setup(page);
  await page.goto("/");
  const input = page.getByRole("textbox", { name: "Message Cellar" });
  await expect(
    page.getByRole("button", { name: "Send message", exact: true }),
  ).toBeDisabled();
  await input.fill("Calculate 24 times 7");
  await input.press("Enter");
  await expect(page.getByText("A live draft")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Send message", exact: true }),
  ).toBeDisabled();
  const id = server.requests[0].id;
  await expect(page).toHaveURL(new RegExp(`task=${id}`));
  server.complete();
  await expect(
    page.getByRole("heading", { name: "Verified result" }),
  ).toBeVisible();
  await expect(page.getByText("Answer and tool results saved")).toBeVisible();
  await expect(
    page.getByRole("region", { name: "Scrollable results table" }),
  ).toHaveAttribute("tabindex", "0");
  await expect(page.locator(".markdown script")).toHaveCount(0);
  await page.reload();
  await expect(
    page.getByRole("heading", { name: "Verified result" }),
  ).toBeVisible();
  expect(server.requests).toHaveLength(1);
  await input.fill("What was the answer?");
  await input.press("Enter");
  await expect.poll(() => server.requests.length).toBe(2);
  expect(server.requests[1].id).not.toBe(id);
  expect(server.requests[1].previous).toBe(id);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});

test("failed submission retries the same durable ID and prompt", async ({
  page,
}) => {
  const server = await setup(page);
  server.rejectNext();
  await page.goto("/");
  await page
    .getByRole("textbox", { name: "Message Cellar" })
    .fill("Keep this request");
  await page.getByRole("button", { name: "Send message", exact: true }).click();
  await page.getByRole("button", { name: "Retry same turn" }).click();
  await expect.poll(() => server.requests.length).toBe(2);
  expect(server.requests[1]).toEqual(server.requests[0]);
  server.complete();
  await expect(
    page.getByRole("heading", { name: "Verified result" }),
  ).toBeVisible();
});

test("responsive navigation, saved-work sheet, keyboard composer, and short viewports remain usable", async ({
  page,
}, info) => {
  await setup(page);
  await page.goto("/");
  if (info.project.name === "mobile") {
    await page.getByRole("button", { name: "Open conversations" }).click();
    await expect(page.getByRole("dialog")).toBeVisible();
    await page
      .getByRole("dialog")
      .getByRole("button", { name: "New chat" })
      .click();
    await expect(page.getByRole("dialog")).not.toBeVisible();
    await page.getByRole("button", { name: "Open saved work" }).click();
    await expect(
      page
        .getByRole("dialog")
        .getByRole("heading", { name: "Saved work", exact: true })
        .first(),
    ).toBeVisible();
    await page.getByRole("button", { name: "Close", exact: true }).click();
  }
  const input = page.getByRole("textbox", { name: "Message Cellar" });
  await input.fill("Line one");
  await input.press("Shift+Enter");
  await input.press("b");
  await expect(input).toHaveValue("Line one\nb");
  await page.setViewportSize({
    width: info.project.name === "mobile" ? 390 : 1280,
    height: 577,
  });
  const bounds = await input.boundingBox();
  expect(bounds!.y + bounds!.height).toBeLessThanOrEqual(577);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
});
