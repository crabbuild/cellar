// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import { Search } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Feedback, Status } from "@/components/console-ui"
import { resourceRequest, type ResourceTarget } from "@/lib/resource"
import { useSession } from "@/lib/session"
import { date } from "@/lib/api"
import type { WorkflowEvent } from "@/components/workflow-visual"

type SearchPage = {
  instanceId: string
  scope?: string
  events: WorkflowEvent[]
  next_cursor: number | null
  scanned: number
  at_end: boolean
  done: boolean
}
export function WorkflowHistorySearch({
  instance,
  scope,
  target,
  onPin,
}: {
  instance: string
  scope?: string
  target: ResourceTarget
  onPin: (event: WorkflowEvent) => void
}) {
  const { token } = useSession()
  const [query, setQuery] = useState("")
  const search = useMutation({
    mutationFn: async ({
      query,
      cursor,
    }: {
      query: string
      cursor: number
    }) => {
      const reply = await resourceRequest(token, target, {
        action: "workflow",
        instance,
        command: { operation: "history_search", query, cursor },
      })
      const page = reply.result as SearchPage
      if (!Array.isArray(page?.events) || typeof page.scanned !== "number")
        throw new Error("This owner did not return searchable history.")
      if (page.instanceId !== instance || (scope && page.scope !== scope))
        throw new Error(
          "This search belongs to another resource. Reload history before searching again."
        )
      return { query, page }
    },
    retry: false,
  })
  return (
    <section
      className="flex flex-col gap-3"
      aria-label="Search retained workflow history"
    >
      <form
        onSubmit={(event) => {
          event.preventDefault()
          search.mutate({ query: query.trim(), cursor: 0 })
        }}
      >
        <FieldGroup className="inspection-filter-row">
          <Field>
            <FieldLabel htmlFor="workflow-history-search">
              Search retained history
            </FieldLabel>
            <Input
              id="workflow-history-search"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Step, event, input, output or error…"
              maxLength={256}
            />
          </Field>
          <Button
            variant="outline"
            disabled={!query.trim() || search.isPending}
          >
            <Search data-icon="inline-start" />
            Search history
          </Button>
        </FieldGroup>
      </form>
      <Feedback loading={search.isPending} error={search.error}>
        {search.data ? (
          <>
            <p className="workspace-caption">
              {search.data.page.events.length} matches for “{search.data.query}”
              · {search.data.page.scanned} events scanned on this page
              {search.data.page.at_end
                ? " · end of retained history at observation"
                : " · more history can be searched"}
              . Search covers recorded data, including redacted placeholders.
            </p>
            <ol className="workflow-search-results">
              {search.data.page.events.map((event) => (
                <li key={event.eventId}>
                  <Button variant="outline" onClick={() => onPin(event)}>
                    <span className="break-all">
                      #{event.eventId} ·{" "}
                      {event.stepName ?? event.type.replaceAll("_", " ")}
                    </span>
                  </Button>
                  <Status label={event.type.replaceAll("_", " ")} />
                  <small>
                    {date(event.timestamp)} ·{" "}
                    {event.generation ?? "Legacy generation"}
                  </small>
                </li>
              ))}
            </ol>
            {!search.data.page.events.length ? (
              <p className="workspace-caption">
                No matches in this scanned page.
              </p>
            ) : null}
            {search.data.page.next_cursor != null ? (
              <Button
                className="w-fit"
                variant="outline"
                disabled={search.isPending}
                onClick={() =>
                  search.mutate({
                    query: search.data!.query,
                    cursor: search.data!.page.next_cursor!,
                  })
                }
              >
                Search next history page
              </Button>
            ) : null}
          </>
        ) : null}
      </Feedback>
    </section>
  )
}
