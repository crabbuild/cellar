import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Field, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Progress } from "@/components/ui/progress"
import { Feedback, JsonView, Refresh, Status } from "@/components/console-ui"
import { EventNotice } from "@/components/event-ui"
import {
  useEvent,
  exact,
  age,
  eventTime,
  type Subscription,
} from "@/lib/events"

export function SubscriptionDetails({
  path,
  resource,
  selected,
}: {
  path: string
  resource: string
  selected: Subscription
}) {
  const [name, setName] = useState(selected.identity.name)
  const [lookup, setLookup] = useState(selected.identity.name)
  const data = useEvent<Subscription>(
    path,
    { kind: "subscription", name: lookup },
    resource
  )
  const sub = data.data
  const first = BigInt(sub?.earliest ?? "1"),
    end = BigInt(sub?.end ?? "1"),
    checkpoint = BigInt(sub?.checkpoint ?? "0") + 1n
  const retained = end - first
  const consumed =
    checkpoint < first ? 0n : checkpoint > end ? retained : checkpoint - first
  const progress = retained > 0n ? Number((consumed * 100n) / retained) : 100
  return (
    <div className="flex flex-col gap-4 p-4">
      <form
        className="flex flex-wrap items-end gap-3"
        onSubmit={(e) => {
          e.preventDefault()
          setLookup(name.trim())
        }}
      >
        <Field className="min-w-0 flex-[1_1_12rem]">
          <FieldLabel htmlFor="subscription-name">Subscription name</FieldLabel>
          <Input
            id="subscription-name"
            value={name}
            onChange={(e) => setName(e.target.value)}
            maxLength={128}
          />
        </Field>
        <Button variant="outline" type="submit" disabled={!name.trim()}>
          Inspect subscription
        </Button>
      </form>
      <Feedback loading={data.isPending} error={data.error}>
        {sub && (
          <>
            {lookup === selected.identity.name &&
              sub.identity.id !== selected.identity.id && (
                <EventNotice>
                  This name now resolves to a different subscription
                  incarnation. The selected inventory row is retained history;
                  refresh the inventory before managing the current incarnation.
                </EventNotice>
              )}
            <Status label={sub.state} />
            <div className="flex flex-col gap-2">
              <p className="text-sm">
                Retained delivery progress · {progress}%
              </p>
              <Progress
                value={progress}
                aria-valuenow={progress}
                aria-label="Retained delivery progress"
              />
              <p className="text-xs text-muted-foreground">
                Checkpoint {exact(sub.checkpoint)} · retained range{" "}
                {exact(sub.earliest)} to {exact(sub.end)} (exclusive)
              </p>
            </div>
            {sub.observation.history_expired && (
              <EventNotice>
                The checkpoint precedes retained history. Logical lag includes
                expired events that cannot be replayed from this stream.
              </EventNotice>
            )}
            <dl className="detail-grid">
              <dt>Logical unread</dt>
              <dd>{exact(sub.observation.lag_records)}</dd>
              <dt>Retained unread</dt>
              <dd>{exact(sub.observation.retained_lag_records)}</dd>
              <dt>Oldest retained unread</dt>
              <dd>{age(sub.observation.oldest_retained_age_ms)}</dd>
              <dt>Retention protection</dt>
              <dd>{sub.required ? "Required" : "Optional"}</dd>
              <dt>Lease</dt>
              <dd>
                {sub.observation.lease_active
                  ? "Active"
                  : sub.lease
                    ? "Expired observation"
                    : "None"}
              </dd>
              {sub.lease && (
                <>
                  <dt>Lease range</dt>
                  <dd>
                    {exact(sub.lease.first)}–{exact(sub.lease.last)}
                  </dd>
                  <dt>Lease expiry</dt>
                  <dd>{eventTime(sub.lease.expires_ms)}</dd>
                </>
              )}
              <dt>Retry eligible</dt>
              <dd>
                {sub.retry_at_ms === "0" ? "Now" : eventTime(sub.retry_at_ms)}
              </dd>
              <dt>Observed</dt>
              <dd>{eventTime(sub.observation.observed_ms)}</dd>
            </dl>
            <Refresh
              busy={data.isFetching}
              onClick={() => void data.refetch()}
            />
            <JsonView value={sub} />
          </>
        )}
      </Feedback>
    </div>
  )
}
