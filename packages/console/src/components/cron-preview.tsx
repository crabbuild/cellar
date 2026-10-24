import { useState } from "react"
import { useMutation } from "@tanstack/react-query"
import { Clock3 } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { fleetPath, request } from "@/lib/api"
import { useSession } from "@/lib/session"

export function CronPreview({ fleet }: { fleet: string }) {
  const { token } = useSession()
  const [expression, setExpression] = useState("*/5 * * * *")
  const preview = useMutation({
    mutationFn: () =>
      request<{
        occurrences: number[]
        expression: string
        satisfiable: boolean
      }>(token, fleetPath(fleet, "/cron/preview"), {
        method: "POST",
        body: JSON.stringify({ expression }),
      }),
  })
  return (
    <Card>
      <CardHeader>
        <CardTitle>Preview a schedule</CardTitle>
        <CardDescription>
          Uses Cellar’s five-field cron parser. Times are UTC; weekdays are 1–7,
          Sunday–Saturday. Previewing does not change the deployment.
        </CardDescription>
      </CardHeader>
      <CardContent>
        <form
          className="space-y-4"
          onSubmit={(e) => {
            e.preventDefault()
            preview.mutate()
          }}
        >
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="cron-expression">Cron expression</FieldLabel>
              <Input
                id="cron-expression"
                className="font-mono"
                value={expression}
                maxLength={256}
                onChange={(e) => setExpression(e.target.value)}
              />
            </Field>
            <Button
              className="w-fit"
              disabled={!expression || preview.isPending}
            >
              <Clock3 data-icon="inline-start" />
              Preview next occurrences
            </Button>
          </FieldGroup>
          {preview.error ? (
            <Alert variant="destructive">
              <AlertTitle>Schedule could not be evaluated</AlertTitle>
              <AlertDescription>{preview.error.message}</AlertDescription>
            </Alert>
          ) : null}
          {preview.data ? (
            <div className="rounded-lg border p-4">
              <p className="mb-3 text-sm font-medium">
                Next occurrences · <code>{preview.data.expression}</code>
              </p>
              {preview.data.satisfiable ? (
                <ol className="space-y-2 font-mono text-xs">
                  {preview.data.occurrences.map((time) => (
                    <li key={time}>
                      {new Date(time)
                        .toISOString()
                        .replace("T", " ")
                        .replace(".000Z", " UTC")}
                    </li>
                  ))}
                </ol>
              ) : (
                <p className="text-sm text-muted-foreground">
                  This expression has no occurrence in the complete Gregorian
                  calendar cycle.
                </p>
              )}
            </div>
          ) : null}
        </form>
      </CardContent>
    </Card>
  )
}
