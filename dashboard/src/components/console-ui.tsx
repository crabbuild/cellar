import type { ReactNode } from "react"
import {
  AlertCircle,
  ArrowRight,
  CircleCheck,
  Clock3,
  Copy,
  Inbox,
  RefreshCw,
} from "lucide-react"
import { toast } from "sonner"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty"
import { Skeleton } from "@/components/ui/skeleton"
import { date, type Page } from "@/lib/api"

export function PageHeading({
  title,
  description,
  children,
}: {
  title: string
  description: string
  children?: ReactNode
}) {
  return (
    <div className="page-heading">
      <div>
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      <div className="flex items-center gap-2">{children}</div>
    </div>
  )
}

export function Feedback({
  loading,
  error,
  children,
}: {
  loading?: boolean
  error?: Error | null
  children: ReactNode
}) {
  if (loading)
    return (
      <div className="flex flex-col gap-4" role="status" aria-label="Loading">
        <Skeleton className="h-12 w-1/3" />
        <Skeleton className="h-64 w-full" />
      </div>
    )
  return (
    <>
      {error ? (
        <Alert variant="destructive">
          <AlertCircle />
          <AlertTitle>Could not refresh this view</AlertTitle>
          <AlertDescription>
            {error.message} Any data below is the last successful observation.
          </AlertDescription>
        </Alert>
      ) : null}
      {children}
    </>
  )
}

export function Nothing({
  title = "Nothing here yet",
  description,
}: {
  title?: string
  description: string
}) {
  return (
    <Empty>
      <EmptyHeader>
        <EmptyMedia variant="icon">
          <Inbox />
        </EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription>{description}</EmptyDescription>
      </EmptyHeader>
    </Empty>
  )
}

export function Status({
  label,
  tone = "neutral",
}: {
  label: string
  tone?: "good" | "warning" | "neutral" | "error"
}) {
  const Icon =
    tone === "good"
      ? CircleCheck
      : tone === "warning" || tone === "error"
        ? AlertCircle
        : Clock3
  return (
    <Badge
      variant={
        tone === "error"
          ? "destructive"
          : tone === "good"
            ? "success"
            : tone === "warning"
              ? "warning"
              : "secondary"
      }
    >
      <Icon data-icon="inline-start" aria-hidden />
      {label}
    </Badge>
  )
}

export function Refresh({
  onClick,
  busy,
}: {
  onClick: () => void
  busy?: boolean
}) {
  return (
    <Button variant="outline" disabled={busy} onClick={onClick}>
      <RefreshCw
        data-icon="inline-start"
        className={busy ? "animate-spin" : undefined}
      />
      Refresh
    </Button>
  )
}

export function Coverage<T>({ page }: { page?: Page<T> }) {
  if (!page) return null
  return (
    <div className="observation">
      <span>
        <Clock3 aria-hidden />
        Observed {date(page.observed_at)}
      </span>
      <span>
        {page.items.length} records ·{" "}
        {page.coverage === "complete"
          ? "Complete page set"
          : "Partial inventory"}
      </span>
      {page.warnings.map((w) => (
        <span key={w}>{w}</span>
      ))}
    </div>
  )
}

export function NextPage({
  cursor,
  onNext,
}: {
  cursor?: string | null
  onNext: (value: string) => void
}) {
  return cursor ? (
    <Button variant="outline" onClick={() => onNext(cursor)}>
      Next page
      <ArrowRight data-icon="inline-end" />
    </Button>
  ) : null
}

export function CopyValue({ value }: { value: string }) {
  return (
    <Button
      variant="ghost"
      size="icon-sm"
      aria-label={`Copy ${value}`}
      onClick={() => {
        void navigator.clipboard.writeText(value).then(
          () => toast.success("Copied to clipboard"),
          () => toast.error("Clipboard access was denied")
        )
      }}
    >
      <Copy />
    </Button>
  )
}

export function JsonView({ value }: { value: unknown }) {
  return (
    <pre className="json-view" tabIndex={0}>
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}
