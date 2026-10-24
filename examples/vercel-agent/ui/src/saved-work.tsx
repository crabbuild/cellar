import { cn } from "cn";
import { Check, RotateCcw, ShieldCheck, Wrench } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Spinner } from "@/components/ui/spinner";
import { names } from "./transcript";
import type { Task, Progress } from "./use-task";

export function SavedWork({
  task,
  state,
  progress,
  restarting,
  restart,
}: {
  task: string | null;
  state: Task | null;
  progress: Progress | null;
  restarting: boolean;
  restart: () => Promise<void>;
}) {
  const active = Boolean(task && (!state || state.status === "pending"));
  const recovered = (state?.attempts ?? 0) > 1;
  return (
    <div className="ledger-content">
      <div className="ledger-heading">
        <ShieldCheck aria-hidden="true" />
        <h2>Saved work</h2>
      </div>
      <p className="muted">A record of what this turn has done.</p>
      <div
        className="save-status"
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        <span className={cn("status-orbit", active && "working")} />
        <div>
          <strong>
            {!task
              ? "Ready when you are"
              : state?.status === "completed"
                ? "All work saved"
                : state?.status === "failed"
                  ? "Saved for inspection"
                  : recovered
                    ? "Resuming saved work"
                    : "Saving as work happens"}
          </strong>
          <p>
            {!task
              ? "Start a conversation to begin."
              : state?.status === "completed"
                ? "Safe to close this tab."
                : "You can return to this link."}
          </p>
        </div>
      </div>
      <Separator />
      <dl className="run-facts">
        <div>
          <dt>Attempts</dt>
          <dd>{state?.attempts ?? "—"}</dd>
        </div>
        <div>
          <dt>Model checkpoints</dt>
          <dd>{state?.modelSteps ?? "—"}</dd>
        </div>
        <div>
          <dt>Saved tool results</dt>
          <dd>{state?.toolReceipts ?? "—"}</dd>
        </div>
        <div>
          <dt>Output tokens</dt>
          <dd>
            {state?.usage.reduce(
              (sum, usage) => sum + (usage.outputTokens.total ?? 0),
              0,
            ) ?? "—"}
          </dd>
        </div>
      </dl>
      <Separator />
      <div className="nav-label">Tool activity</div>
      {state?.receipts.length ? (
        <ol className="receipt-list">
          {state.receipts.map((receipt, index) => {
            const { label, icon: Icon } = names[receipt.name] ?? {
              label: receipt.name,
              icon: Wrench,
            };
            return (
              <li key={index}>
                <div className="receipt-dot">
                  <Check aria-hidden="true" />
                </div>
                <div>
                  <strong>
                    <Icon aria-hidden="true" />
                    {label}
                  </strong>
                  <p>Result durably saved</p>
                </div>
              </li>
            );
          })}
        </ol>
      ) : (
        <p className="muted">Tool results appear here after they are saved.</p>
      )}
      {active && progress?.replayed ? (
        <Badge variant="secondary">
          <RotateCcw data-icon="inline-start" />
          Reusing saved work
        </Badge>
      ) : null}
      <div className="recovery-test">
        <h3>Try a recovery</h3>
        <p>
          Restart the worker to see pending work resume from its last saved
          checkpoint.
        </p>
        <Button
          variant="outline"
          className="w-full"
          disabled={restarting}
          onClick={() => void restart()}
        >
          {restarting ? (
            <Spinner data-icon="inline-start" />
          ) : (
            <RotateCcw data-icon="inline-start" />
          )}
          {restarting ? "Recovering…" : "Restart worker"}
        </Button>
      </div>
      {task ? (
        <p className="task-id" title={task}>
          Turn {task}
        </p>
      ) : null}
    </div>
  );
}
