import { lazy, memo, Suspense } from "react";
import { cn } from "cn";
import {
  BookOpen,
  Calculator,
  ChevronDown,
  Database,
  FileText,
  Layers3,
  Ticket,
  Wrench,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import type { Message } from "./use-task";

export const names: Record<string, { label: string; icon: typeof Wrench }> = {
  increment: { label: "Update counter", icon: Database },
  calculate: { label: "Calculate", icon: Calculator },
  lookup: { label: "Look up reference", icon: BookOpen },
  saveNote: { label: "Save note", icon: FileText },
  readState: { label: "Read saved state", icon: Database },
  createTicket: { label: "Create ticket", icon: Ticket },
};
export const textOf = (message: Message) =>
  typeof message.content === "string"
    ? message.content
    : message.content
        .filter((part) => part.type === "text")
        .map((part) => part.text ?? "")
        .join("");
const MarkdownRenderer = lazy(() => import("./Markdown"));
export function Markdown({
  text,
  streaming = false,
}: {
  text: string;
  streaming?: boolean;
}) {
  return (
    <Suspense fallback={<p className="user-text">{text}</p>}>
      <MarkdownRenderer text={text} streaming={streaming} />
    </Suspense>
  );
}

function ToolCall({
  name,
  input,
  output,
}: {
  name: string;
  input: unknown;
  output: unknown;
}) {
  const { label, icon: Icon } = names[name] ?? { label: name, icon: Wrench };
  return (
    <details className="tool-call">
      <summary>
        <Icon aria-hidden="true" />
        <span>{label}</span>
        <Badge variant="secondary">Saved</Badge>
        <ChevronDown aria-hidden="true" />
      </summary>
      <div className="tool-detail">
        <p>Input</p>
        <pre>{JSON.stringify(input, null, 2)}</pre>
        <p>Result</p>
        <pre>{JSON.stringify(output, null, 2)}</pre>
      </div>
    </details>
  );
}

export const Transcript = memo(function Transcript({
  messages,
}: {
  messages: Message[];
}) {
  const results = new Map(
    messages.flatMap((message) =>
      typeof message.content === "string"
        ? []
        : message.content
            .filter((part) => part.type === "tool-result")
            .map((part) => [part.toolCallId, part.output] as const),
    ),
  );
  return messages.map((message, index) => {
    if (message.role === "tool") return null;
    const text = textOf(message);
    const calls =
      typeof message.content === "string"
        ? []
        : message.content.filter((part) => part.type === "tool-call");
    if (!text && !calls.length) return null;
    return (
      <article
        key={index}
        className={cn(
          "message",
          message.role === "user" ? "user-message" : "assistant-message",
        )}
      >
        <div className="message-label">
          {message.role === "user" ? (
            "You"
          ) : (
            <>
              <Layers3 aria-hidden="true" /> Cellar
            </>
          )}
        </div>
        {text ? (
          message.role === "user" ? (
            <p className="user-text">{text}</p>
          ) : (
            <Markdown text={text} />
          )
        ) : null}
        {calls.map((part, call) => (
          <ToolCall
            key={part.toolCallId ?? call}
            name={part.toolName ?? "Tool"}
            input={part.input}
            output={results.get(part.toolCallId)}
          />
        ))}
      </article>
    );
  });
});
