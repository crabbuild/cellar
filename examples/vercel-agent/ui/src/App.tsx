// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { ChatNavigation, type Chat } from "./chat-navigation";
import { SavedWork } from "./saved-work";
import { Markdown, Transcript, textOf } from "./transcript";
import { useEffect, useRef, useState, type FormEvent } from "react";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpRight,
  Calculator,
  Check,
  CheckCheck,
  Copy,
  FileText,
  Layers3,
  Menu,
  PanelRight,
  ShieldCheck,
  Ticket,
  WifiOff,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertTitle, AlertDescription } from "@/components/ui/alert";
import {
  Empty,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
  EmptyDescription,
  EmptyContent,
} from "@/components/ui/empty";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupTextarea,
} from "@/components/ui/input-group";
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetDescription,
  SheetTrigger,
} from "@/components/ui/sheet";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Spinner } from "@/components/ui/spinner";
import { api, useTask, type Task } from "./use-task";
import { useConversations } from "./use-conversations";

const suggestions = [
  {
    icon: Calculator,
    title: "Work through the numbers",
    prompt:
      "Calculate 24 × 7, then add 13 to the counter once. Explain the two results.",
  },
  {
    icon: FileText,
    title: "Research and save a note",
    prompt:
      "Look up how Cellar durability works, then save a note titled Recovery with the key points. Summarize what you saved.",
  },
  {
    icon: Ticket,
    title: "Put a plan into action",
    prompt:
      "Create a normal-priority ticket titled Verify recovery. Then explain how its idempotency key prevents duplicate tickets.",
  },
];

export default function App() {
  const [task, setTask] = useState<string | null>(() => {
    const id = new URL(location.href).searchParams.get("task");
    return id && /^[a-zA-Z0-9_-]{1,64}$/.test(id) ? id : null;
  });
  const [seed, setSeed] = useState<Task | null>(null);
  const [draft, setDraft] = useState("");
  const [config, setConfig] = useState({ provider: "", model: "Connecting…" });
  const [sending, setSending] = useState(false);
  const [restarting, setRestarting] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const [revision, setRevision] = useState(0);
  const [atBottom, setAtBottom] = useState(true);
  const [menuOpen, setMenuOpen] = useState(false);
  const viewport = useRef<HTMLDivElement>(null);
  const input = useRef<HTMLTextAreaElement>(null);
  const { state, progress, connected, error } = useTask(task, seed, revision);
  const {
    chats,
    setChats,
    loading: historyLoading,
    error: historyError,
  } = useConversations(task, state, revision);
  const active = Boolean(task && (!state || state.status === "pending"));
  const pending = chats.find((chat) => chat.task === task);
  const preview =
    active &&
    progress?.type !== "recovering" &&
    (progress?.attempt ?? 0) >= (state?.attempts ?? 0)
      ? (progress?.text ?? "")
      : "";
  const recovered = (state?.attempts ?? 0) > 1;
  const title =
    pending?.title ??
    (task
      ? (state?.prompt.slice(0, 48) ?? "Restoring conversation")
      : "New conversation");

  useEffect(() => {
    api<typeof config>("/api/config")
      .then(setConfig)
      .catch(() =>
        setActionError(
          "Cannot reach the chat service. Check that it is running, then reload.",
        ),
      );
  }, []);
  useEffect(() => {
    const url = new URL(location.href);
    if (task) url.searchParams.set("task", task);
    else url.searchParams.delete("task");
    history.replaceState(null, "", url);
  }, [task]);
  useEffect(() => {
    if (task && atBottom && viewport.current)
      viewport.current.scrollTo({
        top: viewport.current.scrollHeight,
        behavior: "instant",
      });
  }, [task, state?.messages, preview, atBottom]);

  function choose(id: string | null) {
    setSeed(null);
    setTask(id);
    setActionError(null);
    setDraft("");
    setAtBottom(true);
    setMenuOpen(false);
  }
  async function submit(event?: FormEvent, retry?: Chat) {
    event?.preventDefault();
    if (sending || (!retry && (active || !draft.trim()))) return;
    const prompt = retry?.prompt ?? draft.trim();
    const id = retry?.task ?? crypto.randomUUID();
    const previous = retry
      ? retry.previous
      : state?.status === "completed"
        ? task
        : null;
    const item = retry ?? {
      task: id,
      title: prompt.slice(0, 48),
      prompt,
      previous,
      updatedAt: Date.now(),
    };
    setSending(true);
    setActionError(null);
    if (!retry) {
      setChats((current) =>
        [item, ...current.filter((chat) => chat.task !== previous)].slice(
          0,
          20,
        ),
      );
      setSeed({
        status: "pending",
        prompt,
        previous,
        updatedAt: item.updatedAt,
        attempts: 0,
        modelSteps: 0,
        toolReceipts: 0,
        count: 0,
        messages: [
          ...(state?.status === "completed" ? state.messages : []),
          { role: "user", content: prompt },
        ],
        receipts: [],
        usage: [],
      });
      setTask(id);
      setDraft("");
      setAtBottom(true);
    }
    try {
      await api(`/api/tasks/${id}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ prompt, previous }),
      });
      if (retry) setRevision((value) => value + 1);
    } catch (failure) {
      setActionError(
        `${failure instanceof Error ? failure.message : "Connection interrupted"}. Retry this turn with the same ID.`,
      );
    } finally {
      setSending(false);
      input.current?.focus();
    }
  }
  async function restart() {
    if (restarting) return;
    setRestarting(true);
    setActionError(null);
    try {
      await api("/api/restart", { method: "POST" });
      setRevision((value) => value + 1);
    } catch {
      setActionError(
        "Worker restart failed. Saved work is retained. Try again.",
      );
    } finally {
      setRestarting(false);
    }
  }
  async function copy() {
    try {
      await navigator.clipboard.writeText(
        (state?.messages ?? [])
          .filter((message) => message.role !== "tool")
          .map(
            (message) =>
              `${message.role === "user" ? "You" : "Cellar"}\n${textOf(message)}`,
          )
          .join("\n\n"),
      );
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      setActionError(
        "Copy is unavailable in this browser. Select the text to copy it.",
      );
    }
  }

  const navigation = (
    <ChatNavigation
      chats={chats}
      task={task}
      choose={choose}
      loading={historyLoading}
      error={historyError}
    />
  );

  const ledger = (
    <SavedWork
      task={task}
      state={state}
      progress={progress}
      restarting={restarting}
      restart={restart}
    />
  );

  return (
    <TooltipProvider>
      <div className="workspace">
        <a className="skip-link" href="#chat">
          Skip to conversation
        </a>
        <aside className="sidebar">{navigation}</aside>
        <div className="main-column">
          <header className="topbar">
            <Sheet open={menuOpen} onOpenChange={setMenuOpen}>
              <SheetTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon-lg"
                  className="mobile-menu"
                  aria-label="Open conversations"
                >
                  <Menu />
                </Button>
              </SheetTrigger>
              <SheetContent side="left">
                <SheetHeader>
                  <SheetTitle>Conversations</SheetTitle>
                  <SheetDescription>
                    Return to saved work or start a new chat.
                  </SheetDescription>
                </SheetHeader>
                {navigation}
              </SheetContent>
            </Sheet>
            <div className="page-heading">
              <h1>Agent workspace</h1>
              <span>{title}</span>
            </div>
            <Badge variant="outline" className="model-badge">
              {config.provider === "fixture" ? "Fixture · " : ""}
              {config.model}
            </Badge>
            <Sheet>
              <SheetTrigger asChild>
                <Button
                  variant="outline"
                  size="icon-lg"
                  className="mobile-ledger"
                  aria-label="Open saved work"
                >
                  <PanelRight />
                </Button>
              </SheetTrigger>
              <SheetContent>
                <SheetHeader>
                  <SheetTitle>Saved work</SheetTitle>
                  <SheetDescription>
                    Checkpoints, tool results and recovery.
                  </SheetDescription>
                </SheetHeader>
                {ledger}
              </SheetContent>
            </Sheet>
          </header>
          <main id="chat" className="chat-main" tabIndex={-1}>
            <div
              className="conversation-scroll"
              role="region"
              aria-label="Conversation"
              tabIndex={0}
              ref={viewport}
              onScroll={(event) => {
                const el = event.currentTarget;
                setAtBottom(
                  el.scrollHeight - el.scrollTop - el.clientHeight < 100,
                );
              }}
            >
              <div className="conversation">
                {!task ? (
                  <Empty className="welcome">
                    <EmptyHeader>
                      <EmptyMedia variant="icon">
                        <Layers3 />
                      </EmptyMedia>
                      <EmptyTitle>
                        <h2>
                          A little help.
                          <br />A lasting result.
                        </h2>
                      </EmptyTitle>
                      <EmptyDescription>
                        Ask a question, save a note, or put a plan into action.
                        Your work stays here, even when the worker restarts.
                      </EmptyDescription>
                    </EmptyHeader>
                    <EmptyContent>
                      <span className="welcome-label">
                        A FEW PLACES TO START
                      </span>
                    </EmptyContent>
                    <div className="suggestions">
                      {suggestions.map(({ icon: Icon, title, prompt }) => (
                        <Button
                          key={title}
                          variant="outline"
                          className="suggestion"
                          onClick={() => {
                            setDraft(prompt);
                            input.current?.focus();
                          }}
                        >
                          <Icon data-icon="inline-start" />
                          <span>{title}</span>
                          <ArrowUpRight data-icon="inline-end" />
                        </Button>
                      ))}
                    </div>
                  </Empty>
                ) : null}
                {state ? (
                  <Transcript messages={state.messages} />
                ) : task ? (
                  <p className="restore-note">
                    <Spinner />
                    Restoring your conversation…
                  </p>
                ) : null}
                {active ? (
                  <article className="message assistant-message provisional">
                    <div className="message-label">
                      <Layers3 aria-hidden="true" />
                      Cellar
                      <Badge variant="outline">
                        {preview
                          ? "Live draft"
                          : recovered
                            ? "Resuming"
                            : "Working"}
                      </Badge>
                    </div>
                    {preview ? (
                      <Markdown text={preview} streaming />
                    ) : (
                      <p className="thinking">
                        <Spinner />
                        {restarting || progress?.type === "recovering"
                          ? "Recovering the worker. Your saved work is safe."
                          : progress?.type === "tool"
                            ? "Tool result saved. Preparing the next step…"
                            : "Working on your request…"}
                      </p>
                    )}
                    {!connected && !restarting ? (
                      <p className="connection-note">
                        <WifiOff aria-hidden="true" />
                        Live connection interrupted. Checking saved progress…
                      </p>
                    ) : null}
                  </article>
                ) : null}
                {state?.status === "completed" ? (
                  <div className="answer-actions">
                    <span>
                      <CheckCheck aria-hidden="true" />
                      {recovered
                        ? `Recovered and saved · ${state.attempts} attempts`
                        : "Answer and tool results saved"}
                    </span>
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <Button
                          variant="ghost"
                          size="icon"
                          aria-label="Copy conversation"
                          onClick={() => void copy()}
                        >
                          {copied ? <Check /> : <Copy />}
                        </Button>
                      </TooltipTrigger>
                      <TooltipContent>
                        {copied ? "Copied" : "Copy conversation"}
                      </TooltipContent>
                    </Tooltip>
                  </div>
                ) : null}
                {state?.status === "failed" ? (
                  <Alert variant="destructive">
                    <AlertTitle>This turn needs attention</AlertTitle>
                    <AlertDescription>
                      Automatic attempts have stopped. Saved tool results are
                      retained. Start a new chat to try another request.
                    </AlertDescription>
                  </Alert>
                ) : null}
                {actionError || error ? (
                  <Alert>
                    <WifiOff />
                    <AlertTitle>
                      {actionError ? "Action interrupted" : "Reconnecting"}
                    </AlertTitle>
                    <AlertDescription>
                      {actionError ?? error}
                      {actionError && pending ? (
                        <Button
                          variant="outline"
                          disabled={sending}
                          onClick={() => void submit(undefined, pending)}
                        >
                          Retry same turn
                        </Button>
                      ) : null}
                    </AlertDescription>
                  </Alert>
                ) : null}
              </div>
            </div>
            {!atBottom ? (
              <Button
                variant="outline"
                size="icon-lg"
                className="jump-latest"
                aria-label="Jump to latest message"
                onClick={() => setAtBottom(true)}
              >
                <ArrowDown />
              </Button>
            ) : null}
            <div className="composer-area">
              <form onSubmit={(event) => void submit(event)}>
                <FieldGroup>
                  <Field data-invalid={draft.length > 8000}>
                    <FieldLabel htmlFor="message" className="sr-only">
                      Message Cellar
                    </FieldLabel>
                    <InputGroup>
                      <InputGroupTextarea
                        id="message"
                        ref={input}
                        value={draft}
                        onChange={(event) => setDraft(event.target.value)}
                        placeholder={
                          active
                            ? "You can draft your next message while Cellar works…"
                            : "Ask Cellar to help with something…"
                        }
                        maxLength={8000}
                        rows={2}
                        aria-invalid={draft.length > 8000}
                        onKeyDown={(event) => {
                          if (
                            event.key === "Enter" &&
                            !event.shiftKey &&
                            !event.nativeEvent.isComposing
                          ) {
                            event.preventDefault();
                            void submit();
                          }
                        }}
                      />
                      <InputGroupAddon align="block-end">
                        <span className="composer-caption">
                          <ShieldCheck aria-hidden="true" />
                          Saved as you go
                        </span>
                        <Button
                          type="submit"
                          size="icon-lg"
                          aria-label="Send message"
                          disabled={
                            sending ||
                            active ||
                            !draft.trim() ||
                            !config.provider
                          }
                        >
                          {sending ? <Spinner /> : <ArrowUp />}
                        </Button>
                      </InputGroupAddon>
                    </InputGroup>
                  </Field>
                </FieldGroup>
              </form>
              <p className="composer-help">
                Enter to send · Shift + Enter for a new line{" "}
                <span>Live text is saved when each step completes.</span>
              </p>
            </div>
          </main>
        </div>
        <aside className="ledger" aria-label="Saved work">
          {ledger}
        </aside>
      </div>
    </TooltipProvider>
  );
}
