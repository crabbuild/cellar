// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { Layers3, MessageSquare, Plus, ShieldCheck } from "lucide-react";
import { Button } from "@/components/ui/button";

export interface Chat {
  task: string;
  title: string;
  prompt: string;
  previous: string | null;
  updatedAt: number | null;
}
export function ChatNavigation({
  chats,
  task,
  choose,
  loading,
  error,
}: {
  chats: Chat[];
  task: string | null;
  choose: (id: string | null) => void;
  loading: boolean;
  error: boolean;
}) {
  return (
    <div className="navigation-content">
      <a
        href="/"
        className="wordmark"
        onClick={(event) => {
          event.preventDefault();
          choose(null);
        }}
      >
        <Layers3 aria-hidden="true" />
        cellar<span>agent studio</span>
      </a>
      <Button
        variant="outline"
        size="lg"
        onClick={() => choose(null)}
        className="w-full"
      >
        <Plus data-icon="inline-start" />
        New chat
      </Button>
      <div className="nav-label">
        Conversations{" "}
        <span>
          {!chats.length && loading
            ? "…"
            : !chats.length && error
              ? "—"
              : chats.length}
        </span>
      </div>
      <nav aria-label="Conversations" className="chat-list" aria-busy={loading}>
        {chats.map((chat) => (
          <Button
            key={chat.task}
            variant={chat.task === task ? "secondary" : "ghost"}
            className="justify-start min-w-0 w-full"
            onClick={() => choose(chat.task)}
            aria-current={chat.task === task ? "page" : undefined}
          >
            <MessageSquare data-icon="inline-start" />
            <span className="truncate">{chat.title}</span>
          </Button>
        ))}
      </nav>
      {loading || error || !chats.length ? (
        <p className="nav-hint">
          {loading
            ? "Restoring conversations…"
            : error
              ? "Conversation history is unavailable. Retrying automatically…"
              : "Your conversations will appear here. Each one has a link you can return to."}
        </p>
      ) : null}
      <div className="nav-footer">
        <ShieldCheck aria-hidden="true" />
        <div>
          <strong>Work, safely saved</strong>
          <p>Pick up where you left off.</p>
        </div>
      </div>
    </div>
  );
}
