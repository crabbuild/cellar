// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useEffect, useRef, useState } from "react";

export interface Part {
  type: string;
  text?: string;
  toolName?: string;
  toolCallId?: string;
  input?: unknown;
  output?: unknown;
}
export interface Message {
  role: "user" | "assistant" | "tool";
  content: string | Part[];
}
export interface Receipt {
  name: string;
  input: unknown;
  result: unknown;
}
export interface Task {
  status: "pending" | "completed" | "failed";
  prompt: string;
  previous: string | null;
  updatedAt: number | null;
  attempts: number;
  modelSteps: number;
  toolReceipts: number;
  count: number;
  messages: Message[];
  receipts: Receipt[];
  usage: {
    inputTokens: { total?: number };
    outputTokens: { total?: number };
  }[];
}
export interface Progress {
  type: string;
  attempt: number;
  text?: string;
  step?: number;
  tool?: string;
  replayed?: boolean;
}

export async function api<T>(path: string, options?: RequestInit): Promise<T> {
  const reply = await fetch(path, {
    ...options,
    signal: options?.signal ?? AbortSignal.timeout(15000),
  });
  if (!reply.ok) {
    const value = await reply.json().catch(() => ({}));
    throw new Error(
      typeof value.error === "string"
        ? value.error
        : `Request failed (${reply.status})`,
    );
  }
  return reply.json();
}

export function useTask(
  id: string | null,
  seed: Task | null,
  revision: number,
) {
  const [loaded, setLoaded] = useState({ id, value: seed });
  // A selection change must not expose the previous turn's state while the
  // new effect is starting; the history directory also consumes this value.
  const state = loaded.id === id ? loaded.value : seed;
  const [progress, setProgress] = useState<Progress | null>(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const initial = useRef(seed);
  initial.current = seed;

  useEffect(() => {
    setLoaded({ id, value: initial.current });
    setProgress(null);
    setError(null);
    setConnected(false);
    if (!id) return;

    // SSE carries provisional text. Polling remains the source of durable state,
    // including after a dropped stream or a worker restart.
    const controller = new AbortController();
    const events = new EventSource(`/api/tasks/${id}/events`);
    let timer: ReturnType<typeof setTimeout>;
    events.onopen = () => setConnected(true);
    events.onerror = () => setConnected(false);
    events.addEventListener("progress", (event) => {
      try {
        const value = JSON.parse(event.data) as Progress;
        setProgress(value);
      } catch {
        /* Ignore a malformed preview; poll authoritative state. */
      }
    });

    async function poll() {
      try {
        const value = await api<Task>(`/api/tasks/${id}`, {
          signal: controller.signal,
        });
        if (controller.signal.aborted) return;
        setLoaded({ id, value });
        setError(null);
        if (value.status !== "pending") {
          events.close();
          setConnected(false);
          return;
        }
      } catch (failure) {
        if (controller.signal.aborted) return;
        setError(
          failure instanceof Error
            ? failure.message
            : "Connection interrupted. Reconnecting…",
        );
      }
      timer = setTimeout(() => void poll(), 1000);
    }

    void poll();
    return () => {
      controller.abort();
      events.close();
      clearTimeout(timer);
    };
  }, [id, revision]);

  return { state, progress, connected, error };
}
