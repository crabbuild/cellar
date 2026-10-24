import { useEffect, useRef, useState } from "react";
import type { Chat } from "./chat-navigation";
import { api, type Task } from "./use-task";

const storageKey = "cellar.chats.v1";

function cachedChats(): Chat[] {
  try {
    const saved: unknown = JSON.parse(localStorage.getItem(storageKey) ?? "[]");
    return Array.isArray(saved)
      ? saved
          .filter(
            (item): item is Chat =>
              item &&
              typeof item === "object" &&
              typeof item.task === "string" &&
              /^[a-zA-Z0-9_-]{1,64}$/.test(item.task) &&
              typeof item.title === "string" &&
              typeof item.prompt === "string" &&
              (item.previous === null || typeof item.previous === "string"),
          )
          .slice(0, 20)
      : [];
  } catch {
    return [];
  }
}

function latestTurns(chats: Chat[]): Chat[] {
  const unique = [...new Map(chats.map((chat) => [chat.task, chat])).values()];
  const parents = new Set(unique.map((chat) => chat.previous));
  return unique.filter((chat) => !parents.has(chat.task)).slice(0, 20);
}

export function useConversations(
  task: string | null,
  state: Task | null,
  revision: number,
) {
  const [chats, setChats] = useState(cachedChats);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const currentTask = useRef(task);
  currentTask.current = task;

  useEffect(() => {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function refresh() {
      try {
        const saved = await api<{ chats: Chat[] }>("/api/conversations", {
          signal: controller.signal,
        });
        if (controller.signal.aborted) return;
        // Keep the active optimistic submission until its acknowledgement is
        // known. Everything else comes from the durable workspace directory.
        setChats((current) =>
          latestTurns([
            ...current.filter((chat) => chat.task === currentTask.current),
            ...saved.chats,
          ]),
        );
        setError(false);
      } catch {
        if (controller.signal.aborted) return;
        setError(true);
      }
      setLoading(false);
      timer = setTimeout(() => void refresh(), 5000);
    }
    void refresh();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [task, revision]);

  const first = state?.messages.find((message) => message.role === "user");
  const title = (
    first && typeof first.content === "string" ? first.content : state?.prompt
  )?.slice(0, 48);
  useEffect(() => {
    if (!task || !state || !title) return;
    const item = {
      task,
      title,
      prompt: state.prompt,
      previous: state.previous,
    };
    setChats((current) => {
      const existing = current.find((chat) => chat.task === task);
      return existing &&
        existing.title === title &&
        existing.prompt === item.prompt &&
        existing.previous === item.previous
        ? current
        : latestTurns([item, ...current]);
    });
  }, [task, title, state?.prompt, state?.previous]);

  useEffect(() => {
    try {
      localStorage.setItem(storageKey, JSON.stringify(chats));
    } catch {
      /* Browser storage is only a cache; discovery lives in Cellar. */
    }
  }, [chats]);

  return { chats, setChats, loading, error };
}
