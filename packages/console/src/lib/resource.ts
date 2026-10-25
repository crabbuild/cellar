// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { useMutation, useQueryClient } from "@tanstack/react-query"
import { fleetPath, request } from "@/lib/api"
import { useSession } from "@/lib/session"

export type ResourceTarget = {
  fleet: string
  application: string
  binding: string
  identity?: string
}
export type ResourceReply = {
  result?: unknown
  operation?: { status: string }
  replayed?: boolean
}

export function resourceRequest(
  token: string,
  target: ResourceTarget,
  command: Record<string, unknown>,
  signal?: AbortSignal
) {
  return request<ResourceReply>(
    token,
    fleetPath(target.fleet, "/resources/action"),
    {
      method: "POST",
      signal,
      body: JSON.stringify({
        application: target.application,
        binding: target.binding,
        expected_identity: target.identity,
        operation_id: crypto.randomUUID(),
        command,
      }),
    }
  )
}

export function useResourceAction(target: ResourceTarget) {
  const { token } = useSession()
  const cache = useQueryClient()
  return useMutation({
    mutationFn: (command: Record<string, unknown>) =>
      resourceRequest(token, target, command),
    retry: false,
    onSettled: () =>
      void cache.invalidateQueries({
        queryKey: [fleetPath(target.fleet, "/operations")],
      }),
  })
}

export function download(
  name: string,
  content: string | Uint8Array,
  type: string
) {
  const url = URL.createObjectURL(
    new Blob(
      [typeof content === "string" ? content : Uint8Array.from(content)],
      { type }
    )
  )
  const anchor = document.createElement("a")
  anchor.href = url
  anchor.download = name
  anchor.click()
  URL.revokeObjectURL(url)
}
