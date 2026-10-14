import { createContext, useContext } from "react"
import type { Session } from "@/lib/api"

export const SessionContext = createContext<{
  token: string
  session: Session
  logout: () => void
} | null>(null)
export function useSession() {
  const session = useContext(SessionContext)
  if (!session) throw new Error("Session provider missing")
  return session
}
