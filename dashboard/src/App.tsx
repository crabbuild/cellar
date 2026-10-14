import { lazy, Suspense, useState } from "react"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom"
import { Box, KeyRound, Network, Server, Workflow } from "lucide-react"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Button } from "@/components/ui/button"
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Toaster } from "@/components/ui/sonner"
import { TooltipProvider } from "@/components/ui/tooltip"
import { ConsoleLayout } from "@/components/console-layout"
import { Feedback, Nothing } from "@/components/console-ui"
import { request, type Session } from "@/lib/api"
import { SessionContext } from "@/lib/session"

const Overview = lazy(() => import("@/pages/overview"))
const Inventory = lazy(() => import("@/pages/inventory"))
const Details = lazy(() => import("@/pages/details"))
const Resources = lazy(() => import("@/pages/resources"))
const Workspace = lazy(() => import("@/pages/workspace"))
const Operations = lazy(() =>
  import("@/pages/management").then((module) => ({
    default: module.Operations,
  }))
)
const Settings = lazy(() =>
  import("@/pages/management").then((module) => ({ default: module.Settings }))
)
const ApplicationDetail = lazy(() =>
  import("@/pages/management").then((module) => ({
    default: module.ApplicationDetail,
  }))
)

export default function App() {
  const [auth, setAuth] = useState<{ token: string; session: Session } | null>(
    null
  )
  const [client] = useState(
    () => new QueryClient({ defaultOptions: { mutations: { retry: false } } })
  )
  function logout() {
    client.clear()
    setAuth(null)
  }
  return (
    <TooltipProvider>
      <Toaster />
      {!auth ? (
        <Login onLogin={setAuth} />
      ) : (
        <SessionContext.Provider value={{ ...auth, logout }}>
          <QueryClientProvider client={client}>
            <BrowserRouter>
              <a href="#main" className="skip-link">
                Skip to content
              </a>
              <Suspense
                fallback={
                  <div className="p-8">
                    <Feedback loading>{null}</Feedback>
                  </div>
                }
              >
                <Routes>
                  <Route
                    path="/"
                    element={
                      auth.session.fleets[0] ? (
                        <Navigate
                          replace
                          to={`/fleets/${auth.session.fleets[0].id}/overview`}
                        />
                      ) : (
                        <Nothing
                          title="No fleet access"
                          description="Ask your console administrator to grant metadata access to a configured fleet."
                        />
                      )
                    }
                  />
                  <Route path="/fleets/:fleet" element={<ConsoleLayout />}>
                    <Route index element={<Navigate replace to="overview" />} />
                    <Route path="overview" element={<Overview />} />
                    <Route path="nodes" element={<Inventory kind="nodes" />} />
                    <Route
                      path="nodes/:id"
                      element={<Details kind="nodes" />}
                    />
                    <Route path="cells" element={<Inventory kind="cells" />} />
                    <Route
                      path="cells/:id"
                      element={<Details kind="cells" />}
                    />
                    <Route
                      path="applications"
                      element={<Inventory kind="applications" />}
                    />
                    <Route
                      path="applications/:id"
                      element={<ApplicationDetail />}
                    />
                    {(
                      ["sql", "kv", "queues", "workflows", "cron"] as const
                    ).map((kind) => (
                      <Route key={kind} path={kind}>
                        <Route index element={<Resources kind={kind} />} />
                        <Route
                          path=":application/:binding"
                          element={<Workspace kind={kind} />}
                        />
                      </Route>
                    ))}
                    <Route path="operations" element={<Operations />} />
                    <Route path="settings" element={<Settings />} />
                  </Route>
                  <Route
                    path="*"
                    element={
                      <Nothing
                        title="Page not found"
                        description="Choose a fleet and resource from the console navigation."
                      />
                    }
                  />
                </Routes>
              </Suspense>
            </BrowserRouter>
          </QueryClientProvider>
        </SessionContext.Provider>
      )}
    </TooltipProvider>
  )
}

function Login({
  onLogin,
}: {
  onLogin: (auth: { token: string; session: Session }) => void
}) {
  const [token, setToken] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [pending, setPending] = useState(false)
  return (
    <div className="login-shell">
      <aside className="login-story">
        <div className="brand">
          <span className="brand-mark">
            <Network />
          </span>
          <span>
            cellar<span className="brand-caption">FLEET CONSOLE</span>
          </span>
        </div>
        <div>
          <span className="eyebrow">Built for application builders</span>
          <h1 className="mt-5">
            Your infrastructure.
            <br />
            Every cell in view.
          </h1>
          <p>
            Follow ownership, inspect durable state, and manage the work running
            across your fleet.
          </p>
          <div className="login-topology">
            <span>
              <Server />
              Nodes
            </span>
            <span>
              <Box />
              Cells
            </span>
            <span>
              <Workflow />
              Workflows
            </span>
          </div>
        </div>
        <small>Native execution. Durable state. Clear ownership.</small>
      </aside>
      <main className="login-form">
        <div>
          <h2>Connect to your console</h2>
          <p>
            Use the access token issued by your console administrator. Your
            permissions determine which fleets and resources you can manage.
          </p>
          <form
            onSubmit={(event) => {
              event.preventDefault()
              setPending(true)
              setError(null)
              void request<Session>(token, "/session")
                .then((session) => onLogin({ token, session }))
                .catch((error: Error) => setError(error.message))
                .finally(() => setPending(false))
            }}
          >
            <FieldGroup>
              <Field data-invalid={Boolean(error)}>
                <FieldLabel htmlFor="access-token">
                  Dashboard access token
                </FieldLabel>
                <Input
                  id="access-token"
                  type="password"
                  autoComplete="off"
                  placeholder="Paste your access token"
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  aria-invalid={Boolean(error)}
                  required
                />
                <FieldDescription>
                  Your token is held in memory and cleared when you close or
                  reload this page.
                </FieldDescription>
              </Field>
              {error ? (
                <Alert variant="destructive">
                  <AlertTitle>Could not sign in</AlertTitle>
                  <AlertDescription>{error}</AlertDescription>
                </Alert>
              ) : null}
              <Button type="submit" disabled={pending || !token} size="lg">
                <KeyRound data-icon="inline-start" />
                {pending ? "Connecting…" : "Connect to fleet console"}
              </Button>
            </FieldGroup>
          </form>
        </div>
      </main>
    </div>
  )
}
