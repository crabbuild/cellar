import { Link, NavLink, Outlet, useNavigate, useParams } from "react-router-dom"
import {
  Activity,
  Blocks,
  Box,
  Braces,
  ChevronRight,
  CircleUserRound,
  Clock3,
  Database,
  GitBranch,
  Layers3,
  LayoutDashboard,
  LogOut,
  Moon,
  Network,
  Server,
  Settings2,
  Sun,
  Workflow,
  type LucideIcon,
} from "lucide-react"
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarInset,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarProvider,
  SidebarTrigger,
} from "@/components/ui/sidebar"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Separator } from "@/components/ui/separator"
import { useTheme } from "@/components/theme-provider"
import { useSession } from "@/lib/session"

const groups: {
  label: string
  links: { path: string; label: string; icon: LucideIcon }[]
}[] = [
  {
    label: "Fleet",
    links: [
      { path: "overview", label: "Overview", icon: LayoutDashboard },
      { path: "applications", label: "Applications", icon: Blocks },
      { path: "nodes", label: "Nodes", icon: Server },
      { path: "cells", label: "Cells", icon: Box },
    ],
  },
  {
    label: "Data & storage",
    links: [
      { path: "sql", label: "SQL databases", icon: Database },
      { path: "kv", label: "Key-value", icon: Braces },
    ],
  },
  {
    label: "Asynchronous work",
    links: [
      { path: "queues", label: "Queues", icon: Layers3 },
      { path: "workflows", label: "Workflows", icon: Workflow },
      { path: "cron", label: "Cron schedules", icon: Clock3 },
    ],
  },
  {
    label: "Manage",
    links: [
      { path: "operations", label: "Operations", icon: Activity },
      { path: "settings", label: "Fleet settings", icon: Settings2 },
    ],
  },
]

export function ConsoleLayout() {
  const { fleet = "" } = useParams()
  const navigate = useNavigate()
  const { session, logout } = useSession()
  const { theme, setTheme } = useTheme()
  const selected = session.fleets.find((f) => f.id === fleet)
  return (
    <SidebarProvider>
      <Sidebar>
        <SidebarHeader>
          <Link to={`/fleets/${fleet}/overview`} className="brand">
            <span className="brand-mark">
              <Network />
            </span>
            <span>
              cellar<span className="brand-caption">FLEET CONSOLE</span>
            </span>
          </Link>
          <div className="fleet-switcher">
            <span className="eyebrow">Your workspace</span>
            <Select
              value={fleet}
              onValueChange={(value) => navigate(`/fleets/${value}/overview`)}
            >
              <SelectTrigger className="w-full" aria-label="Select fleet">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectGroup>
                  {session.fleets.map((f) => (
                    <SelectItem value={f.id} key={f.id}>
                      {f.name}
                    </SelectItem>
                  ))}
                </SelectGroup>
              </SelectContent>
            </Select>
          </div>
        </SidebarHeader>
        <SidebarContent>
          {groups.map((group) => (
            <SidebarGroup key={group.label}>
              <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
              <SidebarGroupContent>
                <SidebarMenu>
                  {group.links.map(({ path, label, icon: Icon }) => (
                    <SidebarMenuItem key={path}>
                      <NavLink to={`/fleets/${fleet}/${path}`}>
                        {({ isActive }) => (
                          <SidebarMenuButton asChild isActive={isActive}>
                            <span>
                              <Icon aria-hidden />
                              <span>{label}</span>
                            </span>
                          </SidebarMenuButton>
                        )}
                      </NavLink>
                    </SidebarMenuItem>
                  ))}
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>
          ))}
        </SidebarContent>
        <SidebarFooter>
          <div className="sidebar-note">
            <GitBranch />
            <div>
              One fleet. Every cell.<p>Ownership you can inspect.</p>
            </div>
          </div>
          <Separator />
          <div className="user-row">
            <CircleUserRound />
            <span>{session.user}</span>
            <Button
              variant="ghost"
              size="icon-sm"
              aria-label="Sign out"
              onClick={logout}
            >
              <LogOut />
            </Button>
          </div>
        </SidebarFooter>
      </Sidebar>
      <SidebarInset className="min-w-0">
        <header className="topbar">
          <div className="flex items-center gap-3">
            <SidebarTrigger />
            <Separator orientation="vertical" className="h-4" />
            <span className="text-muted-foreground">Fleets</span>
            <ChevronRight className="size-3 text-muted-foreground" />
            <span>{selected?.name ?? "Unknown fleet"}</span>
            <Badge variant="outline">
              {selected?.environment ?? "Unconfigured"}
            </Badge>
          </div>
          <Button
            variant="ghost"
            size="icon"
            aria-label="Toggle color theme"
            onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
          >
            {theme === "dark" ? <Sun /> : <Moon />}
          </Button>
        </header>
        <main className="console-content" id="main">
          <Outlet />
        </main>
        <footer className="console-footer">
          <span>Cellar · Durable applications, visible infrastructure.</span>
          <span>Observation is not ownership authority</span>
        </footer>
      </SidebarInset>
    </SidebarProvider>
  )
}
