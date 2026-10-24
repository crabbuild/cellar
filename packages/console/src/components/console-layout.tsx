import {
  Link,
  NavLink,
  Outlet,
  useLocation,
  useNavigate,
  useParams,
} from "react-router-dom"
import {
  Activity,
  Blocks,
  Box,
  Braces,
  ChevronRight,
  CircleUserRound,
  Clock3,
  Database,
  Layers3,
  LayoutDashboard,
  LogOut,
  Monitor,
  Moon,
  Server,
  Settings2,
  Sun,
  Workflow,
  Radio,
  Send,
  Users,
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
  useSidebar,
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
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import { useTheme } from "@/components/theme-provider"
import { useSession } from "@/lib/session"
import { CellarMark } from "@/components/cellar-mark"

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
      { path: "kv", label: "KV", icon: Braces },
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
    label: "Events & delivery",
    links: [
      { path: "outboxes", label: "Outboxes", icon: Send },
      { path: "streams", label: "Event streams", icon: Radio },
      { path: "subscriptions", label: "Subscriptions", icon: Users },
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

function FleetNavigation({ fleet }: { fleet: string }) {
  const { pathname } = useLocation()
  const { setOpenMobile } = useSidebar()
  return (
    <nav aria-label="Fleet navigation">
      {groups.map((group) => (
        <SidebarGroup key={group.label}>
          <SidebarGroupLabel>{group.label}</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {group.links.map(({ path, label, icon: Icon }) => {
                const target = `/fleets/${fleet}/${path}`
                return (
                  <SidebarMenuItem key={path}>
                    <SidebarMenuButton
                      asChild
                      isActive={
                        pathname === target || pathname.startsWith(`${target}/`)
                      }
                    >
                      <NavLink to={target} onClick={() => setOpenMobile(false)}>
                        <Icon aria-hidden />
                        <span>{label}</span>
                      </NavLink>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                )
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      ))}
    </nav>
  )
}

function ThemeSwitcher() {
  const { theme, setTheme } = useTheme()
  return (
    <div className="theme-switcher">
      <ToggleGroup
        type="single"
        value={theme}
        onValueChange={(value) => {
          if (value === "light" || value === "dark" || value === "system")
            setTheme(value)
        }}
        spacing={1}
        size="sm"
        aria-label="Color theme"
      >
        <ToggleGroupItem
          value="light"
          aria-label="Light theme"
          title="Light theme"
        >
          <Sun aria-hidden />
        </ToggleGroupItem>
        <ToggleGroupItem
          value="system"
          aria-label="System theme"
          title="System theme"
        >
          <Monitor aria-hidden />
        </ToggleGroupItem>
        <ToggleGroupItem
          value="dark"
          aria-label="Dark theme"
          title="Dark theme"
        >
          <Moon aria-hidden />
        </ToggleGroupItem>
      </ToggleGroup>
    </div>
  )
}

export function ConsoleLayout() {
  const { fleet = "" } = useParams()
  const navigate = useNavigate()
  const { session, logout } = useSession()
  const { pathname } = useLocation()
  const section = groups
    .flatMap((group) => group.links)
    .find(({ path }) => pathname.split("/")[3] === path)
  const selected = session.fleets.find((f) => f.id === fleet)
  return (
    <SidebarProvider>
      <Sidebar>
        <SidebarHeader>
          <Link to={`/fleets/${fleet}/overview`} className="brand">
            <CellarMark />
            <span>
              cellar<span className="brand-caption">Fleet console</span>
            </span>
          </Link>
          <div className="fleet-switcher">
            <span className="eyebrow">Fleet</span>
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
          <FleetNavigation fleet={fleet} />
        </SidebarContent>
        <SidebarFooter>
          <Separator />
          <div className="user-row">
            <CircleUserRound />
            <span>
              {session.user}
              <small>Signed in</small>
            </span>
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
          <div className="console-breadcrumb">
            <SidebarTrigger />
            <Separator orientation="vertical" className="h-4" />
            <Link
              className="breadcrumb-fleet"
              to={`/fleets/${fleet}/overview`}
              title={selected?.name}
            >
              {selected?.name ?? "Unknown fleet"}
            </Link>
            <Badge variant="outline">
              {selected?.environment ?? "Unconfigured"}
            </Badge>
            {section ? (
              <span className="breadcrumb-section">
                <ChevronRight aria-hidden />
                {section.label}
              </span>
            ) : null}
          </div>
          <ThemeSwitcher />
        </header>
        <main className="console-content" id="main">
          <Outlet />
        </main>
        <footer className="console-footer">
          <span>Cellar · Fleet console</span>
          <span>Data reflects the last successful observation</span>
        </footer>
      </SidebarInset>
    </SidebarProvider>
  )
}
