import { Plus, ExternalLink, Server } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
export function AddNodeGuide() {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline">
          <Plus data-icon="inline-start" />
          Add node
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>
            <span className="flex items-center gap-2">
              <Server className="size-5" />
              Join a node to this fleet
            </span>
          </DialogTitle>
          <DialogDescription>
            A node joins when it starts with this fleet’s storage authority and
            publishes its lease. Prepare and launch it through your
            infrastructure or embedding host.
          </DialogDescription>
        </DialogHeader>
        <ol className="list-decimal space-y-4 pl-5 text-sm">
          <li>
            <strong>Install the compiled application.</strong>
            <p className="mt-1 text-muted-foreground">
              Use the verified executable for the host platform, with this
              application’s factories compiled into it.
            </p>
          </li>
          <li>
            <strong>Assign a unique node identity and data directory.</strong>
            <p className="mt-1 text-muted-foreground">
              Give the process its own writable durable directory. Keep the
              previous process stopped and joined before reusing an identity.
            </p>
          </li>
          <li>
            <strong>Connect the same fleet storage and private network.</strong>
            <p className="mt-1 text-muted-foreground">
              Configure the exact bucket/backend/prefix, storage credentials,
              shared peer authentication and a private advertised address
              reachable by the fleet. Keep these credentials on the host.
            </p>
          </li>
          <li>
            <strong>Start the host and verify readiness.</strong>
            <p className="mt-1 text-muted-foreground">
              Refresh Nodes, open the new node and inspect its live state.
              Confirm startup recovery, fleet readiness and an application
              request before sending production traffic.
            </p>
          </li>
        </ol>
        <a
          className="text-link flex items-center gap-2 text-sm"
          href="https://github.com/crabbuild/cellar/blob/main/docs/13-fleet-topology.md"
          target="_blank"
          rel="noreferrer"
        >
          Read the fleet infrastructure guide
          <ExternalLink className="size-4" />
        </a>
      </DialogContent>
    </Dialog>
  )
}
