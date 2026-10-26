<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# Local Sandbox HTTP previews

[Workers facade](README.md) · [Pinned qualification inputs](../../tools/sandbox/compat/README.md)

The local Node profile runs the published Sandbox 0.12.9 preview helper and its
Durable Object authorization. The selected container provider must support native
queued lifecycle, running observations and port forwarding.

## Application wiring

Route previews before ordinary application endpoints. The SDK parses the
hostname, replaces internal proxy headers, checks its token and verifies that
exposure belongs to the active sandbox runtime.

```js
import { getSandbox, proxyToSandbox, Sandbox } from "@cloudflare/sandbox";
import { workersApplication } from "@cellar/cloudflare";

export default workersApplication({
  Sandbox,
  default: {
    async fetch(request, env) {
      const preview = await proxyToSandbox(request, env);
      if (preview) return preview;

      const sandbox = getSandbox(env.Sandbox, "demo");
      // Start your application on port 8080 before exposing it.
      const { url } = await sandbox.exposePort(8080, {
        hostname: "preview.example.com",
      });
      return Response.json({ url });
    },
  },
}, {
  namespaces: { Sandbox: "Sandbox" },
  containers: ["Sandbox"],
});
```

The operator must route the preview domain and its wildcard subdomains to this
application. Local qualification uses an isolated ingress with explicit hostname
resolution; it does not publish DNS or deploy a hosted Cloudflare gateway.

## Declare additional container ports

Linux reaches container bridge addresses directly. A macOS host uses ports
published by its Docker VM. Add `exposed_ports` when a preview port is absent
from the pinned image's `EXPOSE` metadata:

```json
{
  "containers": [{
    "class_name": "Sandbox",
    "image": "cloudflare/sandbox:0.12.9",
    "exposed_ports": [8080]
  }]
}
```

Use the versioned image reference selected by your deployment. Qualification
pins the exact package, control-server image digest and platform.

| Rule | Behavior |
| --- | --- |
| Configuration | Per-class deployment configuration; guest start options cannot add publications |
| Bounds | At most 64 additional distinct TCP ports, each 1–65535 |
| Image identity | Preserves the original image; Docker merges additional declarations with image ports |
| Publication | Off Linux, additional ports bind loopback with daemon-assigned host ports |
| Admission | Packaged deployments require `container-ports-v1`; unsupported hosts refuse the feature |
| Authorization | This declares reachability. SDK token/runtime checks and native cell/port authority still govern forwarding |

## Preview lifetime

| Situation | Published SDK behavior |
| --- | --- |
| Matching token and active exposure | Forward method, path/query and bounded binary body; stream the response |
| Invalid token or revoked port | Return 404 |
| Stopped/replaced runtime with retained authorization | Return 410; do not wake a container for preview traffic |
| Explicit `exposePort` after restart | Reactivate retained authorization for the new runtime |
| `unexposePort` | Revoke the port in DO storage |
| SDK `destroy` | Revoke authorization and tear down the sandbox |

The Node profile supplies Workers promise-rejection events and
`crypto.subtle.timingSafeEqual` using Node's native comparison. This permits the
unchanged SDK's process-readiness stream cancellation and preview token checks.
Floating rejections are reported unless an event listener prevents default;
awaited failures and explicit `waitUntil` work retain their failure semantics.
Ordinary Node applications retain Node's fatal unhandled-rejection behavior.

The published Containers idle alarm also uses the native lifecycle: `sleepAfter`
expires, `onActivityExpired` signals the process, its monitor records exit and
`onStop` completes before a later wake. Start/stop hooks may persist DO state.
Queued container controls inside transaction/block callbacks remain unsupported.
Asynchronous alarm controls yield while native callback admission is held. The
[overlapping callback/alarm regression](../../plans/sandbox-provider-progress.md#resolved-callbackalarm-qualification-failure)
passes with the original stop barrier; the complete callback profile remains open.

The same published preview helper now forwards WebSocket upgrades through Worker,
Durable Object and container boundaries. The pinned-image fixture runs a small
HTTP/WebSocket echo service using its included Bun 1.3.12; it checks text/binary
frames, routing-header removal, clean close and abrupt disconnect before and
after explicit re-exposure. It does not replace the SDK or change the image.

These are selected Node preview paths. Tunnels, bucket mounts, full slow-peer,
ownership-loss and remote-node qualification, hibernation and Sandbox 1.x require
separate evidence.
