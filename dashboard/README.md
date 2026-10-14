# Cellar fleet console

Vite + React + TypeScript + shadcn/ui (Radix) + Lucide. The frontend uses the
[Cellar management service](../crates/cellar-admin/README.md), never fleet
credentials or a node's private listener. See the
[implemented scope and verification record](../plans/003-fleet-dashboard.md).

```sh
npm ci
npm run dev       # 127.0.0.1:5173; API proxy to 127.0.0.1:4174
npm run build    # production assets in dist/
npm run lint
npm run test:ui  # browser interactions against explicit test fixtures
```

Playwright tests use synthetic fixtures to check UI behavior. They are not live
fleet integration tests. Screenshots are saved under ignored `test-results/`.
The deployed browser application contains no demo provider or synthetic fleet.

Pages are loaded on demand. Query caches are cleared on logout. Resource identity
and permission checks are enforced by the Rust server; hiding a button is not
an authorization boundary. Write requests are not automatically retried.
