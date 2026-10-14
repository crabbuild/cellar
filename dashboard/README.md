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

## Theme and design tokens

The console uses a restrained developer-tool style: neutral surfaces, crisp borders,
Geist for interface text, system monospace for identifiers and code, and blue for
links, selection and focus. Status colors always accompany a label or icon.
The semantic color roles take inspiration from [Geist](https://vercel.com/geist/colors)
and [Cloudflare's component system](https://cloudflare.github.io/cf-ui/).

`src/index.css` is the single source for theme values. Its foundation scale feeds
semantic roles, which are exported to Tailwind through `@theme inline`. Both
light and dark modes use the same component classes; avoid page-level hex colors
or separate dark-mode color overrides.

| Role | Tokens / usage |
| --- | --- |
| Surfaces | `background` for the workspace, `card` for panels, `muted` for recessed controls |
| Text | `foreground` for content, `muted-foreground` for supporting text |
| Actions | `primary` / `primary-foreground` / `primary-hover` for the main action; `info` for links and selection |
| Boundaries | `border`, `border-hover`, `input`; `ring` for keyboard focus |
| Status | `success`, `warning`, `destructive`, `info`, each with `-muted` and `-border` companions |
| Typography | 14px body, 12px desktop captions, 28px headings; captions rise to 14px on narrow screens |
| Spacing | `--space-*` on a 4px scale; `--page-gutter` and `--content-width` define the shell |
| Controls | 36px standard, 32px small, 44px large; primary mobile controls are at least 44px |
| Shape & motion | 8px base radius, subtle/floating shadow tokens, 150ms transitions with reduced-motion support |

Use the shared `components/ui` variants for forms, tables, badges and actions.
The theme selector provides light, system and dark modes, persists the preference,
and follows operating-system changes in system mode. There is no unmodified
single-key theme shortcut. Browser coverage verifies theme persistence, semantic
text contrast, mobile control sizes, navigation dismissal and resource filtering.
