<!-- Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0. -->

# JavaScript tooling

Install the locked build and formatting tools with:

```sh
npm ci --ignore-scripts --prefix tools/javascript
```

Format the JavaScript runtime, integration fixtures, Cloudflare compatibility
package, and JavaScript/sandbox tooling with:

```sh
npm run format --prefix tools/javascript
npm run format:check --prefix tools/javascript
```

The formatter uses two-space indentation and an 80-column print width. Private
runtime factories omit the final expression semicolon because the native
bootstrap embeds them inside parentheses. Generated sandbox bundles,
`node_modules`, and the byte-pinned upstream files in
`tools/sandbox/compat/fixtures/celld` are excluded.

Run build-tool tests with `npm test --prefix tools/javascript`. See the
[sandbox input guide](../sandbox/compat/README.md) for compatibility inventory
verification and fixture builds.
