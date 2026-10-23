# @cellar/sdk

Types and buffered HTTP helpers for embedded Cellar JavaScript/TypeScript
applications. This is an ordinary ESM npm package with TypeScript declarations.
It uses ECMAScript builtins; it does not import Node or browser APIs.

```ts
import {jsonResponse, type Context, type Request} from "@cellar/sdk";
export async function fetch(_request: Request, ctx: Context) {
  return jsonResponse({now: await ctx.now()});
}
```

Handlers receive their invocation-scoped `Context` from Cellar. Never retain it
for later invocations. Await every operation before returning. Use checked
`storage.apply` batches for atomic updates; there is no async transaction
callback. See the adapter README in the Cellar repository for limits and failure
outcomes. `request` and `response` encode byte bodies for the native bridge;
their returned objects are not browser Request/Response objects. `text()` and
`json()` reject invalid UTF-8.

Reusable components can export ordinary functions/classes taking `Context`.
Publish them with ESM exports, `types` declarations, a license, and an explicit
supported Cellar SDK range. Test with the Cellar build tool and actual runtime;
installing successfully through npm does not establish runtime compatibility.

Build a distributable archive with `npm pack`. The archive contains only the
ESM module, declaration file, this guide and license. Publishing remains a
separate release action.

## Node.js execution

The same types and buffered helpers work with Cellar’s optional `node` backend.
Applications then use the actual Node runtime and npm module loader, including
CommonJS and platform-compatible native addons. See the
[Node example](../../examples/node/README.md) for TypeScript compilation, delivery,
process lifecycle and the distinction between Cellar effects and direct Node I/O.
