<!-- Modified for Cellar from Celld. -->
# Static assets

Static assets serve the files of a directory, with a Worker or without one.
`cellar deploy` uploads the directory to the fleet bucket with the deployment,
and a node caches a file on its own disk when it first serves that file. Read
the
[Cloudflare Static Assets documentation](https://developers.cloudflare.com/workers/static-assets/)
for the standard behavior.

## Example

The [Rust counter project](../../examples/native-counter-project.json) serves
an HTML page from `native-counter-assets` and exposes the `ASSETS` binding to
the Rust application.

## API

The `assets` block configures the directory and the routing:

- `directory` names the directory to upload.
- `binding` exposes `env.ASSETS.fetch()` to a Worker.
- `html_handling` maps a path to an HTML file.
- `not_found_handling` selects `single-page-application` or `404-page`.
- `run_worker_first` sends a matching request to the Worker first.
- A `_headers` file and a `_redirects` file in the directory set headers and
  redirects.

## Deployment

The `assets` block accepts `directory`, `binding`, `html_handling`,
`not_found_handling`, and `run_worker_first`. Any other key stops the
deployment. A project with `assets` and no `main` cannot set `run_worker_first`
or declare a binding.

`cellar deploy` stores each file body once under its SHA-256 digest, and a
redeploy uploads only the changed bodies. A file with an unknown extension gets
no `Content-Type` header, as with Wrangler.

## Routing

cellar looks up an asset only for a `GET` or a `HEAD` request. Every other
method goes to the Worker. `run_worker_first` takes `true`, or a list of up to
100 route patterns in which a `!` prefix excludes a path. A match sends the
request to the Worker first. The exact `/` pattern matches only the root path.

![The routing decision for one request: the method test, run_worker_first, the _redirects rules, the html_handling lookup, the pointer re-read after a miss, not_found_handling, and the fall through to the Worker](static-assets-flow.svg)

The `_redirects` rules run before any file lookup, and the first match wins. A
rule with status 301, 302, 303, 307, or 308 answers with a `Location` header. A
rule with status 200 restarts routing at the local path and keeps the original
URL.

`html_handling` is `auto-trailing-slash` by default, which maps `/about` to
`/about.html` and `/about/` to `/about/index.html`. The other modes are
`force-trailing-slash`, `drop-trailing-slash`, and `none`. A non-canonical
spelling gets a `307` to the canonical path with the query string.

After a miss, a node re-reads the deployment pointer at most once in five
seconds. During a rolling restart, a newer index can then serve a file that an
upgraded node references.

`not_found_handling` set to `single-page-application` returns `/index.html`
with status 200, and `404-page` returns the nearest `404.html` with status 404.
With a Worker, cellar applies it only to a navigation request, and the Worker
answers any other miss. With no Worker and no match, the node answers `404`. A
body that the fleet bucket cannot supply gives `502` and never reaches the
Worker.

## Responses

The disk cache holds 512 MiB by default, and `CELLAR_ASSET_CACHE_BYTES` changes
it. The cache evicts the least recently used file.

The `ETag` is the strong SHA-256 digest of the body, so `If-None-Match` gives
`304`. A `Range` header gives `206`, an unsatisfiable range gives `416`, and a
`HEAD` response keeps the real `Content-Length`. A plain response carries
`Cache-Control: public, max-age=0, must-revalidate`. Use a `_headers` rule to
set a long `Cache-Control` on a content-hashed file.

A `binding` exposes `env.ASSETS.fetch()`, which takes a `Request`, a `URL`, or a
string. It answers `405` for a method other than `GET` or `HEAD`, and it always
applies `not_found_handling`.

Compiled Rust handlers use `context.assets("ASSETS")?.fetch_url(&context, url)`
for a GET, or `fetch(&context, request)` for a method and headers. The result
is the existing native HTTP response with its body stream. The binding resolves
assets in the invoking application's generation, including calls from a cell
still running the previous generation during reload. See the
[Rust-native migration](../rust-native-migration.md) for the execution path and
remaining deployment work.

Native Cargo project files declare `assets` inside their primary or cohosted
application configuration. `cellar-native build` stages the asset directory
relative to that project file, including `_headers` and `_redirects`.
`cellar-native deploy` verifies the staged indexes and bodies and publishes
them through the shared bucket protocol. It retains the same configuration
fields, compatibility settings, binding-name rules and limits described here.
The [compiled counter project](../../examples/native-counter-project.json)
includes an asset page and routes requests through a Rust `ASSETS` binding.

## Differences from Cloudflare

- cellar does not compress an asset response. Use a compressing ingress proxy
  when a client needs gzip or brotli.
- cellar has no edge cache. Each node keeps a 512 MiB disk cache, configured by
  `CELLAR_ASSET_CACHE_BYTES`, and requires browser revalidation.
- A node checks the deployment pointer after an asset miss at most once in five
  seconds.
- cellar reads `If-None-Match` and `If-Range`. It sends no `Last-Modified` header
  and reads no `If-Modified-Since` header.
- A `_headers` file cannot change `connection`, `content-length`, or
  `transfer-encoding`.
- A deployment can contain 20,000 assets and 1 GiB in total. A file can be at
  most 25 MiB. `CELLAR_MAX_ASSET_FILE_BYTES` changes the per-file limit, and it
  must have the same value on the deployment builder, the managed deployment
  agent, and the serving nodes. A process reads it once, so a new value needs
  a restart. An error for a file that is too large reports its path, its size,
  and the limit. Each `_headers` or `_redirects` file has a 100 KiB limit.
- `cellar deploy` accepts only `directory`, `binding`, `html_handling`,
  `not_found_handling`, and `run_worker_first` in the `assets` block.
- `cellar deploy` refuses a `.assetsignore` file, and it stops the deployment
  instead of ignoring the file. It also refuses a symbolic link, a special
  file, a non-UTF-8 name, an unsafe decoded path, and a `_worker.js` entry.
- The assets binding has one method, `fetch()`. cellar supplies no `unstable_`
  helper.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page
lists the runtime APIs and the unsupported services.
