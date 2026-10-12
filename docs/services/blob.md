<!-- Modified for Cellar from Celld. -->
# Blob

Blob provides an object storage API compatible with [Cloudflare R2](https://developers.cloudflare.com/r2/). A Worker reaches
a bucket through a `blob_buckets` binding. cellar stores the objects in the fleet
bucket that the node opened with `--bucket`, so Blob has the durability of that
object store. On a node without a fleet bucket, every Blob method fails. Read the
[Workers API reference](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/)
for the API.

## API

- `get(key, { range, onlyIf })` returns a `BlobObjectBody` with a streamed body,
  or `null`. A range is an `offset`, a `length`, a `suffix`, or a `Range`
  header.
- `head(key)` returns the object record without the body.
- `put(key, value, { httpMetadata, customMetadata, onlyIf })` takes a string,
  an `ArrayBuffer`, a typed array, a `Blob`, or a `ReadableStream`. A refused
  condition returns `null`.
- `delete(keys)` removes one key or a list of keys.
- `list({ prefix, delimiter, cursor, startAfter })` returns a page of objects.
- `createMultipartUpload(key)` returns a handle. `uploadPart(number, value)`
  sends one part, and `complete(parts)` assembles the object.

## Keys and storage

Each binding has its own `bucket_name`, and Cellar stores key `<key>` under
a reserved prefix for that binding. A key is 1 to 1024 bytes. A streamed `put()`
body that grows past 8 MiB becomes a multipart upload. `delete()` removes up to
1000 keys in one call. cellar has no public bucket URL, no presigned URL, and no
S3 endpoint, so put a Worker in front of the bytes to publish them.

The object store keeps some keys in a percent-encoded form: a key with a
non-ASCII character, a control character, or one of
``% \ { } ^ ` [ ] " < > ~ # | * ?``. For example, `přehled.html` becomes
`<binding-prefix>/p%C5%99ehled.html`. Each empty segment becomes `%`, so
`photos/` becomes `<binding-prefix>/photos/%`, and `a/b`, `/a/b`, `a//b`, and
`a/b/` are four different objects, as in Cloudflare R2. `list()` returns the
decoded key, but it sorts by the encoded form and compares `startAfter` with the
encoded form. Cloudflare R2 sorts by UTF-8 bytes. For example, Cloudflare R2
returns `a~` after `startAfter: "aa"`, but cellar compares `a%7E` with `aa` and
skips the key. A `cursor` from a cellar listing does not have this problem.

`list()` fails when it reaches an object that another tool wrote with a
percent-encoded key that is not UTF-8, such as `a%FF`.

Versions 0.5.1 and earlier removed each empty segment from a key. An object that
such a version wrote under `photos/` stays under `photos`, and `list()` returns
it as `photos`.

## Multipart uploads

A multipart upload lives on the node that opened it. Read
[Multipart upload](https://developers.cloudflare.com/r2/objects/multipart-objects/)
for the part rules. A part that arrives before its predecessor waits in memory,
up to 256 MiB, and cellar then refuses a further part. A waiting part can be
replaced. A part that already went to the object store cannot, and cellar
refuses that write.

## Differences from Cloudflare

- A Blob binding uses a reserved keyspace in the fleet bucket.
- The five content headers are stored as headers. `customMetadata`,
  `cacheExpiry`, the checksums, and the storage class are stored together as
  one JSON value. The stored prefix and metadata key retain their legacy
  spelling so existing objects remain readable; see
  [stored-state compatibility](../rust-native-migration.md#cellar-naming-and-stored-state-compatibility).
- An object another tool wrote still reads through the binding. Its user
  metadata becomes its `customMetadata`, and its headers become its
  `httpMetadata`. Use `cellar blob put` to write the complete record.
- `cellar blob get|head|put|delete|list` replaces `wrangler blob object`. The
  command reads the fleet bucket directly, so it needs no running node.
- An object `version` equals its content ETag, so identical content produces
  the same version.
- `ssecKey` and `jurisdiction` are unavailable.
- A conditional write cannot use a streamed body larger than 8 MiB.
- `createMultipartUpload()` does not accept a checksum.
- A multipart upload cannot resume on another node or after a restart. cellar
  also cannot replace a part that the object store already holds.
- Out-of-order parts can use at most 256 MiB of memory, and completion cannot
  change the stored part order.

The [Cloudflare compatibility](../cloudflare-compat.md#services) page lists the
runtime APIs and the unsupported services.
