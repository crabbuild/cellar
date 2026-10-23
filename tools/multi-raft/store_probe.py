"""Exercise actual S3 conditional object operations against the private RustFS.

The object-store probe is independent of Cellar restore/acknowledgement proofs.
Only the Python standard library is required. Never serialize credentials.
"""

import datetime
import hashlib
import hmac
import urllib.error
import urllib.parse
import urllib.request


def probe(endpoint, bucket, access, secret):
    def request(method, path, body=b"", conditions=None):
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        date = timestamp[:8]
        digest = hashlib.sha256(body).hexdigest()
        headers = {
            "host": urllib.parse.urlsplit(endpoint).netloc,
            "x-amz-content-sha256": digest, "x-amz-date": timestamp,
            **(conditions or {}),
        }
        names = sorted(headers)
        signed = ";".join(names)
        canonical_headers = "".join(name + ":" + headers[name] + "\n" for name in names)
        canonical = f"{method}\n{path}\n\n{canonical_headers}\n{signed}\n{digest}"
        scope = date + "/us-east-1/s3/aws4_request"
        message = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
        key = ("AWS4" + secret).encode()
        for component in (date, "us-east-1", "s3", "aws4_request"):
            key = hmac.new(key, component.encode(), hashlib.sha256).digest()
        signature = hmac.new(key, message.encode(), hashlib.sha256).hexdigest()
        headers["authorization"] = (
            f"AWS4-HMAC-SHA256 Credential={access}/{scope}, "
            f"SignedHeaders={signed}, Signature={signature}"
        )
        query = urllib.request.Request(endpoint + path, data=body, method=method, headers=headers)
        try:
            response = urllib.request.urlopen(query, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.headers.get("ETag"), response.read(4096)

    observations = []

    def expect(name, expected, response, body=None):
        status, etag, actual = response
        observations.append({"operation": name, "status": status})
        if status != expected or (body is not None and actual != body):
            raise RuntimeError(f"S3 probe {name} failed: expected HTTP {expected}, observed {status}")
        return etag

    expect("create-bucket", 200, request("PUT", "/" + bucket))
    path = "/" + bucket + "/conditional-probe"
    first = expect("create-only", 200, request("PUT", path, b"first", {"if-none-match": "*"}))
    if not first:
        raise RuntimeError("S3 did not return the initial ETag")
    expect("get-matching", 200, request("GET", path, conditions={"if-match": first}), b"first")
    expect("refuse-overwrite", 412, request("PUT", path, b"wrong", {"if-none-match": "*"}))
    second = expect("compare-and-swap", 200, request("PUT", path, b"second", {"if-match": first}))
    if not second or second == first:
        raise RuntimeError("S3 did not return a changed ETag")
    expect("refuse-stale-cas", 412, request("PUT", path, b"wrong", {"if-match": first}))
    expect("read-after-cas", 200, request("GET", path, conditions={"if-match": second}), b"second")
    return observations
