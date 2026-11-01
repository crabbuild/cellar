#!/bin/sh
# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.
set -eu
: "${R2_BUCKET:?R2_BUCKET is required}"
: "${R2_ENDPOINT:?R2_ENDPOINT is required}"
export CELLAR_DURABILITY=bucket
exec /usr/local/bin/cellar-cloudflare-counter --bucket "s3://${R2_BUCKET}" --endpoint "$R2_ENDPOINT" --region auto --listen 0.0.0.0:8080 --internal-listen 127.0.0.1:9877 --advertise 127.0.0.1:9877 --data-dir /tmp/cellar-counter
