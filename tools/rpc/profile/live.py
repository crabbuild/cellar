# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""Real generated Python clients and handlers for the portable profile."""
from urllib.parse import parse_qsl, urlsplit

from cellar import Response
from google.protobuf import json_format
from profile_cellar import ProfileClient, ProfileRegistration
from live_profile_vectors import MESSAGES, VECTORS


class PythonCounter:
    async def echo(self, request, ctx):
        return request

    async def ping(self, request, ctx):
        return request

    async def echo_child(self, request, ctx):
        return request

    async def echo_timestamp(self, request, ctx):
        return request

    async def echo_duration(self, request, ctx):
        return request

    async def echo_mappings(self, request, ctx):
        return request


__cellar_rpc__ = {"PythonCounter": ProfileRegistration}
METHODS = {"Echo": "echo", "Ping": "ping", "EchoChild": "echo_child",
           "EchoTimestamp": "echo_timestamp", "EchoDuration": "echo_duration",
           "EchoMappings": "echo_mappings"}


async def fetch(request, ctx):
    stage = "select-vector"
    try:
        query = dict(parse_qsl(urlsplit(request.url).query))
        vector = VECTORS[query["vector"]]
        binding = {"rust": "RUST", "python": "PYTHON", "v8": "V8", "node": "NODE"}[query["target"]]
        stage = "bind-client"
        client = ProfileClient.bind(ctx, binding, "profile-" + vector["id"])
        stage = "encode-request"
        message = json_format.ParseDict(vector["json"], MESSAGES[vector["method"]]())
        stage = "native-rpc"
        reply = await getattr(client, METHODS[vector["method"]])(message)
        stage = "verify-reply"
        assert json_format.MessageToDict(reply) == vector["canonical"], vector["id"]
        stage = "encode-response"
        return Response.json({"id": vector["id"], "method": vector["method"], "verified": True})
    except Exception:
        # This finite qualification fixture reports only a fixed stage. Keep
        # exception text, class names and tracebacks out of the native response.
        # The matrix still requires success and fails on this diagnostic reply.
        return Response.json({"profile_failure_stage": stage}, status=500)
