# Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

"""FastAPI order service hosted by Cellar's buffered Python HTTP adapter."""
import asyncio
import os
import sys
from typing import Annotated, Literal
from urllib.parse import unquote, urlsplit

import fastapi
from fastapi import Depends, FastAPI, Path, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field
from cellar import Conflict, Context, Request as CellRequest, Response as CellResponse, SDKError


async def buffered_http(application, request, ctx, *, max_response_bytes=1024 * 1024):
    """Example-only ASGI HTTP transport. Cellar owns I/O and invocation lifetime.

    Complete finite responses are buffered; no lifespan, WebSockets, socket
    extensions or client-address forwarding. Cancellation propagates unchanged.
    """
    url = urlsplit(request.url)
    scope = {
        "type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1", "method": request.method, "scheme": url.scheme,
        "path": unquote(url.path), "raw_path": url.path.encode("ascii"),
        "query_string": url.query.encode("ascii"), "root_path": "",
        "headers": [(key.lower().encode("ascii"), value.encode("latin-1"))
                    for key, value in request.headers],
        "server": (url.hostname, url.port or (443 if url.scheme == "https" else 80)),
        "client": None, "state": {"cellar": ctx}, "extensions": {},
    }
    status, headers, body = None, [], bytearray()
    received, complete = False, False

    async def receive():
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": request.body, "more_body": False}
        # There is no socket receive loop here. Further receives wait for native
        # invocation cancellation rather than fabricating a client disconnect.
        await asyncio.Future()

    async def send(message):
        nonlocal status, headers, complete
        if complete:
            raise ValueError("ASGI response already complete")
        if message["type"] == "http.response.start":
            if status is not None or message.get("trailers", False):
                raise ValueError("Unsupported ASGI response start")
            status = message["status"]
            if not isinstance(status, int) or not 200 <= status <= 599:
                raise ValueError("Invalid ASGI response status")
            raw = message.get("headers", [])
            if len(raw) > 128 or sum(len(k) + len(v) for k, v in raw) > 32768:
                raise ValueError("ASGI response headers exceed limit")
            headers = [(key.decode("ascii"), value.decode("latin-1")) for key, value in raw]
        elif message["type"] == "http.response.body" and status is not None:
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > max_response_bytes:
                raise ValueError("ASGI response exceeds byte limit")
            body.extend(chunk)
            complete = not message.get("more_body", False)
        else:
            raise ValueError("Unsupported ASGI HTTP event")

    await application(scope, receive, send)
    if not complete:
        raise ValueError("ASGI application returned an incomplete response")
    return CellResponse(bytes(body), status, headers)


app = FastAPI(title="Cellar Orders", version="1.0.0")


async def cellar_context(request: Request) -> Context:
    return request.state.cellar


Cellar = Annotated[Context, Depends(cellar_context)]
OrderID = Annotated[str, Path(pattern=r"^[a-zA-Z0-9_-]{1,64}$")]


class Item(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sku: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    quantity: int = Field(strict=True, ge=1, le=1000)
    unit_price_cents: int = Field(strict=True, ge=1, le=1_000_000_000)


class CreateOrder(BaseModel):
    model_config = ConfigDict(extra="forbid")
    customer_id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,64}$")
    items: list[Item] = Field(min_length=1, max_length=20)


class ChangeStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_version: int = Field(strict=True, ge=1)
    status: Literal["confirmed", "cancelled"]


class OrderView(CreateOrder):
    order_id: str
    total_cents: int
    status: Literal["pending", "confirmed", "cancelled"]
    version: int
    created_at_ms: int


@app.exception_handler(SDKError)
async def sdk_failure(request: Request, error: SDKError):
    # No automatic mutation retries or claim that an unknown result rolled back.
    return JSONResponse({"detail": "Cellar operation failed", "code": error.code,
                         "outcome": error.outcome}, status_code=503)


@app.get("/health")
async def health():
    headers = {}
    if instance := os.environ.get("CELLAR_SUPERVISOR_INSTANCE"):
        headers["x-cellar-supervisor-instance"] = instance
    return JSONResponse({"framework": "fastapi", "version": fastapi.__version__,
                         "free_threaded": "t" in sys.abiflags,
                         "gil_enabled": sys._is_gil_enabled()}, headers=headers)


async def forward(ctx, order_id, method, payload=None):
    request = CellRequest("https://orders/" + order_id, method,
                          b"" if payload is None else CellResponse.json(payload).body)
    if ctx.identity is None:
        result = await ctx.namespace("ORDERS").get_by_name(order_id).fetch(request)
    else:
        if ctx.identity.class_name != "Order" or ctx.identity.name != order_id:
            raise RuntimeError("Order route does not match its cell")
        result = await Order.handle(request, ctx)
    response = Response(result.body, status_code=result.status)
    response.raw_headers = [(k.encode("ascii"), v.encode("latin-1")) for k, v in result.headers]
    return response


@app.post("/orders/{order_id}", response_model=OrderView, status_code=201)
async def create_order(order_id: OrderID, order: CreateOrder, ctx: Cellar):
    return await forward(ctx, order_id, "POST", order.model_dump())


@app.get("/orders/{order_id}", response_model=OrderView)
async def read_order(order_id: OrderID, ctx: Cellar):
    return await forward(ctx, order_id, "GET")


@app.patch("/orders/{order_id}", response_model=OrderView)
async def change_status(order_id: OrderID, change: ChangeStatus, ctx: Cellar):
    return await forward(ctx, order_id, "PATCH", change.model_dump())


async def fetch(request, ctx):
    return await buffered_http(app, request, ctx)


def render(order, status=200):
    response = CellResponse.json(order, status)
    response.headers.append(("etag", '"' + str(order["version"]) + '"'))
    return response


class Order:
    async def fetch(self, request, ctx):
        if request.path.startswith("/orders/"):
            return await buffered_http(app, request, ctx)
        return await self.handle(request, ctx)

    @staticmethod
    async def handle(request, ctx):
        # The compiled host captures this invocation's state before Python
        # dispatch. Checked writes still detect any intervening writer.
        current = ctx.initial_storage["order"]
        if request.method == "GET":
            return render(current.value) if current.present else CellResponse.json({"detail": "Order not found"}, 404)
        if request.method == "POST":
            payload = CreateOrder.model_validate(await request.json()).model_dump()
            receipt = await ctx.storage.read_json("creation")
            if receipt.present:
                if receipt.value["payload"] != payload:
                    return CellResponse.json({"detail": "Order ID already used for a different order"}, 409)
                saved = receipt.value["response"]
                return CellResponse(saved["body"], saved["status"], saved["headers"])
            if current.present:
                return CellResponse.json({"detail": "Order already exists"}, 409)
            order = dict(payload, order_id=request.path.lstrip("/"), status="pending", version=1,
                         created_at_ms=await ctx.now_ms(),
                         total_cents=sum(item["quantity"] * item["unit_price_cents"] for item in payload["items"]))
            response = render(order, 201)
            puts = {"order": order, "creation": {"payload": payload, "response": {
                "body": response.body.decode("ascii"), "status": response.status, "headers": response.headers}}}
            checks = {"order": current, "creation": receipt}
        elif request.method == "PATCH" and current.present:
            change = ChangeStatus.model_validate(await request.json())
            if current.value["version"] != change.expected_version or current.value["status"] != "pending":
                return CellResponse.json({"detail": "Order version or status conflict"}, 409)
            order = dict(current.value, status=change.status, version=change.expected_version + 1)
            response, checks, puts = render(order), {"order": current}, {"order": order}
        else:
            return CellResponse.json({"detail": "Order not found"}, 404)
        try:
            await ctx.storage.apply(checks=checks, puts=puts)
        except Conflict:
            return CellResponse.json({"detail": "Concurrent update; read state and reconcile"}, 409)
        return response
