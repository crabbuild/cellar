"""Boundaries of the example's buffered ASGI transport, using real FastAPI."""
import asyncio
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "crates/cellar-python/python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from app import buffered_http, fetch
from cellar import Request


class BufferedHTTP(unittest.IsolatedAsyncioTestCase):
    async def test_scope_body_binary_and_repeated_headers(self):
        ctx = object()

        async def application(scope, receive, send):
            self.assertIs(scope["state"]["cellar"], ctx)
            self.assertEqual(scope["path"], "/café")
            self.assertEqual(scope["raw_path"], b"/caf%C3%A9")
            self.assertEqual(scope["query_string"], b"tag=a&tag=b")
            self.assertEqual(scope["headers"], [(b"x-tag", b"a"), (b"x-tag", b"b")])
            self.assertEqual(await receive(), {"type": "http.request", "body": b"\x00\xff", "more_body": False})
            await send({"type": "http.response.start", "status": 201,
                        "headers": [(b"set-cookie", b"a=1"), (b"set-cookie", b"b=2")]})
            await send({"type": "http.response.body", "body": b"\x00", "more_body": True})
            await send({"type": "http.response.body", "body": b"\xff"})

        result = await buffered_http(application, Request("https://orders/caf%C3%A9?tag=a&tag=b",
            "POST", b"\x00\xff", [("X-Tag", "a"), ("X-Tag", "b")]), ctx, max_response_bytes=2)
        self.assertEqual((result.status, result.body), (201, b"\x00\xff"))
        self.assertEqual(result.headers, [("set-cookie", "a=1"), ("set-cookie", "b=2")])

    async def test_overflow_and_protocol_failures_return_no_partial_response(self):
        cases = [
            [{"type": "http.response.body", "body": b"x"}],
            [{"type": "http.response.start", "status": 200}],
            [{"type": "http.response.start", "status": 200}, {"type": "http.response.start", "status": 201}],
            [{"type": "http.response.start", "status": 200}, {"type": "http.response.body", "body": b"abc"}],
            [{"type": "http.response.start", "status": 200, "trailers": True}],
            [{"type": "http.response.start", "status": 200, "headers": [(b"x", b"y")] * 129}],
            [{"type": "http.response.start", "status": 200, "headers": [(b"x", b"y" * 32768)]}],
            [{"type": "http.response.start", "status": 200}, {"type": "http.response.body"}, {"type": "http.response.body"}],
        ]
        for messages in cases:
            with self.subTest(messages=messages[:1]):
                async def application(scope, receive, send):
                    for message in messages:
                        await send(message)
                with self.assertRaises(ValueError):
                    await buffered_http(application, Request("https://orders/"), None, max_response_bytes=2)

    async def test_cancellation_reaches_application_after_partial_body(self):
        entered, cleaned = asyncio.Event(), asyncio.Event()

        async def application(scope, receive, send):
            try:
                await send({"type": "http.response.start", "status": 200})
                await send({"type": "http.response.body", "body": b"x", "more_body": True})
                await receive()
                entered.set()
                await receive()
            finally:
                cleaned.set()

        task = asyncio.create_task(buffered_http(application, Request("https://orders/"), None))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertTrue(cleaned.is_set())

    async def test_simultaneous_requests_keep_separate_contexts(self):
        ready = asyncio.Event()
        seen = []

        async def application(scope, receive, send):
            ctx = scope["state"]["cellar"]
            seen.append(ctx)
            if len(seen) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), 2)
            self.assertIs(scope["state"]["cellar"], ctx)
            await send({"type": "http.response.start", "status": 204})
            await send({"type": "http.response.body"})

        contexts = [object(), object()]
        await asyncio.gather(*(buffered_http(application, Request("https://orders/"), ctx) for ctx in contexts))
        self.assertEqual(seen, contexts)

    async def test_fastapi_validation_and_openapi_without_sdk_side_effects(self):
        result = await fetch(Request("https://orders/orders/example", "POST", b'{"items":[]}',
                                     [("content-type", "application/json")]), None)
        self.assertEqual(result.status, 422)
        schema = await fetch(Request("https://orders/openapi.json"), None)
        self.assertEqual(schema.status, 200)
        self.assertIn(b'"/orders/{order_id}"', schema.body)


if __name__ == "__main__":
    unittest.main()
