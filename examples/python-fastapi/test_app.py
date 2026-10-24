"""Boundaries of the example's buffered ASGI transport, using real FastAPI."""
import asyncio
import copy
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "crates/cellar-python/python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from app import Order, buffered_http, fetch
from cellar import CellIdentity, Conflict, Request, Snapshot


class OrderSnapshots(unittest.IsolatedAsyncioTestCase):
    def context(self, present=True):
        order = dict(order_id='example', customer_id='customer',
                     items=[dict(sku='widget', quantity=1, unit_price_cents=1250)], total_cents=1250,
                     status='pending', version=1, created_at_ms=1000)
        snapshot = Snapshot(present, order if present else None)
        storage = SimpleNamespace(read_json=AsyncMock(side_effect=AssertionError('unexpected live read')),
                                  apply=AsyncMock())
        return SimpleNamespace(initial_storage={'order': snapshot}, storage=storage,
                               now_ms=AsyncMock(return_value=1000)), snapshot

    async def test_reads_use_captured_present_or_missing_order(self):
        for present in (True, False):
            with self.subTest(present=present):
                ctx, snapshot = self.context(present)
                response = await Order().fetch(Request('https://orders/example'), ctx)
                self.assertEqual(response.status, 200 if present else 404)
                if present:
                    self.assertEqual(json.loads(response.body), snapshot.value)
                ctx.storage.read_json.assert_not_awaited()
                ctx.storage.apply.assert_not_awaited()

    async def test_missing_declaration_cannot_fall_back_to_a_live_read(self):
        ctx, _ = self.context()
        ctx.initial_storage = {}
        with self.assertRaises(KeyError):
            await Order().fetch(Request('https://orders/example'), ctx)
        ctx.storage.read_json.assert_not_awaited()

    async def test_patch_checks_original_snapshot_and_does_not_retry_conflicts(self):
        for conflict in (False, True):
            with self.subTest(conflict=conflict):
                ctx, snapshot = self.context()
                original = copy.deepcopy(snapshot.value)
                if conflict:
                    ctx.storage.apply.side_effect = Conflict('intervening writer')
                response = await Order().fetch(Request('https://orders/example', 'PATCH',
                    b'{"expected_version":1,"status":"confirmed"}'), ctx)
                self.assertEqual(response.status, 409 if conflict else 200)
                ctx.storage.apply.assert_awaited_once_with(checks={'order': snapshot},
                    puts={'order': dict(original, version=2, status='confirmed')})
                self.assertEqual(snapshot.value, original)
                ctx.storage.read_json.assert_not_awaited()

    async def test_create_keeps_receipt_and_order_in_one_checked_batch(self):
        ctx, snapshot = self.context(False)
        receipt = Snapshot(False, None)
        ctx.storage.read_json.side_effect = None
        ctx.storage.read_json.return_value = receipt
        payload = dict(customer_id='customer', items=[dict(sku='widget', quantity=2, unit_price_cents=1250)])
        response = await Order().fetch(Request('https://orders/example', 'POST', json.dumps(payload)), ctx)
        self.assertEqual(response.status, 201)
        ctx.storage.read_json.assert_awaited_once_with('creation')
        ctx.storage.apply.assert_awaited_once()
        batch = ctx.storage.apply.await_args.kwargs
        self.assertEqual(batch['checks'], {'order': snapshot, 'creation': receipt})
        self.assertEqual(batch['puts']['order'], json.loads(response.body))
        self.assertEqual(batch['puts']['creation']['payload'], payload)
        self.assertEqual(batch['puts']['creation']['response']['body'], response.body.decode('ascii'))

    async def test_direct_cell_fastapi_matches_python_gateway_validation_and_results(self):
        payload = dict(customer_id='customer', items=[dict(sku='widget', quantity=2, unit_price_cents=1250)])
        cases = [('GET', None, True, 200), ('GET', None, False, 404),
                 ('POST', payload, False, 201), ('POST', {'items': []}, False, 422),
                 ('POST', payload, True, 409),
                 ('PATCH', {'expected_version': 1, 'status': 'confirmed'}, True, 200),
                 ('PATCH', {'expected_version': 2, 'status': 'confirmed'}, True, 409),
                 ('PATCH', {'expected_version': 1, 'status': 'unknown'}, True, 422)]
        for method, payload, present, status in cases:
            with self.subTest(method=method, status=status):
                observed = []
                for direct in (False, True):
                    ctx, _ = self.context(present)
                    ctx.identity = CellIdentity('example', 'Order', 'test-scope', 1)
                    ctx.storage.read_json.side_effect = None
                    ctx.storage.read_json.return_value = Snapshot(False, None)
                    cell = Order()

                    async def dispatch(request):
                        return await cell.fetch(request, ctx)

                    forwarded = AsyncMock(side_effect=dispatch)
                    namespace = SimpleNamespace(get_by_name=lambda name: SimpleNamespace(fetch=forwarded))
                    outer = SimpleNamespace(identity=None, namespace=lambda name: namespace)
                    request = Request('https://orders/orders/example?tag=a&tag=b', method,
                        b'' if payload is None else json.dumps(payload), [('content-type', 'application/json')])
                    response = await (cell.fetch(request, ctx) if direct else fetch(request, outer))
                    self.assertEqual(response.status, status)
                    if direct:
                        forwarded.assert_not_awaited()
                    elif status != 422:
                        forwarded.assert_awaited_once()
                    else:
                        forwarded.assert_not_awaited()
                    observed.append((response.status, response.body, response.headers,
                                     ctx.storage.apply.await_args_list, ctx.storage.read_json.await_args_list))
                self.assertEqual(observed[0], observed[1])

    async def test_direct_route_cannot_use_another_order_identity(self):
        ctx, _ = self.context()
        ctx.identity = CellIdentity('different', 'Order', 'test-scope', 1)
        with self.assertRaisesRegex(RuntimeError, 'does not match'):
            await Order().fetch(Request('https://orders/orders/example'), ctx)
        ctx.storage.read_json.assert_not_awaited()
        ctx.storage.apply.assert_not_awaited()


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
