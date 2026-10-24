"""Generated Python codec/client boundaries; real host proof is tested separately."""
import asyncio
from dataclasses import replace
import importlib.util
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "crates/cellar-python/python"))
sys.path.insert(0, str(ROOT / "tools/rpc/fixtures/generated/python"))
sys.path.insert(0, str(ROOT / "tools/rpc/fixtures/profile-generated/python"))
spec = importlib.util.spec_from_file_location("cellar_rpc", ROOT / "crates/cellar-python/python/rpc.py")
rpc = importlib.util.module_from_spec(spec)
sys.modules["cellar_rpc"] = rpc
spec.loader.exec_module(rpc)

from cellar import SDKError, ValidationError
from counter_cellar import CounterClient, CounterRegistration
from counter_pb2 import Count, GetCountRequest, GetCountResponse, IncrementRequest, IncrementResponse
from google.protobuf.struct_pb2 import Struct, Value
from google.protobuf.empty_pb2 import Empty
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration
from profile_pb2 import AllTypes, RuntimeMappings
from profile_cellar import ProfileRegistration
from cellar_rpc_shapes import CodecCatalog
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


def stale_fixture(name, mutate):
    directory = "generated" if name == "counter" else "profile-generated"
    contract = json.loads((ROOT / f"tools/rpc/fixtures/{directory}/rpc.json").read_text())
    image = descriptor_pb2.FileDescriptorSet.FromString((ROOT / f"tools/rpc/fixtures/{name}.binpb").read_bytes())
    file = next(file for file in image.file if not file.name.startswith("google/"))
    mutate(file)
    pool = descriptor_pool.DescriptorPool()
    for file in image.file:
        pool.Add(file)
    def message(name):
        return message_factory.GetMessageClass(pool.FindMessageTypeByName(name))
    return contract, message


class Budget:
    """Deterministic stand-in for the opaque native budget, never guest time."""
    def __init__(self, context, started=None, deadline=None):
        self.context = context
        self.started = context.elapsed if started is None else started
        self.deadline = min(self.started + context.host_timeout, context.parent_deadline) if deadline is None else deadline

    def limit(self, timeout):
        return Budget(self.context, self.started, min(self.deadline, self.started + timeout))

    def remaining_ms(self):
        remaining = self.deadline - self.context.elapsed
        if remaining <= 0:
            raise RuntimeError("DeadlineExceeded")
        return remaining


class Context:
    def __init__(self, reply=b""):
        self.reply = reply
        self.calls = []
        self.active = True
        self.elapsed = 0
        self.host_timeout = 5000
        self.parent_deadline = float("inf")
        contract = json.loads((ROOT / "tools/rpc/fixtures/generated/rpc.json").read_text())
        self.codecs = CodecCatalog({contract["digest"]: contract})

    def _check(self):
        if not self.active:
            raise SDKError("ContextExpired", "ContextExpired")

    async def _call(self, operation, **metadata):
        self.calls.append((operation, metadata))
        return None, self.reply

    def _verify_rpc(self, method):
        return self.codecs.verify(method)

    def _rpc_budget(self):
        return Budget(self)


class PythonRpc(unittest.TestCase):
    def setUp(self):
        self.increment = CounterRegistration.methods["example.counter.v1.Counter/Increment"]

    def test_optional_presence_and_i64_extremes(self):
        self.assertEqual(rpc.encode_message(IncrementRequest(), self.increment), b"")
        self.assertEqual(rpc.encode_message(IncrementRequest(delta=0), self.increment), b"\x08\x00")
        for value in [-(2**63), -1, 0, 2**53 + 1, 2**63 - 1]:
            encoded = rpc.encode_message(IncrementRequest(delta=value), self.increment)
            self.assertEqual(IncrementRequest.FromString(encoded).delta, value)
        with self.assertRaises(ValueError):
            IncrementRequest(delta=2**63)

    def test_actual_codec_shapes_cover_all_generated_roots(self):
        for registration, directory in [(CounterRegistration, "generated"), (ProfileRegistration, "profile-generated")]:
            contract = json.loads((ROOT / f"tools/rpc/fixtures/{directory}/rpc.json").read_text())
            catalog = CodecCatalog({contract["digest"]: contract})
            for method in registration.methods.values():
                catalog.verify(method)
                catalog.verify(method)  # Shared recursive descriptors are memoized.

    def test_stale_codec_fields_maps_enums_and_presence_are_refused(self):
        def field(message, name):
            return next(field for field in message.field if field.name == name)
        def optional(file):
            value = file.message_type[0].field[0]
            value.proto3_optional = False
            value.ClearField("oneof_index")
            file.message_type[0].ClearField("oneof_decl")
        mutations = [
            ("counter", lambda file: setattr(file.message_type[0].field[0], "number", 99)),
            ("counter", lambda file: setattr(file.message_type[0].field[0], "type", 4)),
            ("counter", optional),
            ("profile", lambda file: setattr(field(file.message_type[0], "packed").options, "packed", False)),
            ("profile", lambda file: setattr(file.message_type[0].enum_type[0].value[1], "number", 77)),
            ("profile", lambda file: setattr(file.message_type[0].nested_type[1].field[0], "type", 5)),
            ("profile", lambda file: field(file.message_type[0], "selected_text").ClearField("oneof_index")),
        ]
        for name, mutate in mutations:
            contract, message = stale_fixture(name, mutate)
            original = self.increment if name == "counter" else ProfileRegistration.methods["cellar.rpc.conformance.v1.Profile/Echo"]
            method = replace(original, input=message(original.input.DESCRIPTOR.full_name), output=message(original.output.DESCRIPTOR.full_name))
            with self.subTest(name=name, mutate=mutate), self.assertRaises(TypeError):
                CodecCatalog({contract["digest"]: contract}).verify(method)

    def test_stale_provider_is_refused_by_cold_runner_validation(self):
        import runner
        contract, message = stale_fixture("counter", lambda file: setattr(file.message_type[0].field[0], "type", 4))
        constructors = []
        class Counter:
            def __init__(self):
                constructors.append(True)
            async def increment(self, request, ctx):
                raise AssertionError("Must not run")
            async def get_count(self, request, ctx):
                raise AssertionError("Must not run")
        rules = [replace(rule, input=message(rule.input.DESCRIPTOR.full_name), output=message(rule.output.DESCRIPTOR.full_name))
                 for rule in CounterRegistration.methods.values()]
        methods = {rule.name: dict(digest=rule.digest, input=rule.input.DESCRIPTOR.full_name,
                                  output=rule.output.DESCRIPTOR.full_name, limits=contract["limits"]) for rule in rules}
        app = SimpleNamespace(Counter=Counter, __cellar_rpc__={"Counter":rpc.RpcRegistration(rules)})
        descriptor = dict(events=[], cells={"Counter":[]}, rpc={"Counter":methods}, rpc_contracts={contract["digest"]:contract})
        with self.assertRaises(TypeError):
            runner.validate(app,json.dumps(descriptor))
        self.assertEqual(constructors,[])

    def test_stale_client_never_encodes_or_submits(self):
        from cellar import Context as NativeContext, _current_invocation
        contract, message = stale_fixture("counter", lambda file: setattr(file.message_type[0].field[0], "type", 4))
        method = replace(self.increment, input=message(self.increment.input.DESCRIPTOR.full_name))
        class Bridge:
            def active(self): return True
            def rpc_budget(self): return Budget(Context())
            def submit(self, *args): raise AssertionError("Must not submit")
        async def run():
            bridge = Bridge()
            token = _current_invocation.set(bridge)
            try:
                context = NativeContext(bridge,None)
                context._rpc_codecs = CodecCatalog({contract["digest"]:contract})
                client = rpc.RpcClient(context,"COUNTERS","alice")
                with patch.object(rpc,"encode_message",side_effect=AssertionError("Must not encode")) as encode:
                    with self.assertRaises(ValidationError) as caught:
                        await client._invoke(method,method.input(delta=1))
                    self.assertEqual((caught.exception.code,caught.exception.outcome),("ContractMismatch","not_committed"))
                    encode.assert_not_called()
            finally:
                _current_invocation.reset(token)
        asyncio.run(run())

    def test_copying_a_descriptor_onto_an_old_message_class_is_not_codec_evidence(self):
        contract, message = stale_fixture("counter", lambda file: setattr(file.message_type[0].field[0], "type", 4))
        old = message(self.increment.input.DESCRIPTOR.full_name)
        old.DESCRIPTOR = self.increment.input.DESCRIPTOR
        method = replace(self.increment,input=old)
        with self.assertRaises(TypeError):
            CodecCatalog({contract["digest"]:contract}).verify(method)

    def test_client_encoding_uses_native_contract_limits(self):
        async def run():
            context = Context()
            contract = json.loads((ROOT / "tools/rpc/fixtures/generated/rpc.json").read_text())
            contract["limits"]["max_message_bytes"] = 128
            context.codecs = CodecCatalog({contract["digest"]:contract})
            with self.assertRaises(ValidationError):
                await CounterClient.bind(context,"COUNTERS","alice").increment(IncrementRequest(delta=1,operation_id="k" * 128))
            self.assertEqual(context.calls,[])
        asyncio.run(run())

    def test_client_layout_uses_native_slots_before_serialization_or_submission(self):
        async def run():
            context = Context()
            contract = json.loads((ROOT / "tools/rpc/fixtures/generated/rpc.json").read_text())
            contract["limits"]["max_decoded_slots"] = 4
            context.codecs = CodecCatalog({contract["digest"]:contract})
            with patch.object(IncrementRequest,"SerializeToString",side_effect=AssertionError("must not serialize")) as encode:
                with self.assertRaises(ValidationError) as caught:
                    await CounterClient.bind(context,"COUNTERS","alice").increment(IncrementRequest(delta=1,operation_id="id"))
                self.assertEqual((caught.exception.code,caught.exception.outcome),("ResourceExhausted","not_committed"))
                encode.assert_not_called()
            self.assertEqual(context.calls,[])
        asyncio.run(run())

    def test_profile_preserves_scalars_maps_presence_and_well_known_messages(self):
        method = ProfileRegistration.methods["cellar.rpc.conformance.v1.Profile/Echo"]
        message = AllTypes(
            signed32=-(2**31), unsigned32=2**32 - 1, zigzag32=-(2**31),
            fixed_unsigned32=2**32 - 1, fixed_signed32=-(2**31),
            signed64=-(2**63), unsigned64=2**64 - 1, zigzag64=-(2**63),
            fixed_unsigned64=2**64 - 1, fixed_signed64=-(2**63),
            float32=float("inf"), float64=float("-inf"), enabled=True,
            text="\ufeff😀\0", data=bytes([0, 255, 128]), state=777,
            present=0, selected_text="",
            packed=[-(2**31), -1, 0, 2**31 - 1], unpacked=[-(2**31), -1, 0, 2**31 - 1],
            by_name={"\ufeff😀": AllTypes.Child(name="nested")},
            by_ID={-(2**63): True, 2**63 - 1: False},
            timestamp=Timestamp(seconds=-1, nanos=1), duration=Duration(seconds=-1, nanos=-1),
        )
        reply = AllTypes.FromString(rpc.encode_message(message, method))
        self.assertEqual(reply, message)
        self.assertTrue(reply.HasField("present"))
        self.assertEqual(reply.WhichOneof("choice"), "selected_text")

    def test_generated_empty_registration_encodes_zero_bytes(self):
        method = ProfileRegistration.methods["cellar.rpc.conformance.v1.Profile/Ping"]
        self.assertIs(method.input, Empty)
        self.assertEqual(rpc.encode_message(Empty(), method), b"")
        self.assertEqual(method.output.FromString(b""), Empty())

    def test_runtime_mapping_vectors_match_native_and_js_wire_budgets(self):
        vectors = json.loads((ROOT / "tools/rpc/fixtures/message-vectors.json").read_text())
        self.assertEqual(len(vectors), 3)
        for vector in vectors:
            data = bytes(vector["bytes"])
            message = RuntimeMappings.FromString(data)
            rule = replace(self.increment, max_depth=vector["max_depth"], max_elements=vector["max_elements"], max_decoded_slots=vector["max_decoded_slots"])
            self.assertEqual(rpc.encode_message(message, rule), data)
            if vector["name"] == "present-wrapper-zero":
                self.assertTrue(message.HasField("wrapped64"))
                self.assertEqual(message.wrapped64.value, 0)
            elif vector["name"] == "wrapper-map":
                self.assertEqual(message.wrapper_map[True].value, 1)
            elif vector["name"] == "struct-number":
                self.assertEqual(message.object.fields["x"].number_value, 7.0)
            else:
                self.fail("Unknown runtime mapping vector")
            for field in ("max_depth", "max_elements", "max_decoded_slots"):
                if getattr(rule, field) > 1:
                    with self.assertRaises(ValidationError):
                        rpc.encode_message(message, replace(rule, **{field: getattr(rule, field) - 1}))

    def test_limits_refuse_before_serialization(self):
        for message, rule in [
            (IncrementRequest(delta=1), replace(self.increment, max_message_bytes=1)),
            (IncrementRequest(delta=1, operation_id="id"), replace(self.increment, max_elements=1)),
            (IncrementResponse(count=Count(value=1)), replace(self.increment, max_depth=0)),
        ]:
            with self.assertRaises(ValidationError) as caught:
                rpc.encode_message(message, rule)
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("InvalidArgument", "not_committed"))

    def test_default_layout_is_refused_before_sizing_or_serialization(self):
        from google.protobuf.message import Message
        class AllocationSpy(Message):
            DESCRIPTOR = SimpleNamespace(fields=[None] * 512)
            def ListFields(self): raise AssertionError("Must not inspect expanded message")
            def ByteSize(self): raise AssertionError("Must not size")
            def SerializeToString(self): raise AssertionError("Must not serialize")
        with self.assertRaises(ValidationError) as caught:
            rpc.encode_message(AllocationSpy(),replace(self.increment,max_decoded_slots=512))
        self.assertEqual((caught.exception.code,caught.exception.outcome),("ResourceExhausted","not_committed"))

    def test_map_entries_consume_depth_and_wire_element_budgets(self):
        # The map is a repeated entry message with key/value fields; Value
        # contributes one more field. Count that structure before the codec.
        message = Struct(fields={"x": Value(number_value=7.0)})
        rule = replace(self.increment, max_depth=2, max_elements=4)
        self.assertEqual(Struct.FromString(rpc.encode_message(message, rule)), message)
        for rule in [replace(rule, max_depth=1), replace(rule, max_elements=3)]:
            with self.assertRaises(ValidationError):
                rpc.encode_message(message, rule)

    def test_generated_client_uses_owned_binary_and_exact_method(self):
        async def run():
            context = Context(GetCountResponse(count=Count(value=2**53 + 1)).SerializeToString())
            client = CounterClient.bind(context, "COUNTERS", "alice")
            response = await client.get_count(GetCountRequest(), timeout_ms=17)
            self.assertEqual(response.count.value, 2**53 + 1)
            operation, metadata = context.calls[0]
            self.assertEqual(operation, "cell.call")
            self.assertEqual(metadata["method"], "example.counter.v1.Counter/GetCount")
            self.assertEqual(metadata["binding"], "COUNTERS")
            self.assertEqual(metadata["name"], "alice")
            self.assertEqual(metadata["timeout_ms"], 17)
            self.assertEqual(metadata["_body"], b"")
        asyncio.run(run())

    def test_invalid_input_timeout_and_expired_context_do_not_submit(self):
        async def run():
            context = Context()
            client = CounterClient.bind(context, "COUNTERS", "alice")
            for request, timeout in [(IncrementRequest(), None), (GetCountRequest(), -1), (GetCountRequest(), True)]:
                with self.assertRaises(ValidationError):
                    await client.get_count(request, timeout_ms=timeout)
            context.active = False
            with self.assertRaises(SDKError) as caught:
                await client.get_count(GetCountRequest())
            self.assertEqual(caught.exception.code, "ContextExpired")
            self.assertEqual(context.calls, [])
        asyncio.run(run())

    def test_bad_reply_keeps_effects_unknown(self):
        async def run():
            context = Context(b"\x0a\xff")
            client = CounterClient.bind(context, "COUNTERS", "alice")
            with self.assertRaises(SDKError) as caught:
                await client.get_count(GetCountRequest())
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("InvalidResponse", "unknown"))
            self.assertEqual(len(context.calls), 1)
        asyncio.run(run())

    def test_codec_budget_starts_before_verification_and_inherits_caps(self):
        async def run(timeout, host, parent):
            context = Context()
            context.host_timeout, context.parent_deadline = host, parent
            verify = context._verify_rpc
            def slow_verify(method):
                context.elapsed += 11
                return verify(method)
            context._verify_rpc = slow_verify
            with patch.object(rpc, "encode_message", side_effect=AssertionError("Expired call reached encoder")):
                with self.assertRaises(SDKError) as caught:
                    await CounterClient.bind(context, "COUNTERS", "alice").get_count(GetCountRequest(), timeout_ms=timeout)
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("DeadlineExceeded", "not_committed"))
            self.assertEqual(context.calls, [])
        for timeout, host, parent in [(10, 5000, 5000), (None, 10, 5000), (60000, 5000, 10)]:
            with self.subTest(timeout=timeout, host=host, parent=parent):
                asyncio.run(run(timeout, host, parent))

    def test_encoder_time_is_subtracted_without_a_fresh_native_window(self):
        async def run():
            context = Context()
            encode = rpc.encode_message
            def slow_encode(message, method):
                context.elapsed += 7
                return encode(message, method)
            with patch.object(rpc, "encode_message", slow_encode):
                await CounterClient.bind(context, "COUNTERS", "alice").get_count(GetCountRequest(), timeout_ms=10)
            self.assertEqual(context.calls[0][1]["timeout_ms"], 3)
            self.assertIsInstance(context.calls[0][1]["_rpc_budget"], Budget)
        asyncio.run(run())

    def test_zero_budget_refuses_before_any_codec(self):
        async def run():
            context = Context()
            context._verify_rpc = lambda _: self.fail("Expired call reached codec verification")
            with patch.object(rpc, "encode_message", side_effect=AssertionError("Must not encode")):
                with self.assertRaises(SDKError) as caught:
                    await CounterClient.bind(context, "COUNTERS", "alice").get_count(GetCountRequest(), timeout_ms=0)
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("DeadlineExceeded", "not_committed"))
            self.assertEqual(context.calls, [])
        asyncio.run(run())

    def test_late_reply_delivery_refuses_before_decoding_with_unknown_effects(self):
        async def run():
            context = Context()
            call = context._call
            async def late_delivery(*args, **kwargs):
                context.elapsed += 10
                return await call(*args, **kwargs)
            context._call = late_delivery
            with patch.object(GetCountResponse, "FromString", side_effect=AssertionError("Must not decode late reply")):
                with self.assertRaises(SDKError) as caught:
                    await CounterClient.bind(context, "COUNTERS", "alice").get_count(GetCountRequest(), timeout_ms=10)
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("DeadlineExceeded", "unknown"))
            self.assertEqual(len(context.calls), 1)
        asyncio.run(run())

    def test_expired_encoding_never_submits_and_reply_decode_keeps_unknown(self):
        async def run():
            context = Context()
            encode = rpc.encode_message
            def expired_encode(message, method):
                context.elapsed += 10
                return encode(message, method)
            with patch.object(rpc, "encode_message", expired_encode):
                with self.assertRaises(SDKError) as caught:
                    await CounterClient.bind(context, "COUNTERS", "alice").get_count(GetCountRequest(), timeout_ms=10)
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("DeadlineExceeded", "not_committed"))
            self.assertEqual(context.calls, [])

            context = Context()
            decode = GetCountResponse.FromString
            def slow_decode(body):
                context.elapsed += 10
                return decode(body)
            with patch.object(GetCountResponse, "FromString", slow_decode):
                with self.assertRaises(SDKError) as caught:
                    await CounterClient.bind(context, "COUNTERS", "alice").get_count(GetCountRequest(), timeout_ms=10)
            self.assertEqual((caught.exception.code, caught.exception.outcome), ("DeadlineExceeded", "unknown"))
            self.assertEqual(len(context.calls), 1)
        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
