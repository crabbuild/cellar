import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { configureTextEncoding } from "@bufbuild/protobuf/wire";
import { createCodec } from "../../packages/cellar-sdk/rpc/codec.js";
import { IncrementRequestSchema } from "./fixtures/generated/typescript/counter_pb.js";

const codec = createCodec({ create, fromBinary, toBinary, configureTextEncoding });
const message = create(IncrementRequestSchema, { delta: 9007199254740993n });
const bytes: Uint8Array = codec.encodeMessage(IncrementRequestSchema, message);
const decoded = fromBinary(IncrementRequestSchema, bytes);
const delta: bigint | undefined = decoded.delta;
void delta;

// This assertion must fail if generated i64 fields start accepting JS numbers.
// @ts-expect-error i64 application inputs require exact bigint values.
create(IncrementRequestSchema, { delta: 1 });
import type { Context } from "../../packages/cellar-sdk/index.js";
import { RpcClient } from "../../packages/cellar-sdk/rpc/index.js";

declare const context: Context;
const client = new RpcClient(context, "COUNTERS", "alice");
// @ts-expect-error Call timeouts are finite numeric durations, not strings.
client.call(undefined as never, {}, { timeoutMs: "17" });
