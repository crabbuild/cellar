// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

import { jsonResponse, type Context, type Request } from "@cellar/sdk";
import { fromJson, toJson, type JsonValue } from "@bufbuild/protobuf";
import { DurationSchema, EmptySchema, TimestampSchema, type Duration, type Empty, type Timestamp } from "@bufbuild/protobuf/wkt";
import { AllTypesSchema, AllTypes_ChildSchema, RuntimeMappingsSchema, type AllTypes, type AllTypes_Child, type RuntimeMappings } from "../fixtures/profile-generated/typescript/profile_pb.js";
import type { ProfileClient, ProfileHandler } from "../fixtures/profile-generated/typescript/profile_cellar.js";
import { vectors } from "./corpus.js";

/** Business methods receive and return generated messages through the real SDK. */
export class Echo implements ProfileHandler {
  async echo(request: AllTypes) { return request; }
  async ping(request: Empty) { return request; }
  async echoChild(request: AllTypes_Child) { return request; }
  async echoTimestamp(request: Timestamp) { return request; }
  async echoDuration(request: Duration) { return request; }
  async echoMappings(request: RuntimeMappings) { return request; }
}

type Calls = Pick<ProfileClient, "echo" | "ping" | "echoChild" | "echoTimestamp" | "echoDuration" | "echoMappings">;
type Client = {bind(context: Context, binding: string, name: string): Calls};

function canonical(value: JsonValue): JsonValue {
  if (Array.isArray(value)) return value.map(canonical);
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  }
  return value;
}

/** Each HTTP request submits one named call; HTTP is qualification ingress only. */
export function handler(Client: Client) {
  return async (request: Request, context: Context) => {
    const query = Object.fromEntries((request.url.split("?")[1] ?? "").split("&").filter(Boolean).map(entry => {
      const [key, value = ""] = entry.split("=");
      return [decodeURIComponent(key), decodeURIComponent(value)];
    }));
    const vector = vectors.find(candidate => candidate.id === query.vector);
    const bindings: Record<string, string> = {rust:"RUST", python:"PYTHON", v8:"V8", node:"NODE"};
    if (!vector || !Object.hasOwn(bindings, query.target)) throw Error("Invalid profile case");
    const client = Client.bind(context, bindings[query.target], `profile-${vector.id}`);
    // The fixture is frozen JSON. Each branch uses its independently generated,
    // typed business method and the appropriate generated message descriptor.
    const input = vector.json as JsonValue;
    let output: JsonValue;
    switch (vector.method) {
      case "Echo": output = toJson(AllTypesSchema, await client.echo(fromJson(AllTypesSchema, input))); break;
      case "Ping": output = toJson(EmptySchema, await client.ping(fromJson(EmptySchema, input))); break;
      case "EchoChild": output = toJson(AllTypes_ChildSchema, await client.echoChild(fromJson(AllTypes_ChildSchema, input))); break;
      case "EchoTimestamp": output = toJson(TimestampSchema, await client.echoTimestamp(fromJson(TimestampSchema, input))); break;
      case "EchoDuration": output = toJson(DurationSchema, await client.echoDuration(fromJson(DurationSchema, input))); break;
      case "EchoMappings": output = toJson(RuntimeMappingsSchema, await client.echoMappings(fromJson(RuntimeMappingsSchema, input))); break;
    }
    if (JSON.stringify(canonical(output)) !== JSON.stringify(canonical(vector.canonical as JsonValue))) {
      throw Error(`Profile reply changed ${vector.id}`);
    }
    return jsonResponse({id:vector.id, method:vector.method, verified:true});
  };
}
