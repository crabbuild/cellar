import { Counter, handler } from "./app.js";
import { CounterClient, CounterRegistration } from "./fixtures/generated/typescript/counter_cellar.js";
import { AbortController } from "abort-controller";

export const cells = {V8Counter:Counter};
export const rpc = {V8Counter:CounterRegistration};
export const fetch = handler(CounterClient, () => new AbortController());
