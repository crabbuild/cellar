# Shared guest operation boundary

This crate owns the native operation protocol used by the Python and JavaScript
adapters: input validation, buffered HTTP conversion, checked storage batches,
infrastructure bindings and typed failure envelopes. It contains no engine.

HTTP conversion shares limits across JSON/base64 transports and Python's raw-byte
transport. `request_parts`, `output_body` and `response_parts` retain the latter
without adding an encoding pass. `operations::fetch_bytes` applies the same
request validation and invokes the original native SDK before collecting the
reply. `failure::result_parts` charges the complete base64-equivalent result
envelope and drops an oversized body while preserving an unknown mutation
outcome. Application overloads preserve their classification across nested
calls while retaining an unknown mutation outcome.

Call `operations::execute` only from the original `sdk::Context` invocation.
Never move its SDK futures to an interpreter thread. This is adapter machinery;
application code should use the language's SDK facade. Moving the Python
implementation here keeps both adapters on the same native transaction and
ambiguity rules. Tests for value limits and error provenance live here;
interpreter lifecycle tests stay in their owning adapters.
