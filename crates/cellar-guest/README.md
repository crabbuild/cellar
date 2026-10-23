# Shared guest operation boundary

This crate owns the native operation protocol used by the Python and JavaScript
adapters: input validation, buffered HTTP conversion, checked storage batches,
infrastructure bindings and typed failure envelopes. It contains no engine.

Call `operations::execute` only from the original `sdk::Context` invocation.
Never move its SDK futures to an interpreter thread. This is adapter machinery;
application code should use the language's SDK facade. Moving the Python
implementation here keeps both adapters on the same native transaction and
ambiguity rules. Tests for value limits and error provenance live here;
interpreter lifecycle tests stay in their owning adapters.
