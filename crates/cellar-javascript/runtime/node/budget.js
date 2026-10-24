// Evaluated before application imports. Only durations cross IPC; each native
// handle retains its own clock/parent cap. Node charges the control round trip
// conservatively so neither native submission nor decoding buys a new window.
(monotonic, current, active, maximum, send, next) => {
  const pending = new Map();
  const min = Math.min, max = Math.max, number = Number, integer = Number.isInteger;
  const bigint = BigInt, ErrorClass = Error, assign = Object.assign, PromiseClass = Promise;
  const nanos = 1000000n;
  const error = code => assign(new ErrorClass(code), {code, outcome:"not_committed"});
  const elapsed = record => {
    const value = monotonic() - record.started;
    if (value < 0n) throw error("Unavailable");
    return value;
  };
  const owner = (invocation, id) => {
    if (current() !== invocation) throw error("ContextMismatch");
    const state = active.get(invocation);
    if (!state) throw error("ContextExpired");
    const record = state.budgets.get(id);
    if (!record || record.released) throw error("ContextMismatch");
    return record;
  };
  const remaining = record => {
    // Native deadlines use whole monotonic milliseconds. Reserve one tick so
    // a differently phased Node clock never outlives that native ceiling.
    const value = bigint(record.ceiling) * nanos - elapsed(record) - nanos;
    if (value <= 0n) throw error("DeadlineExceeded");
    return number((value + nanos - 1n) / nanos);
  };

  function start(invocation, state) {
    if (state.budgets.size >= maximum) throw error("ResourceExhausted");
    const id = next();
    const record = {started:monotonic(), state, invocation, id, released:false};
    state.budgets.set(id, record);
    return new PromiseClass((resolve, reject) => {
      pending.set(id, {record, resolve, reject});
      send({type:"budget_start", invocation, id});
    });
  }

  function receive(message) {
    const entry = pending.get(message.id);
    if (!entry) return; // Revocation already joined this control locally.
    const {record, resolve, reject} = entry;
    if (message.invocation !== record.invocation) throw error("ContextMismatch");
    pending.delete(record.id);
    if (!message.ok) {
      record.state.budgets.delete(record.id);
      reject(error(message.code));
      return;
    }
    if (!integer(message.remaining_ms) || message.remaining_ms <= 0 || message.remaining_ms > 0xffffffff) throw error("Unavailable");
    record.handshake = number((elapsed(record) + nanos - 1n) / nanos);
    record.ceiling = max(0, message.remaining_ms - record.handshake);
    const read = () => remaining(owner(record.invocation, record.id));
    const limit = timeout => {
      owner(record.invocation, record.id);
      if (!integer(timeout) || timeout < 0 || timeout > 0xffffffff) throw error("InvalidArgument");
      record.ceiling = min(record.ceiling, max(0, timeout - record.handshake));
    };
    const release = () => {
      if (record.released) return;
      record.released = true;
      record.state.budgets.delete(record.id);
      if (active.get(record.invocation) === record.state) send({type:"budget_end", invocation:record.invocation, id:record.id});
    };
    resolve([record.id, read, limit, release]);
  }

  function parameters(invocation, id) {
    if (id === undefined) return {};
    const record = owner(invocation, id);
    remaining(record);
    return {budget_id:id, budget_ms:record.ceiling};
  }

  function revoke(invocation, state) {
    for (const [id, {record, reject}] of pending) {
      if (record.invocation === invocation) {
        pending.delete(id);
        record.released = true;
        record.state.budgets.delete(id);
        reject(error("ContextExpired"));
      }
    }
    if (state) {
      for (const record of state.budgets.values()) record.released = true;
      state.budgets.clear();
    }
  }
  return {start, receive, parameters, revoke};
}
