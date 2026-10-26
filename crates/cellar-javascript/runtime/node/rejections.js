// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// The Workers profile reports floating promise failures as web events. Ordinary
// Node applications keep Node's fatal default. Late handling must not borrow the
// event which happened to attach a catch to an older promise.
(current, run, report, global = globalThis) => {
  const EventClass = Event;
  const TargetClass = EventTarget;
  const define = Object.defineProperties;
  const target = new TargetClass();
  const reasons = new WeakMap();
  const dispatch = target.dispatchEvent.bind(target);

  class PromiseRejectionEvent extends EventClass {
    constructor(type, init) {
      super(type, init);
      define(this, {
        promise: { value: init.promise, enumerable: true },
        reason: { value: init.reason, enumerable: true },
      });
    }
  }
  define(global, {
    PromiseRejectionEvent: {
      value: PromiseRejectionEvent,
      configurable: true,
      writable: true,
    },
    addEventListener: {
      value: target.addEventListener.bind(target),
      configurable: true,
      writable: true,
    },
    removeEventListener: {
      value: target.removeEventListener.bind(target),
      configurable: true,
      writable: true,
    },
    dispatchEvent: { value: dispatch, configurable: true, writable: true },
  });
  return {
    unhandled(reason, promise) {
      const owner = current();
      // Bootstrap/transport failures are outside application event semantics.
      // Throwing here retains Node's fatal behavior for those failures.
      if (owner?.invocation === undefined) throw reason;
      reasons.set(promise, { owner, reason });
      run(owner, () => {
        const event = new PromiseRejectionEvent("unhandledrejection", {
          promise,
          reason,
          cancelable: true,
        });
        if (dispatch(event)) report("Uncaught (in promise)", reason);
      });
    },
    handled(promise) {
      const record = reasons.get(promise);
      if (!record) return;
      reasons.delete(promise);
      run(record.owner, () =>
        dispatch(
          new PromiseRejectionEvent("rejectionhandled", {
            promise,
            reason: record.reason,
          }),
        ),
      );
    },
  };
}
