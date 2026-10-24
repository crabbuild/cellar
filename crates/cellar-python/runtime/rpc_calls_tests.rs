use super::*;

#[test]
fn cancellation_is_idempotent_and_isolated_to_the_original_operation() {
    let calls = Arc::new(RpcCalls::default());
    let first = calls.register(1).unwrap();
    let second = calls.register(2).unwrap();
    assert!(calls.cancel(1).unwrap());
    assert!(calls.cancel(1).unwrap());
    assert!(first.signal().aborted());
    assert!(!second.signal().aborted());
    assert!(!Arc::new(RpcCalls::default()).cancel(1).unwrap());
    assert!(!calls.cancel(99).unwrap());
}

#[test]
fn duplicate_admission_never_replaces_an_outstanding_controller() {
    let calls = Arc::new(RpcCalls::default());
    let first = calls.register(1).unwrap();
    assert!(calls.register(1).is_err());
    assert!(calls.cancel(1).unwrap());
    assert!(first.signal().aborted());
}

#[test]
fn native_future_drop_removes_controls_without_a_python_callback() {
    let calls = Arc::new(RpcCalls::default());
    let call = calls.register(1).unwrap();
    let signal = call.signal();
    drop(call);
    assert!(!calls.cancel(1).unwrap());
    assert!(!signal.aborted());
    assert!(calls.0.lock().unwrap().is_empty());
}
