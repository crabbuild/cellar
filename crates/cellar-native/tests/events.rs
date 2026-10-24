// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar_native::{
    clone::{Object, StructuredValue, Value},
    web::{
        AbortController, AbortReason, AbortSignal, CustomEvent, Event, EventDispatchError,
        EventInit, EventListener, EventTarget, Listener, ListenerOptions,
    },
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Wake, Waker},
};

fn value(value: Value) -> StructuredValue {
    StructuredValue {
        root: value,
        objects: Vec::new(),
    }
}

fn log(log: &Arc<Mutex<Vec<String>>>, label: &str) -> Listener {
    let log = log.clone();
    let label = label.to_string();
    Listener::new(move |_, _| {
        log.lock().unwrap().push(label.clone());
        Ok(())
    })
}

#[test]
fn event_dispatch_keeps_target_state_snapshots_and_cancellation() {
    let target = EventTarget::new();
    let other = EventTarget::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let last = log(&calls, "last");
    let added = log(&calls, "added");
    let removed = log(&calls, "removed");
    let l = calls.clone();
    let r = removed.clone();
    let a = added.clone();
    let first = Listener::new(move |t, e| {
        assert!(e.target().unwrap().ptr_eq(t));
        assert!(e.current_target().unwrap().ptr_eq(t));
        assert!(e.composed_path()[0].ptr_eq(t));
        assert_eq!(e.event_phase(), Event::AT_TARGET);
        l.lock().unwrap().push("first".into());
        t.remove_event_listener("tick", &r, false)?;
        t.add_event_listener("tick", Some(&a), ListenerOptions::default())?;
        e.stop_propagation();
        e.set_cancel_bubble(false);
        assert!(e.cancel_bubble());
        e.prevent_default();
        Ok(())
    });
    for listener in [&first, &removed, &last, &first] {
        target
            .add_event_listener("tick", Some(listener), ListenerOptions::default())
            .unwrap();
    }
    let event = Event::new(
        "tick",
        EventInit {
            bubbles: true,
            cancelable: true,
            composed: true,
        },
    );
    assert!(event.target().is_none());
    assert!(!event.is_trusted());
    assert_eq!(event.time_stamp(), 0.0);
    assert!(event.composed_path().is_empty());
    assert!(!target.dispatch_event(&event).unwrap());
    assert_eq!(*calls.lock().unwrap(), ["first", "removed", "last"]);
    assert_eq!(event.event_phase(), 0);
    assert!(!event.is_dispatching());
    assert!(event.current_target().unwrap().ptr_eq(&target));
    assert!(!event.return_value());
    calls.lock().unwrap().clear();
    target.dispatch_event(&event).unwrap();
    assert_eq!(*calls.lock().unwrap(), ["first", "last", "added"]);
    other.dispatch_event(&event).unwrap();
    assert!(event.target().unwrap().ptr_eq(&other));
    assert!(event.default_prevented());
    assert!(!event.cancel_bubble());
    let e = Event::new_utf16(&[0xd800, 0, 0x96ea], EventInit::default());
    e.prevent_default();
    assert!(!e.default_prevented());
    assert_eq!(e.event_type_utf16(), [0xd800, 0, 0x96ea]);
    e.trust();
    assert!(e.is_trusted());
}

#[test]
fn once_nested_events_handler_replacement_and_callback_throws_keep_original_rules() {
    let target = EventTarget::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let c = calls.clone();
    let once = Listener::new(move |t, _| {
        c.lock().unwrap().push("once".into());
        t.dispatch_event(&Event::new("tick", EventInit::default()))?;
        Ok(())
    });
    target
        .add_event_listener(
            "tick",
            Some(&once),
            ListenerOptions {
                once: true,
                ..Default::default()
            },
        )
        .unwrap();
    target
        .add_event_listener(
            "tick",
            Some(&log(&calls, "regular")),
            ListenerOptions::default(),
        )
        .unwrap();
    target.set_fallback_handler("tick", Some(log(&calls, "fallback")));
    target
        .dispatch_event(&Event::new("tick", EventInit::default()))
        .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        ["once", "regular", "fallback", "regular", "fallback"]
    );
    let target = EventTarget::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let new = log(&calls, "new");
    let changed = new.clone();
    let first = Listener::new(move |t, _| {
        t.set_event_handler("x", Some(changed.clone()));
        Ok(())
    });
    target
        .add_event_listener("x", Some(&first), ListenerOptions::default())
        .unwrap();
    target.set_event_handler("x", Some(log(&calls, "old")));
    target
        .add_event_listener("x", Some(&log(&calls, "after")), ListenerOptions::default())
        .unwrap();
    target
        .dispatch_event(&Event::new("x", EventInit::default()))
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), ["new", "after"]);
    assert!(target.event_handler("x").unwrap().ptr_eq(&new));
    let target = EventTarget::new();
    let nested = Listener::new(move |t, e| {
        let EventDispatchError::Thrown(reason) = t.dispatch_event(e).unwrap_err() else {
            panic!("expected DOM exception")
        };
        assert_eq!(reason.dom_exception().unwrap().code(), 11);
        Ok(())
    });
    target
        .add_event_listener("x", Some(&nested), ListenerOptions::default())
        .unwrap();
    target
        .dispatch_event(&Event::new("x", EventInit::default()))
        .unwrap();
    let reason = AbortReason::from_value(value(Value::Null));
    let r = reason.clone();
    let throwing = Listener::new(move |_, _| Err(EventDispatchError::Thrown(r.clone())));
    let target = EventTarget::new();
    target
        .add_event_listener(
            "x",
            Some(&throwing),
            ListenerOptions {
                once: true,
                ..Default::default()
            },
        )
        .unwrap();
    let event = Event::new("x", EventInit::default());
    let EventDispatchError::Thrown(thrown) = target.dispatch_event(&event).unwrap_err() else {
        panic!("expected thrown value")
    };
    assert!(thrown.ptr_eq(&reason));
    assert!(event.is_dispatching());
    assert_eq!(event.event_phase(), 2);
    assert!(target.dispatch_event(&event).is_err());
    assert!(target
        .dispatch_event(&Event::new("x", EventInit::default()))
        .unwrap());
}

#[test]
fn listener_options_abort_removal_and_shared_custom_detail_are_observable() {
    let target = EventTarget::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let listener = log(&calls, "called");
    let controller = AbortController::new();
    target
        .add_event_listener(
            "x",
            Some(&listener),
            ListenerOptions {
                signal: Some(controller.signal()),
                ..Default::default()
            },
        )
        .unwrap();
    target.remove_event_listener("x", &listener, false).unwrap();
    target
        .add_event_listener("x", Some(&listener), ListenerOptions::default())
        .unwrap();
    controller.abort(None).unwrap();
    target
        .dispatch_event(&Event::new("x", EventInit::default()))
        .unwrap();
    assert!(calls.lock().unwrap().is_empty());
    assert!(target
        .add_event_listener(
            "x",
            None,
            ListenerOptions {
                capture: true,
                ..Default::default()
            }
        )
        .is_ok());
    assert_eq!(
        target
            .add_event_listener(
                "x",
                Some(&listener),
                ListenerOptions {
                    passive: true,
                    ..Default::default()
                }
            )
            .unwrap_err()
            .to_string(),
        "Cells does not support the 'capture' or 'passive' options on addEventListener()."
    );
    assert!(target
        .remove_event_listener("absent", &listener, true)
        .is_err());
    let original = Arc::new(Mutex::new(StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::Object(vec![("self".into(), Value::Reference(0))])],
    }));
    let custom = CustomEvent::from_shared_detail("detail", EventInit::default(), original.clone());
    let old = custom.detail();
    assert!(Arc::ptr_eq(&old, &original));
    let callback = Listener::new(|_, e| {
        let c = e.as_custom_event().unwrap();
        c.detail()
            .lock()
            .unwrap()
            .objects
            .push(Object::Object(Vec::new()));
        c.set_detail(value(Value::String(vec![0xd800])));
        Ok(())
    });
    target
        .add_event_listener("detail", Some(&callback), ListenerOptions::default())
        .unwrap();
    target.dispatch_event(&custom.event).unwrap();
    assert_eq!(original.lock().unwrap().objects.len(), 2);
    assert_eq!(
        custom.detail().lock().unwrap().root,
        Value::String(vec![0xd800])
    );
    assert!(!Arc::ptr_eq(&custom.detail(), &old));
    assert!(matches!(
        CustomEvent::new("x", EventInit::default(), Some(value(Value::Undefined)))
            .detail()
            .lock()
            .unwrap()
            .root,
        Value::Null
    ));
}

#[test]
fn abort_latches_shared_reason_any_order_and_handler_position() {
    let first = AbortController::new();
    let second = AbortController::new();
    let combined = AbortSignal::any([first.signal(), second.signal()]);
    let calls = Arc::new(Mutex::new(Vec::new()));
    let s = first.signal();
    s.add_event_listener(
        "abort",
        Some(&log(&calls, "before")),
        ListenerOptions::default(),
    )
    .unwrap();
    s.set_on_abort(Some(log(&calls, "old")));
    s.add_event_listener(
        "abort",
        Some(&log(&calls, "after")),
        ListenerOptions::default(),
    )
    .unwrap();
    s.set_on_abort(Some(log(&calls, "replacement")));
    let shared = Arc::new(Mutex::new(value(Value::String(vec![0xd800]))));
    let reason = AbortReason::from_shared_value(shared.clone());
    first.abort(Some(reason.clone())).unwrap();
    assert_eq!(*calls.lock().unwrap(), ["before", "replacement", "after"]);
    assert!(s.reason().unwrap().ptr_eq(&reason));
    assert!(combined.throw_if_aborted().unwrap_err().ptr_eq(&reason));
    *shared.lock().unwrap() = value(Value::Boolean(false));
    assert_eq!(
        combined
            .reason()
            .unwrap()
            .value()
            .unwrap()
            .lock()
            .unwrap()
            .root,
        Value::Boolean(false)
    );
    first.abort(Some(AbortReason::default_abort())).unwrap();
    second.abort(None).unwrap();
    assert!(combined.reason().unwrap().ptr_eq(&reason));
    let preferred = AbortSignal::abort(Some(AbortReason::from_value(value(Value::Number(42.0)))));
    let picked = AbortSignal::any([preferred.clone(), first.signal()]);
    assert!(picked
        .reason()
        .unwrap()
        .ptr_eq(&preferred.reason().unwrap()));
    let default = AbortSignal::abort(Some(AbortReason::from_value(value(Value::Undefined))))
        .reason()
        .unwrap()
        .dom_exception()
        .unwrap();
    assert_eq!(default.name(), "AbortError");
    assert_eq!(default.message(), "This operation was aborted");
    assert_eq!(default.code(), 20);
    default.set_name_utf16(&"TimeoutError".encode_utf16().collect::<Vec<_>>());
    assert_eq!(default.code(), 23);
    default.set_message_utf16(&[0xd800]);
    assert_eq!(default.message_utf16(), [0xd800]);
    assert!(!AbortSignal::any([]).aborted());
}

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn abort_waiters_release_wakers_and_wake_once_even_when_a_listener_throws() {
    let controller = AbortController::new();
    let signal = controller.signal();
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let weak = Arc::downgrade(&wake);
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut waiting = signal.cancelled();
    for _ in 0..100 {
        assert!(Pin::new(&mut waiting).poll(&mut context).is_pending());
    }
    assert_eq!(Arc::strong_count(&wake), 3);
    drop(waiting);
    assert_eq!(Arc::strong_count(&wake), 2);
    drop(waker);
    drop(wake);
    assert!(weak.upgrade().is_none());
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    let mut a = signal.cancelled();
    let mut b = signal.cancelled();
    assert!(Pin::new(&mut a).poll(&mut context).is_pending());
    assert!(Pin::new(&mut b).poll(&mut context).is_pending());
    let throwing = Listener::new(|_, _| {
        Err(EventDispatchError::Thrown(AbortReason::error(
            "listener throw",
        )))
    });
    signal
        .add_event_listener("abort", Some(&throwing), ListenerOptions::default())
        .unwrap();
    let reason = AbortReason::from_value(value(Value::Null));
    assert!(controller.abort(Some(reason.clone())).is_err());
    assert_eq!(wake.0.load(Ordering::SeqCst), 2);
    for waiting in [&mut a, &mut b] {
        let Poll::Ready(result) = Pin::new(waiting).poll(&mut context) else {
            panic!("abort did not wake waiter")
        };
        assert!(result.ptr_eq(&reason));
    }
    controller.abort(None).unwrap();
    assert_eq!(wake.0.load(Ordering::SeqCst), 2);
}

#[test]
fn timeout_uses_host_scheduling_and_retains_signed_32_bit_delay_conversion() {
    for (ms, expected) in [
        (f64::NAN, 0),
        (-1.0, 0),
        (1.9, 1),
        (2147483648.0, 0),
        (4294967303.0, 7),
        (f64::INFINITY, 0),
    ] {
        let scheduled = Arc::new(Mutex::new(None));
        let captured = scheduled.clone();
        let signal = AbortSignal::timeout(ms, move |delay, controller| {
            assert_eq!(delay.as_millis(), expected);
            *captured.lock().unwrap() = Some(controller);
            Ok::<_, ()>(())
        })
        .unwrap();
        assert!(!signal.aborted());
        scheduled
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .abort(Some(AbortReason::timeout()))
            .unwrap();
        assert_eq!(signal.reason().unwrap().dom_exception().unwrap().code(), 23);
    }

    struct ObjectListener(AtomicUsize);

    impl EventListener for ObjectListener {
        fn handle_event(&self, _: &Event) -> Result<(), EventDispatchError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let target = EventTarget::new();
    let object = Arc::new(ObjectListener(AtomicUsize::new(0)));
    let a = Listener::Object(object.clone());
    let b = Listener::Object(object.clone());
    target
        .add_event_listener("object", Some(&a), ListenerOptions::default())
        .unwrap();
    target
        .add_event_listener("object", Some(&b), ListenerOptions::default())
        .unwrap();
    target
        .dispatch_event(&Event::new("object", EventInit::default()))
        .unwrap();
    assert_eq!(object.0.load(Ordering::SeqCst), 1);
}
