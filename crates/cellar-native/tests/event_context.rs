use cellar_native::event_context::EventFrames;

#[test]
fn nested_frames_keep_background_and_gates_with_their_invocation() {
    let mut first = EventFrames::<&str, u64>::default();
    let mut second = EventFrames::<&str, u64>::default();
    first.begin();
    first.register_background("outer").unwrap();
    first.register_gate(1).unwrap();
    first.begin();
    first.register_background("inner").unwrap();
    first.register_gate(2).unwrap();
    second.begin();
    second.register_background("other event").unwrap();
    second.register_gate(3).unwrap();
    assert_eq!(first.depth(), 2);
    assert_eq!(first.end().unwrap().background, ["inner"]);
    assert_eq!(first.depth(), 1);
    assert_eq!(first.end().unwrap().background, ["outer"]);
    assert_eq!(first.take_gates(), [1, 2]);
    assert_eq!(second.end().unwrap().background, ["other event"]);
    assert_eq!(second.take_gates(), [3]);
    assert!(first.end().is_none());
}

#[test]
fn each_late_background_batch_extends_only_its_own_registration_window() {
    let mut frames = EventFrames::<u64, ()>::default();
    assert_eq!(frames.register_background(0), Err(0));
    frames.begin();
    frames.register_background(1).unwrap();
    assert_eq!(frames.end().unwrap().background, [1]);
    for id in 2..5 {
        assert!(frames.accepts_background());
        frames.register_background(id).unwrap();
        assert_eq!(frames.take_late_background(), [id]);
    }
    assert!(frames.take_late_background().is_empty());
    assert!(!frames.accepts_background());
    assert_eq!(frames.register_background(5), Err(5));

    frames.begin();
    assert!(frames.end().unwrap().background.is_empty());
    assert!(!frames.accepts_background());
    assert_eq!(frames.register_background(6), Err(6));
}

#[test]
fn explicit_background_seal_returns_abandoned_work() {
    let mut frames = EventFrames::<String, ()>::default();
    frames.begin();
    frames.register_background("handler".into()).unwrap();
    frames.end();
    frames.register_background("late".into()).unwrap();
    assert_eq!(frames.seal_background(), ["late"]);
    assert!(!frames.accepts_background());
    assert_eq!(
        frames.register_background("stale".into()),
        Err("stale".into())
    );
    assert!(frames.seal_background().is_empty());
}

#[test]
fn response_boundary_takes_ended_and_active_gates_and_refuses_late_gates() {
    let mut frames = EventFrames::<(), u64>::default();
    frames.begin();
    frames.register_gate(1).unwrap();
    frames.end();
    // A gate started while building the returned aggregate still belongs to
    // the response even though its top-level frame has already ended.
    frames.register_gate(2).unwrap();
    frames.begin();
    frames.register_gate(3).unwrap();
    frames.begin();
    frames.register_gate(4).unwrap();
    assert_eq!(frames.take_gates(), [1, 2, 3, 4]);
    assert_eq!(frames.register_gate(5), Err(5));
    assert!(frames.take_gates().is_empty());
    assert!(frames.end().unwrap().discarded_gates.is_empty());
    assert!(frames.end().unwrap().discarded_gates.is_empty());
    assert_eq!(frames.register_gate(6), Err(6));
}

#[test]
fn refused_work_and_gates_return_ownership_without_implicit_cleanup() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Owned(Arc<AtomicUsize>);

    impl Drop for Owned {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut frames = EventFrames::<Owned, Owned>::default();
    let work = frames
        .register_background(Owned(dropped.clone()))
        .err()
        .unwrap();
    frames.take_gates();
    let gate = frames.register_gate(Owned(dropped.clone())).err().unwrap();
    assert_eq!(dropped.load(Ordering::Relaxed), 0);
    drop((work, gate));
    assert_eq!(dropped.load(Ordering::Relaxed), 2);
}
