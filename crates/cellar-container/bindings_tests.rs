// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::{Native, Operation, ProviderSession};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Backend {
    binding: String,
    opened: Arc<AtomicUsize>,
}

impl Provider for Backend {
    fn name(&self) -> &str {
        "fixture"
    }

    fn binding_name(&self) -> Option<&str> {
        Some(&self.binding)
    }

    fn operations(&self) -> &[Operation] {
        &[Operation::Running]
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        self.opened.fetch_add(1, Ordering::SeqCst);
        Native.open()
    }
}

fn provider(binding: &str, opened: &Arc<AtomicUsize>) -> Arc<dyn Provider> {
    Arc::new(Backend {
        binding: binding.into(),
        opened: opened.clone(),
    })
}

#[test]
fn configuration_refuses_aliases_and_colliding_default_before_dispatch() {
    let mut bindings = Bindings::default();
    let opened = Arc::new(AtomicUsize::new(0));
    for name in ["", "container", "../escape", "é", &"x".repeat(129)] {
        assert!(bindings.insert(name, provider(name, &opened)).is_err());
    }
    assert!(bindings.insert("build", Arc::new(Native)).is_err());
    assert!(bindings
        .insert("build", provider("other", &opened))
        .is_err());
    bindings
        .insert("build", provider("build", &opened))
        .unwrap();
    assert!(bindings
        .insert("build", provider("build", &opened))
        .is_err());
    assert!(bindings.validate(&Native).is_ok());
    assert!(bindings
        .validate(provider("build", &opened).as_ref())
        .is_err());
    let descriptors = bindings.capabilities(&Limits {
        max_body_bytes: 1024,
    });
    assert_eq!(descriptors.len(), 1);
    assert_eq!(descriptors[0].name, "build");
    assert_eq!(descriptors[0].capabilities.provider, "fixture");
    assert_eq!(opened.load(Ordering::SeqCst), 0);
}

#[test]
fn registry_bounds_sessions_and_never_falls_back_on_unknown_names() {
    let mut bindings = Bindings::default();
    let opened = Arc::new(AtomicUsize::new(0));
    for i in 0..63 {
        let name = format!("slot-{i}");
        bindings.insert(&name, provider(&name, &opened)).unwrap();
    }
    assert!(bindings
        .insert("overflow", provider("overflow", &opened))
        .is_err());
    assert_eq!(opened.load(Ordering::SeqCst), 0);
    let sessions = bindings.sessions();
    assert_eq!(opened.load(Ordering::SeqCst), 63);
    assert_eq!(sessions.names().count(), 63);
    assert!(std::ptr::eq(
        sessions.get("slot-1").unwrap(),
        sessions.get("slot-1").unwrap()
    ));
    for name in ["missing", "container", "../escape"] {
        assert!(sessions.get(name).err().unwrap().is::<Validation>());
    }
}
