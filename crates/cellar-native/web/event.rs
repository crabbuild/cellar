// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! At-target events and listener identity, retaining cellar's dispatch rules.
use super::{AbortReason, AbortSignal, DomException, Error};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

#[derive(Clone, Copy, Debug, Default)]
pub struct EventInit {
    pub bubbles: bool,
    pub cancelable: bool,
    pub composed: bool,
}
struct EventData {
    kind: Vec<u16>,
    init: EventInit,
    prevented: bool,
    phase: u16,
    target: Option<EventTarget>,
    stop: bool,
    immediate: bool,
    dispatching: bool,
    trusted: bool,
    custom_detail: Option<Arc<Mutex<SharedEventValue>>>,
}
#[derive(Clone)]
pub struct Event(Arc<Mutex<EventData>>);
impl Event {
    pub const NONE: u16 = 0;
    pub const CAPTURING_PHASE: u16 = 1;
    pub const AT_TARGET: u16 = 2;
    pub const BUBBLING_PHASE: u16 = 3;
    pub fn new(kind: &str, init: EventInit) -> Self {
        Self::new_utf16(&kind.encode_utf16().collect::<Vec<_>>(), init)
    }
    pub fn new_utf16(kind: &[u16], init: EventInit) -> Self {
        Self(Arc::new(Mutex::new(EventData {
            kind: kind.to_vec(),
            init,
            prevented: false,
            phase: 0,
            target: None,
            stop: false,
            immediate: false,
            dispatching: false,
            trusted: false,
            custom_detail: None,
        })))
    }
    pub fn event_type_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().kind.clone()
    }
    pub fn event_type(&self) -> String {
        String::from_utf16_lossy(&self.event_type_utf16())
    }
    pub fn bubbles(&self) -> bool {
        self.0.lock().unwrap().init.bubbles
    }
    pub fn cancelable(&self) -> bool {
        self.0.lock().unwrap().init.cancelable
    }
    pub fn composed(&self) -> bool {
        self.0.lock().unwrap().init.composed
    }
    pub fn default_prevented(&self) -> bool {
        self.0.lock().unwrap().prevented
    }
    pub fn event_phase(&self) -> u16 {
        self.0.lock().unwrap().phase
    }
    pub fn target(&self) -> Option<EventTarget> {
        self.0.lock().unwrap().target.clone()
    }
    pub fn current_target(&self) -> Option<EventTarget> {
        self.target()
    }
    pub fn composed_path(&self) -> Vec<EventTarget> {
        self.target().into_iter().collect()
    }
    pub fn is_trusted(&self) -> bool {
        self.0.lock().unwrap().trusted
    }
    pub fn time_stamp(&self) -> f64 {
        0.0
    }
    pub fn return_value(&self) -> bool {
        !self.default_prevented()
    }
    pub fn cancel_bubble(&self) -> bool {
        self.0.lock().unwrap().stop
    }
    pub fn set_cancel_bubble(&self, value: bool) {
        if value {
            self.stop_propagation();
        }
    }
    pub fn prevent_default(&self) {
        let mut s = self.0.lock().unwrap();
        if s.init.cancelable {
            s.prevented = true;
        }
    }
    pub fn stop_propagation(&self) {
        self.0.lock().unwrap().stop = true;
    }
    pub fn stop_immediate_propagation(&self) {
        let mut s = self.0.lock().unwrap();
        s.stop = true;
        s.immediate = true;
    }
    pub fn is_dispatching(&self) -> bool {
        self.0.lock().unwrap().dispatching
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn as_custom_event(&self) -> Option<CustomEvent> {
        Some(CustomEvent {
            event: self.clone(),
            detail: self.0.lock().unwrap().custom_detail.clone()?,
        })
    }
    /// For events delivered by a host, rather than constructed by a handler.
    pub fn trust(&self) {
        self.0.lock().unwrap().trusted = true;
    }
    fn begin(&self, target: EventTarget) -> Result<(), EventDispatchError> {
        let mut state = self.0.lock().unwrap();
        if state.dispatching {
            return Err(EventDispatchError::Thrown(AbortReason::from(
                DomException::new(
                    "The event is already being dispatched.",
                    "InvalidStateError",
                ),
            )));
        }
        state.dispatching = true;
        state.target = Some(target);
        state.phase = Self::AT_TARGET;
        state.stop = false;
        state.immediate = false;
        Ok(())
    }
    fn end(&self) {
        let mut s = self.0.lock().unwrap();
        s.dispatching = false;
        s.phase = Self::NONE;
    }
    fn stopped_immediately(&self) -> bool {
        self.0.lock().unwrap().immediate
    }
}
impl std::fmt::Debug for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Event")
            .field("type", &self.event_type())
            .field("phase", &self.event_phase())
            .finish()
    }
}

/// Function callbacks get the target as their receiver. Object listeners get
/// their own receiver through this trait's `self`.
pub trait EventListener: Send + Sync + 'static {
    fn handle_event(&self, event: &Event) -> Result<(), EventDispatchError>;
}
type Callback = dyn Fn(&EventTarget, &Event) -> Result<(), EventDispatchError> + Send + Sync;
#[derive(Clone)]
pub enum Listener {
    Function(Arc<Callback>),
    Object(Arc<dyn EventListener>),
}
impl Listener {
    pub fn new(
        callback: impl Fn(&EventTarget, &Event) -> Result<(), EventDispatchError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self::Function(Arc::new(callback))
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Function(a), Self::Function(b)) => Arc::ptr_eq(a, b),
            (Self::Object(a), Self::Object(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
    fn invoke(&self, target: &EventTarget, event: &Event) -> Result<(), EventDispatchError> {
        match self {
            Self::Function(f) => f(target, event),
            Self::Object(o) => o.handle_event(event),
        }
    }
    fn downgrade(&self) -> WeakListener {
        match self {
            Self::Function(f) => WeakListener::Function(Arc::downgrade(f)),
            Self::Object(o) => WeakListener::Object(Arc::downgrade(o)),
        }
    }
}
enum WeakListener {
    Function(Weak<Callback>),
    Object(Weak<dyn EventListener>),
}
impl WeakListener {
    fn upgrade(&self) -> Option<Listener> {
        match self {
            Self::Function(f) => f.upgrade().map(Listener::Function),
            Self::Object(o) => o.upgrade().map(Listener::Object),
        }
    }
}
#[derive(Clone, Default)]
pub struct ListenerOptions {
    pub once: bool,
    pub capture: bool,
    pub passive: bool,
    pub signal: Option<AbortSignal>,
}
#[derive(Clone, Debug)]
pub enum EventDispatchError {
    Type(Error),
    Thrown(AbortReason),
}
impl std::fmt::Display for EventDispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Type(e) => e.fmt(f),
            Self::Thrown(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for EventDispatchError {}
impl From<Error> for EventDispatchError {
    fn from(value: Error) -> Self {
        Self::Type(value)
    }
}

struct Entry {
    callback: Mutex<Listener>,
    once: bool,
    handler: bool,
}
#[derive(Default)]
struct TargetState {
    listeners: HashMap<Vec<u16>, Vec<Arc<Entry>>>,
    fallback: HashMap<Vec<u16>, Listener>,
}
#[derive(Clone, Default)]
pub struct EventTarget(Arc<Mutex<TargetState>>);
impl std::fmt::Debug for EventTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventTarget").finish_non_exhaustive()
    }
}
impl EventTarget {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn add_event_listener(
        &self,
        kind: &str,
        listener: Option<&Listener>,
        options: ListenerOptions,
    ) -> Result<(), Error> {
        self.add_event_listener_utf16(&kind.encode_utf16().collect::<Vec<_>>(), listener, options)
    }
    pub fn add_event_listener_utf16(
        &self,
        kind: &[u16],
        listener: Option<&Listener>,
        options: ListenerOptions,
    ) -> Result<(), Error> {
        let Some(listener) = listener else {
            return Ok(());
        };
        if options.capture || options.passive {
            return Err(Error::type_error(
                "Cells does not support the 'capture' or 'passive' options on addEventListener().",
            ));
        }
        if options.signal.as_ref().is_some_and(AbortSignal::aborted) {
            return Ok(());
        }
        {
            let mut state = self.0.lock().unwrap();
            let list = state.listeners.entry(kind.to_vec()).or_default();
            if list
                .iter()
                .any(|entry| entry.callback.lock().unwrap().ptr_eq(listener))
            {
                return Ok(());
            }
            list.push(Arc::new(Entry {
                callback: Mutex::new(listener.clone()),
                once: options.once,
                handler: false,
            }));
        }
        if let Some(signal) = options.signal {
            let target = Arc::downgrade(&self.0);
            let listener = listener.downgrade();
            let kind = kind.to_vec();
            let cleanup = Listener::new(move |_, _| {
                if let (Some(target), Some(listener)) = (target.upgrade(), listener.upgrade()) {
                    EventTarget(target).remove_event_listener_utf16(&kind, &listener, false)?;
                }
                Ok(())
            });
            signal.add_event_listener(
                "abort",
                Some(&cleanup),
                ListenerOptions {
                    once: true,
                    ..Default::default()
                },
            )?;
            // Native signals may race on another thread. A latched abort that
            // occurred before subscription must still remove this registration.
            if signal.aborted() {
                cleanup
                    .invoke(
                        &signal.event_target(),
                        &Event::new("abort", EventInit::default()),
                    )
                    .map_err(|e| Error::generic_error(e.to_string()))?;
            }
        }
        Ok(())
    }
    pub fn remove_event_listener(
        &self,
        kind: &str,
        listener: &Listener,
        capture: bool,
    ) -> Result<(), Error> {
        self.remove_event_listener_utf16(
            &kind.encode_utf16().collect::<Vec<_>>(),
            listener,
            capture,
        )
    }
    pub fn remove_event_listener_utf16(
        &self,
        kind: &[u16],
        listener: &Listener,
        capture: bool,
    ) -> Result<(), Error> {
        if capture {
            return Err(Error::type_error(
                "Cells does not support the 'capture' option on removeEventListener().",
            ));
        }
        let mut removed = None;
        let mut state = self.0.lock().unwrap();
        if let Some(list) = state.listeners.get_mut(kind) {
            if let Some(index) = list
                .iter()
                .position(|e| e.callback.lock().unwrap().ptr_eq(listener))
            {
                removed = Some(list.remove(index));
            }
        }
        drop(state);
        drop(removed);
        Ok(())
    }
    /// An IDL event handler occupies one list position. Replacement changes
    /// the shared entry that an in-progress dispatch snapshot also observes.
    pub fn set_event_handler(&self, kind: &str, listener: Option<Listener>) {
        self.set_event_handler_utf16(&kind.encode_utf16().collect::<Vec<_>>(), listener)
    }
    pub fn set_event_handler_utf16(&self, kind: &[u16], listener: Option<Listener>) {
        let mut replaced = None;
        let mut removed = None;
        let mut state = self.0.lock().unwrap();
        if listener.is_none() && !state.listeners.contains_key(kind) {
            return;
        }
        let list = state.listeners.entry(kind.to_vec()).or_default();
        if let Some(index) = list.iter().position(|e| e.handler) {
            match listener {
                Some(listener) => {
                    replaced = Some(std::mem::replace(
                        &mut *list[index].callback.lock().unwrap(),
                        listener,
                    ))
                }
                None => {
                    removed = Some(list.remove(index));
                }
            }
        } else if let Some(listener) = listener {
            list.push(Arc::new(Entry {
                callback: Mutex::new(listener),
                once: false,
                handler: true,
            }));
        }
        drop(state);
        drop(replaced);
        drop(removed);
    }
    pub fn event_handler(&self, kind: &str) -> Option<Listener> {
        self.event_handler_utf16(&kind.encode_utf16().collect::<Vec<_>>())
    }
    pub fn event_handler_utf16(&self, kind: &[u16]) -> Option<Listener> {
        self.0
            .lock()
            .unwrap()
            .listeners
            .get(kind)?
            .iter()
            .find(|e| e.handler)
            .map(|e| e.callback.lock().unwrap().clone())
    }
    /// Ordinary `on<type>` properties run after the listener snapshot unless
    /// an IDL entry already handles that event type.
    pub fn set_fallback_handler(&self, kind: &str, listener: Option<Listener>) {
        let kind = kind.encode_utf16().collect::<Vec<_>>();
        let mut state = self.0.lock().unwrap();
        let old = match listener {
            Some(listener) => state.fallback.insert(kind, listener),
            None => state.fallback.remove(&kind),
        };
        drop(state);
        drop(old);
    }
    pub fn dispatch_event(&self, event: &Event) -> Result<bool, EventDispatchError> {
        event.begin(self.clone())?;
        let kind = event.event_type_utf16();
        let snapshot = self
            .0
            .lock()
            .unwrap()
            .listeners
            .get(&kind)
            .cloned()
            .unwrap_or_default();
        for entry in snapshot {
            if event.stopped_immediately() {
                break;
            }
            let listener = entry.callback.lock().unwrap().clone();
            if entry.once {
                self.remove_event_listener_utf16(&kind, &listener, false)?;
            }
            // Do not hold list or callback locks while calling user code.
            // The retained adapter propagates throws and leaves the event in
            // its dispatching state when a callback prevents _end() running.
            listener.invoke(self, event)?;
        }
        let fallback = {
            let state = self.0.lock().unwrap();
            let has_handler = state
                .listeners
                .get(&kind)
                .is_some_and(|list| list.iter().any(|e| e.handler));
            if has_handler {
                None
            } else {
                state.fallback.get(&kind).cloned()
            }
        };
        if !event.stopped_immediately() {
            if let Some(listener) = fallback {
                listener.invoke(self, event)?;
            }
        }
        event.end();
        Ok(!event.default_prevented())
    }
}

/// The detail graph is shared, including cycles and aliases, rather than
/// serialized into JSON before reaching a listener.
#[derive(Clone)]
pub struct CustomEvent {
    pub event: Event,
    detail: Arc<Mutex<SharedEventValue>>,
}
pub type SharedEventValue = Arc<Mutex<crate::clone::StructuredValue>>;
impl CustomEvent {
    pub fn new(kind: &str, init: EventInit, detail: Option<crate::clone::StructuredValue>) -> Self {
        Self::new_utf16(&kind.encode_utf16().collect::<Vec<_>>(), init, detail)
    }
    pub fn new_utf16(
        kind: &[u16],
        init: EventInit,
        detail: Option<crate::clone::StructuredValue>,
    ) -> Self {
        let detail = detail
            .filter(|v| !matches!(v.root, crate::clone::Value::Undefined))
            .unwrap_or(crate::clone::StructuredValue {
                root: crate::clone::Value::Null,
                objects: Vec::new(),
            });
        let detail = Arc::new(Mutex::new(Arc::new(Mutex::new(detail))));
        let event = Event::new_utf16(kind, init);
        event.0.lock().unwrap().custom_detail = Some(detail.clone());
        Self { event, detail }
    }
    pub fn from_shared_detail(kind: &str, init: EventInit, detail: SharedEventValue) -> Self {
        let event = Self::new(kind, init, None);
        if !matches!(detail.lock().unwrap().root, crate::clone::Value::Undefined) {
            event.set_shared_detail(detail);
        }
        event
    }
    pub fn detail(&self) -> SharedEventValue {
        self.detail.lock().unwrap().clone()
    }
    pub fn set_detail(&self, value: crate::clone::StructuredValue) {
        self.set_shared_detail(Arc::new(Mutex::new(value)));
    }
    pub fn set_shared_detail(&self, value: SharedEventValue) {
        *self.detail.lock().unwrap() = value;
    }
}
