// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Shared abort reasons and engine-independent cancellation notification.
use super::{Error, Event, EventDispatchError, EventInit, EventTarget, Listener, ListenerOptions};
use crate::clone::{StructuredValue, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct DomException(Arc<Mutex<ExceptionData>>);
#[derive(Debug)]
struct ExceptionData {
    name: Vec<u16>,
    message: Vec<u16>,
}
impl DomException {
    pub const INDEX_SIZE_ERR: u16 = 1;
    pub const DOMSTRING_SIZE_ERR: u16 = 2;
    pub const HIERARCHY_REQUEST_ERR: u16 = 3;
    pub const WRONG_DOCUMENT_ERR: u16 = 4;
    pub const INVALID_CHARACTER_ERR: u16 = 5;
    pub const NO_DATA_ALLOWED_ERR: u16 = 6;
    pub const NO_MODIFICATION_ALLOWED_ERR: u16 = 7;
    pub const NOT_FOUND_ERR: u16 = 8;
    pub const NOT_SUPPORTED_ERR: u16 = 9;
    pub const INUSE_ATTRIBUTE_ERR: u16 = 10;
    pub const INVALID_STATE_ERR: u16 = 11;
    pub const SYNTAX_ERR: u16 = 12;
    pub const INVALID_MODIFICATION_ERR: u16 = 13;
    pub const NAMESPACE_ERR: u16 = 14;
    pub const INVALID_ACCESS_ERR: u16 = 15;
    pub const VALIDATION_ERR: u16 = 16;
    pub const TYPE_MISMATCH_ERR: u16 = 17;
    pub const SECURITY_ERR: u16 = 18;
    pub const NETWORK_ERR: u16 = 19;
    pub const ABORT_ERR: u16 = 20;
    pub const URL_MISMATCH_ERR: u16 = 21;
    pub const QUOTA_EXCEEDED_ERR: u16 = 22;
    pub const TIMEOUT_ERR: u16 = 23;
    pub const INVALID_NODE_TYPE_ERR: u16 = 24;
    pub const DATA_CLONE_ERR: u16 = 25;
    pub fn new(message: &str, name: &str) -> Self {
        Self::new_utf16(
            &message.encode_utf16().collect::<Vec<_>>(),
            &name.encode_utf16().collect::<Vec<_>>(),
        )
    }
    pub fn new_utf16(message: &[u16], name: &[u16]) -> Self {
        Self(Arc::new(Mutex::new(ExceptionData {
            name: name.to_vec(),
            message: message.to_vec(),
        })))
    }
    pub fn name_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().name.clone()
    }
    pub fn message_utf16(&self) -> Vec<u16> {
        self.0.lock().unwrap().message.clone()
    }
    pub fn name(&self) -> String {
        String::from_utf16_lossy(&self.name_utf16())
    }
    pub fn message(&self) -> String {
        String::from_utf16_lossy(&self.message_utf16())
    }
    pub fn set_name_utf16(&self, name: &[u16]) {
        self.0.lock().unwrap().name = name.to_vec();
    }
    pub fn set_message_utf16(&self, message: &[u16]) {
        self.0.lock().unwrap().message = message.to_vec();
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn code(&self) -> u16 {
        match self.name().as_str() {
            "IndexSizeError" => 1,
            "HierarchyRequestError" => 3,
            "WrongDocumentError" => 4,
            "InvalidCharacterError" => 5,
            "NoModificationAllowedError" => 7,
            "NotFoundError" => 8,
            "NotSupportedError" => 9,
            "InUseAttributeError" => 10,
            "InvalidStateError" => 11,
            "SyntaxError" => 12,
            "InvalidModificationError" => 13,
            "NamespaceError" => 14,
            "InvalidAccessError" => 15,
            "TypeMismatchError" => 17,
            "SecurityError" => 18,
            "NetworkError" => 19,
            "AbortError" => 20,
            "URLMismatchError" => 21,
            "QuotaExceededError" => 22,
            "TimeoutError" => 23,
            "InvalidNodeTypeError" => 24,
            "DataCloneError" => 25,
            _ => 0,
        }
    }
}
impl Default for DomException {
    fn default() -> Self {
        Self::new("", "Error")
    }
}
impl std::fmt::Display for DomException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.0.lock().unwrap();
        let name = String::from_utf16_lossy(&data.name);
        let message = String::from_utf16_lossy(&data.message);
        if name.is_empty() {
            f.write_str(&message)
        } else if message.is_empty() {
            f.write_str(&name)
        } else {
            write!(f, "{name}: {message}")
        }
    }
}
impl std::error::Error for DomException {}

#[derive(Clone, Debug)]
pub struct AbortReason(Arc<ReasonData>);
#[derive(Debug)]
enum ReasonData {
    Dom(DomException),
    Error(Error),
    Value(Arc<Mutex<StructuredValue>>),
}
impl AbortReason {
    pub fn from_value(value: StructuredValue) -> Self {
        Self::from_shared_value(Arc::new(Mutex::new(value)))
    }
    pub fn from_shared_value(value: Arc<Mutex<StructuredValue>>) -> Self {
        Self(Arc::new(ReasonData::Value(value)))
    }
    pub fn error(message: &str) -> Self {
        Self::from(Error::generic_error(message))
    }
    pub fn default_abort() -> Self {
        DomException::new("This operation was aborted", "AbortError").into()
    }
    pub fn timeout() -> Self {
        DomException::new("The operation was aborted due to timeout", "TimeoutError").into()
    }
    pub fn dom_exception(&self) -> Option<DomException> {
        match &*self.0 {
            ReasonData::Dom(d) => Some(d.clone()),
            _ => None,
        }
    }
    pub fn web_error(&self) -> Option<&Error> {
        match &*self.0 {
            ReasonData::Error(e) => Some(e),
            _ => None,
        }
    }
    pub fn value(&self) -> Option<Arc<Mutex<StructuredValue>>> {
        match &*self.0 {
            ReasonData::Value(v) => Some(v.clone()),
            _ => None,
        }
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        match (&*self.0, &*other.0) {
            (ReasonData::Dom(a), ReasonData::Dom(b)) => a.ptr_eq(b),
            (ReasonData::Value(a), ReasonData::Value(b)) => Arc::ptr_eq(a, b),
            _ => Arc::ptr_eq(&self.0, &other.0),
        }
    }
    fn is_undefined(&self) -> bool {
        matches!(&*self.0,ReasonData::Value(v) if matches!(v.lock().unwrap().root,Value::Undefined))
    }
}
impl From<DomException> for AbortReason {
    fn from(value: DomException) -> Self {
        Self(Arc::new(ReasonData::Dom(value)))
    }
}
impl From<Error> for AbortReason {
    fn from(value: Error) -> Self {
        Self(Arc::new(ReasonData::Error(value)))
    }
}
impl std::fmt::Display for AbortReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self.0 {
            ReasonData::Dom(d) => d.fmt(f),
            ReasonData::Error(e) => e.fmt(f),
            ReasonData::Value(v) => {
                let v = v.lock().unwrap();
                match &v.root {
                    Value::Undefined => f.write_str("undefined"),
                    Value::Null => f.write_str("null"),
                    Value::Boolean(v) => v.fmt(f),
                    Value::Number(v) => v.fmt(f),
                    Value::String(v) => f.write_str(&String::from_utf16_lossy(v)),
                    _ => f.write_str("[object Object]"),
                }
            }
        }
    }
}
impl std::error::Error for AbortReason {}

#[derive(Default)]
struct State {
    reason: Option<AbortReason>,
    waiters: BTreeMap<u64, Waker>,
    next: u64,
}
#[derive(Default)]
struct SignalData {
    target: EventTarget,
    state: Mutex<State>,
}
#[derive(Clone)]
pub struct AbortSignal(Arc<SignalData>);
#[derive(Clone, Debug)]
pub struct WeakAbortSignal(Weak<SignalData>);
impl WeakAbortSignal {
    pub fn upgrade(&self) -> Option<AbortSignal> {
        self.0.upgrade().map(AbortSignal)
    }
}
impl std::fmt::Debug for AbortSignal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AbortSignal")
            .field("aborted", &self.aborted())
            .finish()
    }
}
#[derive(Clone, Debug)]
pub struct AbortController {
    signal: AbortSignal,
}
impl Default for AbortController {
    fn default() -> Self {
        Self::new()
    }
}
impl AbortController {
    pub fn new() -> Self {
        Self {
            signal: AbortSignal(Arc::new(SignalData::default())),
        }
    }
    pub fn signal(&self) -> AbortSignal {
        self.signal.clone()
    }
    pub fn abort(&self, reason: Option<AbortReason>) -> Result<(), EventDispatchError> {
        self.signal.abort_inner(reason)
    }
}
impl AbortSignal {
    pub fn downgrade(&self) -> WeakAbortSignal {
        WeakAbortSignal(Arc::downgrade(&self.0))
    }
    pub fn event_target(&self) -> EventTarget {
        self.0.target.clone()
    }
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn aborted(&self) -> bool {
        self.0.state.lock().unwrap().reason.is_some()
    }
    pub fn reason(&self) -> Option<AbortReason> {
        self.0.state.lock().unwrap().reason.clone()
    }
    pub fn throw_if_aborted(&self) -> Result<(), AbortReason> {
        self.reason().map_or(Ok(()), Err)
    }
    pub fn add_event_listener(
        &self,
        kind: &str,
        listener: Option<&Listener>,
        options: ListenerOptions,
    ) -> Result<(), Error> {
        self.0.target.add_event_listener(kind, listener, options)
    }
    pub fn remove_event_listener(
        &self,
        kind: &str,
        listener: &Listener,
        capture: bool,
    ) -> Result<(), Error> {
        self.0.target.remove_event_listener(kind, listener, capture)
    }
    pub fn dispatch_event(&self, event: &Event) -> Result<bool, EventDispatchError> {
        self.0.target.dispatch_event(event)
    }
    pub fn on_abort(&self) -> Option<Listener> {
        self.0.target.event_handler("abort")
    }
    pub fn set_on_abort(&self, listener: Option<Listener>) {
        self.0.target.set_event_handler("abort", listener)
    }
    pub fn abort(reason: Option<AbortReason>) -> Self {
        let controller = AbortController::new();
        controller
            .abort(reason)
            .expect("new signal has no listeners");
        controller.signal()
    }
    pub fn any(signals: impl IntoIterator<Item = AbortSignal>) -> Self {
        let controller = AbortController::new();
        for signal in signals {
            if let Some(reason) = signal.reason() {
                controller
                    .abort(Some(reason))
                    .expect("new derived signal has no listeners");
                break;
            }
            let combined = Arc::downgrade(&controller.signal.0);
            let source = Arc::downgrade(&signal.0);
            let listener = Listener::new(move |_, _| {
                if let (Some(combined), Some(source)) = (combined.upgrade(), source.upgrade()) {
                    AbortSignal(combined).abort_inner(AbortSignal(source).reason())?;
                }
                Ok(())
            });
            signal
                .add_event_listener(
                    "abort",
                    Some(&listener),
                    ListenerOptions {
                        once: true,
                        ..Default::default()
                    },
                )
                .expect("ordinary at-target listener");
            // Close a race between the first state check and subscription.
            if let Some(reason) = signal.reason() {
                controller
                    .abort(Some(reason))
                    .expect("new derived signal has no listeners");
                break;
            }
        }
        controller.signal()
    }
    /// Schedule against the host's timer domain. The scheduler owns the
    /// controller until its callback, and decides event residency and cleanup.
    pub fn timeout<E>(
        ms: f64,
        schedule: impl FnOnce(Duration, AbortController) -> Result<(), E>,
    ) -> Result<Self, E> {
        let controller = AbortController::new();
        let signal = controller.signal();
        let delay = if !ms.is_finite() || ms == 0.0 {
            0
        } else {
            (ms.trunc().rem_euclid(4294967296.0) as u32 as i32).max(0) as u64
        };
        schedule(Duration::from_millis(delay), controller)?;
        Ok(signal)
    }
    pub fn cancelled(&self) -> AbortWait {
        AbortWait {
            signal: self.clone(),
            id: None,
        }
    }
    fn abort_inner(&self, reason: Option<AbortReason>) -> Result<(), EventDispatchError> {
        if self.aborted() {
            return Ok(());
        }
        let reason = reason
            .filter(|r| !r.is_undefined())
            .unwrap_or_else(AbortReason::default_abort);
        let waiters = {
            let mut state = self.0.state.lock().unwrap();
            if state.reason.is_some() {
                return Ok(());
            }
            state.reason = Some(reason);
            std::mem::take(&mut state.waiters)
        };
        // Wake notification is independent of a listener throw. A latched
        // signal must not strand native futures whose reason is now available.
        struct Wake(BTreeMap<u64, Waker>);
        impl Drop for Wake {
            fn drop(&mut self) {
                for (_, waker) in std::mem::take(&mut self.0) {
                    waker.wake();
                }
            }
        }
        let _wake = Wake(waiters);
        self.0
            .target
            .dispatch_event(&Event::new("abort", EventInit::default()))?;
        Ok(())
    }
}
/// Dropping a waiter releases its waker; each poll replaces that waiter's
/// registration rather than appending another retained task.
pub struct AbortWait {
    signal: AbortSignal,
    id: Option<u64>,
}
impl Future for AbortWait {
    type Output = AbortReason;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut replacement = Some(context.waker().clone());
        let signal = self.signal.clone();
        let (result, old) = {
            let mut state = signal.0.state.lock().unwrap();
            if let Some(reason) = state.reason.clone() {
                let old = self.id.take().and_then(|id| state.waiters.remove(&id));
                (Poll::Ready(reason), old)
            } else {
                let id = match self.id {
                    Some(id) => id,
                    None => {
                        let id = state.next;
                        state.next = state
                            .next
                            .checked_add(1)
                            .expect("abort waiter identifier space exhausted");
                        self.id = Some(id);
                        id
                    }
                };
                let old = if state
                    .waiters
                    .get(&id)
                    .is_some_and(|w| w.will_wake(context.waker()))
                {
                    None
                } else {
                    state.waiters.insert(id, replacement.take().unwrap())
                };
                (Poll::Pending, old)
            }
        };
        drop(old);
        drop(replacement);
        result
    }
}
impl Drop for AbortWait {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let removed = self.signal.0.state.lock().unwrap().waiters.remove(&id);
            drop(removed);
        }
    }
}
