// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Shared byte pipes with read acknowledgements and owned BYOB buffers.
//! No executor or cell authority is stored here. A write starts when called;
//! its future observes consumption rather than driving the other endpoint.
use crate::{
    clone::{StructuredValue, Value},
    web::{AbortController, AbortReason, AbortSignal, Error},
};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, Weak,
    },
    task::{Context, Poll, Waker},
};
type Result<T> = std::result::Result<T, AbortReason>;
const READ_RELEASED: &str = "This ReadableStream reader has been released.";
const WRITE_RELEASED: &str = "This WritableStream writer has been released.";
fn type_error(message: &str) -> AbortReason {
    Error::type_error(message).into()
}
fn undefined() -> AbortReason {
    AbortReason::from_value(StructuredValue {
        root: Value::Undefined,
        objects: Vec::new(),
    })
}
fn cancel_error(reason: Option<AbortReason>) -> AbortReason {
    let Some(reason) = reason else {
        return AbortReason::error("Stream was cancelled.");
    };
    if reason.web_error().is_some() || reason.dom_exception().is_some() {
        return reason;
    }
    let value = reason
        .value()
        .expect("an owned reason has a value or error");
    let value = value.lock().unwrap();
    let message = match &value.root {
        Value::Undefined => "Stream was cancelled.".into(),
        Value::String(s) => return Error::generic_error_utf16(s.clone()).into(),
        Value::Number(n) => ryu_js::Buffer::new().format(*n).to_owned(),
        Value::Null => "null".into(),
        Value::Boolean(b) => b.to_string(),
        _ => "[object Object]".into(),
    };
    AbortReason::error(&message)
}

/// A transferred buffer and view. Bytes outside the view survive BYOB reads.
#[derive(Debug)]
pub struct ByteView {
    buffer: Vec<u8>,
    offset: usize,
    length: usize,
    element_size: usize,
}
impl ByteView {
    pub fn new(buffer: Vec<u8>, offset: usize, length: usize, element_size: usize) -> Result<Self> {
        if !matches!(element_size, 1 | 2 | 4 | 8)
            || !offset.is_multiple_of(element_size)
            || !length.is_multiple_of(element_size)
            || offset
                .checked_add(length)
                .is_none_or(|end| end > buffer.len())
        {
            return Err(type_error("Invalid ArrayBufferView"));
        }
        Ok(Self {
            buffer,
            offset,
            length,
            element_size,
        })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.buffer[self.offset..self.offset + self.length]
    }
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }
    pub fn byte_offset(&self) -> usize {
        self.offset
    }
    pub fn byte_length(&self) -> usize {
        self.length
    }
    pub fn element_size(&self) -> usize {
        self.element_size
    }
    pub fn into_bytes(mut self) -> Vec<u8> {
        self.buffer.truncate(self.offset + self.length);
        if self.offset != 0 {
            self.buffer.drain(..self.offset);
        }
        self.buffer
    }
}
impl From<Vec<u8>> for ByteView {
    fn from(buffer: Vec<u8>) -> Self {
        let length = buffer.len();
        Self {
            buffer,
            offset: 0,
            length,
            element_size: 1,
        }
    }
}
#[derive(Debug)]
pub struct ByteRead {
    pub value: Option<ByteView>,
    pub done: bool,
}

#[derive(Default)]
struct Settled {
    result: Option<Result<()>>,
    waiters: BTreeMap<u64, Waker>,
    next: u64,
}
#[derive(Clone, Default)]
struct Settlement(Arc<Mutex<Settled>>);
impl Settlement {
    fn future(&self) -> StreamWait {
        StreamWait {
            completion: self.clone(),
            registration: None,
        }
    }
    fn complete(&self, result: Result<()>, effects: &mut Effects) {
        let mut s = self.0.lock().unwrap();
        if s.result.is_none() {
            s.result = Some(result);
            effects
                .wake
                .extend(std::mem::take(&mut s.waiters).into_values());
        }
    }
}
/// Repeatable shared ready/closed/abort settlement. Each poll replaces its
/// registration; dropping a waiter releases its waker without cancelling I/O.
pub struct StreamWait {
    completion: Settlement,
    registration: Option<u64>,
}
impl Clone for StreamWait {
    fn clone(&self) -> Self {
        self.completion.future()
    }
}
impl StreamWait {
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.completion.0, &other.completion.0)
    }
}
impl Future for StreamWait {
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = cx.waker().clone();
        let completion = self.completion.clone();
        let mut s = completion.0.lock().unwrap();
        if let Some(result) = s.result.clone() {
            let old = self
                .registration
                .take()
                .and_then(|id| s.waiters.remove(&id));
            drop(s);
            drop(old);
            return Poll::Ready(result);
        }
        let id = *self.registration.get_or_insert_with(|| {
            let id = s.next;
            s.next = s
                .next
                .checked_add(1)
                .expect("stream waiter identity exhausted");
            id
        });
        let old = s.waiters.insert(id, waker);
        drop(s);
        drop(old);
        Poll::Pending
    }
}
impl Drop for StreamWait {
    fn drop(&mut self) {
        if let Some(id) = self.registration.take() {
            let old = self.completion.0.lock().unwrap().waiters.remove(&id);
            drop(old);
        }
    }
}

#[derive(Default)]
struct ReadCompletion {
    result: Option<Result<ByteRead>>,
    waker: Option<Waker>,
}
type ReadSlot = Arc<Mutex<ReadCompletion>>;
/// Dropping an unfinished Rust read removes its reservation. Already consumed
/// BYOB bytes stay consumed, just as releasing its reader discards a descriptor.
pub struct ByteReadFuture {
    slot: ReadSlot,
    pipe: Weak<Pipe>,
    id: u64,
}
impl Future for ByteReadFuture {
    type Output = Result<ByteRead>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker = cx.waker().clone();
        let mut s = self.slot.lock().unwrap();
        if let Some(result) = s.result.take() {
            return Poll::Ready(result);
        }
        let old = s.waker.replace(waker);
        drop(s);
        drop(old);
        Poll::Pending
    }
}
impl Drop for ByteReadFuture {
    fn drop(&mut self) {
        if let Some(pipe) = self.pipe.upgrade() {
            pipe.change(|s, e| {
                if s.read.as_ref().is_some_and(|r| r.id == self.id) {
                    if let Some(read) = s.read.take() {
                        if let ReadKind::Into { view, .. } = read.kind {
                            e.buffers.push(view);
                        }
                    }
                }
            });
        }
        let waker = self.slot.lock().unwrap().waker.take();
        drop(waker);
    }
}
#[derive(Default)]
struct Effects {
    wake: Vec<Waker>,
    buffers: Vec<ByteView>,
}
impl Effects {
    fn read(&mut self, slot: &ReadSlot, result: Result<ByteRead>) {
        let mut slot = slot.lock().unwrap();
        slot.result = Some(result);
        if let Some(w) = slot.waker.take() {
            self.wake.push(w);
        }
    }
}
struct Write {
    view: ByteView,
    consumed: usize,
    completion: Settlement,
}
enum ReadKind {
    Default,
    Into {
        view: ByteView,
        filled: usize,
        min_fill: usize,
    },
}
struct Read {
    id: u64,
    kind: ReadKind,
    slot: ReadSlot,
}
#[derive(Clone)]
enum Status {
    Open,
    Closed,
    Errored(AbortReason),
}
struct Writer {
    id: u64,
    ready: Settlement,
    closed: Settlement,
}
struct Reader {
    id: u64,
    closed: Settlement,
}
struct State {
    readable: Status,
    writable: Status,
    disturbed: bool,
    reader: Option<Reader>,
    writer: Option<Writer>,
    queue: VecDeque<Write>,
    active: Option<Write>,
    read: Option<Read>,
    close: Option<Settlement>,
    abort: Option<Settlement>,
    readable_closed: Settlement,
    backpressure: bool,
    next: u64,
    seen: u64,
    expected: Option<u64>,
}
struct Pipe {
    state: Mutex<State>,
    controller: AbortController,
}
impl Pipe {
    fn new(expected: Option<u64>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                readable: Status::Open,
                writable: Status::Open,
                disturbed: false,
                reader: None,
                writer: None,
                queue: VecDeque::new(),
                active: None,
                read: None,
                close: None,
                abort: None,
                readable_closed: Settlement::default(),
                backpressure: false,
                next: 0,
                seen: 0,
                expected,
            }),
            controller: AbortController::new(),
        })
    }
    fn change<T>(&self, f: impl FnOnce(&mut State, &mut Effects) -> T) -> T {
        let mut effects = Effects::default();
        let result = {
            let mut s = self.state.lock().unwrap();
            f(&mut s, &mut effects)
        };
        // Wake and user-owned waker drops can reenter either endpoint.
        for waker in effects.wake {
            waker.wake();
        }
        drop(effects.buffers);
        result
    }
}
impl Drop for Pipe {
    fn drop(&mut self) {
        // Settlements may outlive both endpoints. Losing the last native owner
        // must release their registered wakers and settle abandoned writes.
        let mut effects = Effects::default();
        let state = self.state.get_mut().unwrap();
        let reason = AbortReason::error("The byte stream was dropped");
        state.error_readable(reason.clone(), &mut effects);
        state.error_writable(reason, &mut effects);
        for waker in effects.wake {
            waker.wake();
        }
    }
}
impl State {
    fn identity(&mut self) -> u64 {
        let id = self.next;
        self.next = self.next.checked_add(1).expect("stream identity exhausted");
        id
    }
    fn desired(&self) -> Option<f64> {
        match self.writable {
            Status::Open => Some(1.0 - self.queue.len() as f64 - f64::from(self.active.is_some())),
            Status::Closed => Some(0.0),
            Status::Errored(_) => None,
        }
    }
    fn update_ready(&mut self, e: &mut Effects) {
        let bp = self.active.is_some() || !self.queue.is_empty();
        if bp != self.backpressure {
            self.backpressure = bp;
            if let Some(writer) = &mut self.writer {
                if bp {
                    writer.ready = Settlement::default();
                } else {
                    writer.ready.complete(Ok(()), e);
                }
            }
        }
    }
    fn close_readable(&mut self, e: &mut Effects) {
        if matches!(self.readable, Status::Open) {
            self.readable = Status::Closed;
            self.readable_closed.complete(Ok(()), e);
            if let Some(r) = &self.reader {
                r.closed.complete(Ok(()), e);
            }
        }
        if let Some(read) = self.read.take() {
            let result = match read.kind {
                ReadKind::Default => ByteRead {
                    value: None,
                    done: true,
                },
                ReadKind::Into {
                    mut view, filled, ..
                } => {
                    view.length = filled / view.element_size * view.element_size;
                    ByteRead {
                        value: Some(view),
                        done: filled == 0,
                    }
                }
            };
            e.read(&read.slot, Ok(result));
        }
    }
    fn error_readable(&mut self, reason: AbortReason, e: &mut Effects) {
        if !matches!(self.readable, Status::Open) {
            return;
        }
        self.readable = Status::Errored(reason.clone());
        self.readable_closed.complete(Err(reason.clone()), e);
        if let Some(r) = &self.reader {
            r.closed.complete(Err(reason.clone()), e);
        }
        if let Some(read) = self.read.take() {
            if let ReadKind::Into { view, .. } = read.kind {
                e.buffers.push(view);
            }
            e.read(&read.slot, Err(reason));
        }
    }
    fn error_writable(&mut self, reason: AbortReason, e: &mut Effects) {
        if !matches!(self.writable, Status::Open) {
            return;
        }
        self.writable = Status::Errored(reason.clone());
        if let Some(write) = self.active.take() {
            write.completion.complete(Err(reason.clone()), e);
            e.buffers.push(write.view);
        }
        for write in self.queue.drain(..) {
            write.completion.complete(Err(reason.clone()), e);
            e.buffers.push(write.view);
        }
        if let Some(close) = self.close.take() {
            close.complete(Err(reason.clone()), e);
        }
        if let Some(writer) = &self.writer {
            writer.ready.complete(Err(reason.clone()), e);
            writer.closed.complete(Err(reason), e);
        }
    }
    fn pump(&mut self, e: &mut Effects) {
        while matches!(self.writable, Status::Open) {
            if self.active.is_none() {
                if let Some(write) = self.queue.pop_front() {
                    let length = write.view.length as u64;
                    self.seen = self.seen.saturating_add(length);
                    if self.expected.is_some_and(|limit| self.seen > limit) {
                        let reason = type_error(
                            "Attempt to write too many bytes through a FixedLengthStream.",
                        );
                        write.completion.complete(Err(reason.clone()), e);
                        e.buffers.push(write.view);
                        self.error_readable(reason.clone(), e);
                        self.error_writable(reason, e);
                        break;
                    }
                    if length == 0 {
                        write.completion.complete(Ok(()), e);
                        e.buffers.push(write.view);
                        continue;
                    }
                    self.active = Some(write);
                } else if let Some(close) = self.close.take() {
                    if self.expected.is_some_and(|limit| self.seen < limit) {
                        let reason = type_error(
                            "FixedLengthStream did not see all expected bytes before close().",
                        );
                        close.complete(Err(reason.clone()), e);
                        self.error_readable(reason.clone(), e);
                        self.error_writable(reason, e);
                    } else {
                        self.writable = Status::Closed;
                        close.complete(Ok(()), e);
                        self.close_readable(e);
                        if let Some(writer) = &self.writer {
                            writer.closed.complete(Ok(()), e);
                        }
                    }
                    break;
                } else {
                    break;
                }
            }
            let Some(read) = self.read.take() else {
                break;
            };
            let mut write = self.active.take().unwrap();
            match read.kind {
                ReadKind::Default => {
                    write.view.offset += write.consumed;
                    write.view.length -= write.consumed;
                    write.view.element_size = 1;
                    e.read(
                        &read.slot,
                        Ok(ByteRead {
                            value: Some(write.view),
                            done: false,
                        }),
                    );
                    write.completion.complete(Ok(()), e);
                }
                ReadKind::Into {
                    mut view,
                    mut filled,
                    min_fill,
                } => {
                    let n = (write.view.length - write.consumed).min(view.length - filled);
                    let offset = write.view.offset + write.consumed;
                    view.buffer[view.offset + filled..view.offset + filled + n]
                        .copy_from_slice(&write.view.buffer[offset..offset + n]);
                    filled += n;
                    write.consumed += n;
                    if write.consumed == write.view.length {
                        write.completion.complete(Ok(()), e);
                        e.buffers.push(write.view);
                    } else {
                        self.active = Some(write);
                    }
                    if filled >= min_fill {
                        view.length = filled / view.element_size * view.element_size;
                        e.read(
                            &read.slot,
                            Ok(ByteRead {
                                value: Some(view),
                                done: false,
                            }),
                        );
                    } else {
                        self.read = Some(Read {
                            id: read.id,
                            kind: ReadKind::Into {
                                view,
                                filled,
                                min_fill,
                            },
                            slot: read.slot,
                        });
                    }
                }
            }
        }
        if self.close.is_none() && matches!(self.writable, Status::Open) {
            self.update_ready(e);
        }
    }
}

#[derive(Clone)]
pub struct ByteReadable(Arc<Pipe>);
#[derive(Clone)]
pub struct ByteWritable(Arc<Pipe>);
pub struct IdentityTransformStream {
    pub readable: ByteReadable,
    pub writable: ByteWritable,
}
impl Default for IdentityTransformStream {
    fn default() -> Self {
        Self::new()
    }
}
impl IdentityTransformStream {
    pub fn new() -> Self {
        let p = Pipe::new(None);
        Self {
            readable: ByteReadable(p.clone()),
            writable: ByteWritable(p),
        }
    }
}
pub struct FixedLengthStream {
    pub readable: ByteReadable,
    pub writable: ByteWritable,
}
impl FixedLengthStream {
    pub fn new(expected_length: f64) -> Result<Self> {
        let length = expected_length.trunc();
        if !length.is_finite() || !(0.0..=9_007_199_254_740_991.0).contains(&length) {
            return Err(type_error(
                "FixedLengthStream requires an integer expected length less than 2^53.",
            ));
        }
        let p = Pipe::new(Some(length as u64));
        Ok(Self {
            readable: ByteReadable(p.clone()),
            writable: ByteWritable(p),
        })
    }
}
struct ReaderLease {
    pipe: Arc<Pipe>,
    id: u64,
    byob: bool,
    released: AtomicBool,
    closed: Settlement,
}
impl ReaderLease {
    fn release(&self) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        let reason = type_error(READ_RELEASED);
        self.pipe.change(|s, e| {
            if s.reader.as_ref().is_some_and(|r| r.id == self.id) {
                s.reader = None;
                if let Some(read) = s.read.take() {
                    if let ReadKind::Into { view, .. } = read.kind {
                        e.buffers.push(view);
                    }
                    e.read(&read.slot, Err(reason.clone()));
                }
            }
            if !self.byob {
                self.closed.complete(Err(reason), e);
            }
        });
    }
}
impl Drop for ReaderLease {
    fn drop(&mut self) {
        self.release();
    }
}
#[derive(Clone)]
pub struct ByteReader(Arc<ReaderLease>);
impl ByteReadable {
    pub fn locked(&self) -> bool {
        self.0.state.lock().unwrap().reader.is_some()
    }
    pub fn disturbed(&self) -> bool {
        self.0.state.lock().unwrap().disturbed
    }
    pub fn expected_length(&self) -> Option<u64> {
        self.0.state.lock().unwrap().expected
    }
    pub fn is_closed(&self) -> bool {
        matches!(self.0.state.lock().unwrap().readable, Status::Closed)
    }
    pub fn get_reader(&self) -> Result<ByteReader> {
        self.reader(false)
    }
    pub fn get_byob_reader(&self) -> Result<ByteReader> {
        self.reader(true)
    }
    fn reader(&self, byob: bool) -> Result<ByteReader> {
        self.0.change(|s, e| {
            if s.reader.is_some() {
                return Err(type_error("ReadableStream is locked"));
            }
            let id = s.identity();
            let closed = if byob {
                s.readable_closed.clone()
            } else {
                Settlement::default()
            };
            match &s.readable {
                Status::Closed => closed.complete(Ok(()), e),
                Status::Errored(r) => closed.complete(Err(r.clone()), e),
                Status::Open => {}
            }
            s.reader = Some(Reader {
                id,
                closed: closed.clone(),
            });
            Ok(ByteReader(Arc::new(ReaderLease {
                pipe: self.0.clone(),
                id,
                byob,
                released: AtomicBool::new(false),
                closed,
            })))
        })
    }
    pub fn cancel(&self, reason: Option<AbortReason>) -> Result<()> {
        self.cancel_for(reason, None)
    }
    fn cancel_for(&self, reason: Option<AbortReason>, reader: Option<&ReaderLease>) -> Result<()> {
        let error = cancel_error(reason);
        self.0.change(|s, e| {
            if let Some(reader) = reader {
                if reader.released.load(Ordering::SeqCst) {
                    return Err(type_error(READ_RELEASED));
                }
            } else if s.reader.is_some() {
                return Err(type_error("Cannot cancel a locked ReadableStream"));
            }
            if let Status::Errored(reason) = &s.readable {
                return Err(reason.clone());
            }
            if matches!(s.readable, Status::Open) {
                s.disturbed = true;
                if let Some(read) = s.read.take() {
                    if let ReadKind::Into { view, .. } = read.kind {
                        e.buffers.push(view);
                    }
                    e.read(
                        &read.slot,
                        Ok(ByteRead {
                            value: None,
                            done: true,
                        }),
                    );
                }
                s.close_readable(e);
                s.error_writable(error, e);
            }
            Ok(())
        })
    }
    /// Fail both endpoints when a native producer is retired or dropped.
    /// Controller errors do not dispatch user abort listeners.
    pub fn error(&self, reason: AbortReason) {
        self.0.change(|s, e| {
            s.error_readable(reason.clone(), e);
            s.error_writable(reason, e);
        });
    }
}
impl ByteReader {
    pub fn release_lock(&self) {
        self.0.release();
    }
    pub fn closed(&self) -> StreamWait {
        if self.0.released.load(Ordering::SeqCst) {
            settled(Err(type_error(READ_RELEASED)))
        } else {
            self.0.closed.future()
        }
    }
    pub fn cancel(&self, reason: Option<AbortReason>) -> Result<()> {
        ByteReadable(self.0.pipe.clone()).cancel_for(reason, Some(&self.0))
    }
    pub fn read(&self) -> ByteReadFuture {
        self.request(ReadKind::Default, None)
    }
    pub fn read_into(&self, view: ByteView, min: usize) -> ByteReadFuture {
        let error = if view.length == 0 {
            Some(type_error("read() requires a non-empty ArrayBufferView"))
        } else if min == 0 || min > view.length / view.element_size {
            Some(type_error("invalid min"))
        } else {
            None
        };
        let min_fill = min.saturating_mul(view.element_size);
        self.request(
            ReadKind::Into {
                view,
                filled: 0,
                min_fill,
            },
            error,
        )
    }
    fn request(&self, kind: ReadKind, error: Option<AbortReason>) -> ByteReadFuture {
        let slot = Arc::new(Mutex::new(ReadCompletion::default()));
        let id=self.0.pipe.change(|s,e| {
            let id=s.identity();
            let error=if self.0.released.load(Ordering::SeqCst){Some(type_error(READ_RELEASED))}else if matches!(kind,ReadKind::Into{..})!=self.0.byob {Some(type_error("Invalid reader mode"))}else{error};
            if let Some(error)=error {if let ReadKind::Into{view,..}=kind{e.buffers.push(view);}e.read(&slot,Err(error));return id;}
            s.disturbed=true;
            if let Status::Errored(reason)=&s.readable {e.read(&slot,Err(reason.clone()));if let ReadKind::Into{view,..}=kind{e.buffers.push(view);}return id;}
            if s.read.is_some() {e.read(&slot,Err(type_error("This ReadableStream only supports a single pending read request at a time.")));if let ReadKind::Into{view,..}=kind{e.buffers.push(view);}return id;}
            s.read=Some(Read{id,kind,slot:slot.clone()});
            if matches!(s.readable,Status::Closed){s.close_readable(e);}else{s.pump(e);}id
        });
        ByteReadFuture {
            slot,
            pipe: Arc::downgrade(&self.0.pipe),
            id,
        }
    }
}
struct WriterLease {
    pipe: Arc<Pipe>,
    id: u64,
    released: AtomicBool,
}
impl WriterLease {
    fn release(&self) {
        if self.released.swap(true, Ordering::SeqCst) {
            return;
        }
        self.pipe.change(|s, e| {
            if s.writer.as_ref().is_some_and(|w| w.id == self.id) {
                let writer = s.writer.take().unwrap();
                let reason = type_error(WRITE_RELEASED);
                writer.ready.complete(Err(reason.clone()), e);
                writer.closed.complete(Err(reason.clone()), e);
                for write in s.queue.drain(..) {
                    write.completion.complete(Err(reason.clone()), e);
                    e.buffers.push(write.view);
                }
            }
        });
    }
}
impl Drop for WriterLease {
    fn drop(&mut self) {
        self.release();
    }
}
#[derive(Clone)]
pub struct ByteWriter(Arc<WriterLease>);
fn settled(result: Result<()>) -> StreamWait {
    let s = Settlement::default();
    s.complete(result, &mut Effects::default());
    s.future()
}
impl ByteWritable {
    pub fn locked(&self) -> bool {
        self.0.state.lock().unwrap().writer.is_some()
    }
    pub fn signal(&self) -> AbortSignal {
        self.0.controller.signal()
    }
    pub fn get_writer(&self) -> Result<ByteWriter> {
        self.0.change(|s, e| {
            if s.writer.is_some() {
                return Err(type_error("WritableStream is locked"));
            }
            let id = s.identity();
            let ready = Settlement::default();
            let closed = Settlement::default();
            match &s.writable {
                Status::Open => {
                    if !s.backpressure || s.close.is_some() {
                        ready.complete(Ok(()), e);
                    }
                }
                Status::Closed => {
                    ready.complete(Ok(()), e);
                    closed.complete(Ok(()), e);
                }
                Status::Errored(r) => {
                    ready.complete(Err(r.clone()), e);
                    closed.complete(Err(r.clone()), e);
                }
            }
            s.writer = Some(Writer { id, ready, closed });
            Ok(ByteWriter(Arc::new(WriterLease {
                pipe: self.0.clone(),
                id,
                released: AtomicBool::new(false),
            })))
        })
    }
    pub fn close(&self) -> StreamWait {
        self.close_for(None)
    }
    fn close_for(&self, writer: Option<&WriterLease>) -> StreamWait {
        self.0.change(|s, e| {
            if let Some(writer) = writer {
                if writer.released.load(Ordering::SeqCst) {
                    return settled(Err(type_error(WRITE_RELEASED)));
                }
            } else if s.writer.is_some() {
                return settled(Err(type_error("Cannot close a locked stream")));
            }
            if !matches!(s.writable, Status::Open) {
                return settled(Err(type_error("Stream already closed/errored")));
            }
            if s.close.is_some() {
                return settled(Err(type_error("Stream already closing")));
            }
            let close = Settlement::default();
            let future = close.future();
            s.close = Some(close);
            if let Some(writer) = &s.writer {
                writer.ready.complete(Ok(()), e);
            }
            s.pump(e);
            future
        })
    }
    pub fn abort(&self, reason: Option<AbortReason>) -> StreamWait {
        self.abort_for(reason, None)
    }
    fn abort_for(&self, reason: Option<AbortReason>, writer: Option<&WriterLease>) -> StreamWait {
        let reason = reason.unwrap_or_else(undefined);
        let read_error = cancel_error(Some(reason.clone()));
        let (future, signal) = self.0.change(|s, e| {
            if let Some(writer) = writer {
                if writer.released.load(Ordering::SeqCst) {
                    return (settled(Err(type_error(WRITE_RELEASED))), false);
                }
            } else if s.writer.is_some() {
                return (
                    settled(Err(type_error("Cannot abort a locked stream"))),
                    false,
                );
            }
            if let Some(abort) = &s.abort {
                return (abort.future(), false);
            }
            match &s.writable {
                Status::Closed => return (settled(Ok(())), false),
                Status::Errored(r) => return (settled(Err(r.clone())), false),
                Status::Open => {}
            }
            let abort = Settlement::default();
            let future = abort.future();
            s.abort = Some(abort.clone());
            s.error_readable(read_error, e);
            s.error_writable(reason.clone(), e);
            abort.complete(Ok(()), e);
            (future, true)
        });
        // All shared state locks are released before invoking sink listeners.
        if signal {
            let _ = self.0.controller.abort(Some(reason));
        }
        future
    }
}
impl ByteWriter {
    pub fn release_lock(&self) {
        self.0.release();
    }
    pub fn desired_size(&self) -> Option<f64> {
        self.0.pipe.state.lock().unwrap().desired()
    }
    pub fn ready(&self) -> StreamWait {
        self.wait(false)
    }
    pub fn closed(&self) -> StreamWait {
        self.wait(true)
    }
    fn wait(&self, closed: bool) -> StreamWait {
        let s = self.0.pipe.state.lock().unwrap();
        if self.0.released.load(Ordering::SeqCst) {
            return settled(Err(type_error(WRITE_RELEASED)));
        }
        let writer = s.writer.as_ref().expect("a live writer owns its slot");
        if closed {
            writer.closed.future()
        } else {
            writer.ready.future()
        }
    }
    pub fn write(&self, bytes: Vec<u8>) -> StreamWait {
        self.write_view(bytes.into())
    }
    pub fn write_view(&self, view: ByteView) -> StreamWait {
        if self.0.released.load(Ordering::SeqCst) {
            return settled(Err(type_error(WRITE_RELEASED)));
        }
        self.0.pipe.change(|s, e| {
            if self.0.released.load(Ordering::SeqCst) {
                e.buffers.push(view);
                return settled(Err(type_error(WRITE_RELEASED)));
            }
            let error = match &s.writable {
                Status::Errored(r) => Some(r.clone()),
                Status::Closed => Some(type_error("Cannot write to a closing/closed stream")),
                Status::Open if s.close.is_some() => {
                    Some(type_error("Cannot write to a closing/closed stream"))
                }
                _ => None,
            };
            if let Some(error) = error {
                e.buffers.push(view);
                return settled(Err(error));
            }
            let completion = Settlement::default();
            let future = completion.future();
            s.queue.push_back(Write {
                view,
                consumed: 0,
                completion,
            });
            s.update_ready(e);
            s.pump(e);
            future
        })
    }
    pub fn close(&self) -> StreamWait {
        ByteWritable(self.0.pipe.clone()).close_for(Some(&self.0))
    }
    pub fn abort(&self, reason: Option<AbortReason>) -> StreamWait {
        ByteWritable(self.0.pipe.clone()).abort_for(reason, Some(&self.0))
    }
}
