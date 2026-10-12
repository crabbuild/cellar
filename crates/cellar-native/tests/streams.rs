// Copyright 2026 Deno Land Inc. Apache-2.0 license.
use cellar_native::{
    clone::{StructuredValue, Value},
    streams::*,
    web::{AbortReason, DomException, Listener, ListenerOptions},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};
#[derive(Default)]
struct Counter(AtomicUsize);
impl Wake for Counter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn poll<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(Counter::default()));
    Pin::new(future).poll(&mut Context::from_waker(&waker))
}
fn ready<F: Future + Unpin>(mut future: F) -> F::Output {
    match poll(&mut future) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("operation did not settle"),
    }
}
fn reason(value: Value) -> AbortReason {
    AbortReason::from_value(StructuredValue {
        root: value,
        objects: Vec::new(),
    })
}
fn name(reason: &AbortReason) -> String {
    reason
        .web_error()
        .map(|e| e.name().to_owned())
        .or_else(|| reason.dom_exception().map(|e| e.name()))
        .unwrap_or_else(|| "value".into())
}

#[test]
fn shared_pipe_acks_consumption_keeps_fifo_and_transfers_the_buffer() {
    let stream = IdentityTransformStream::new();
    let readable = stream.readable.clone();
    let writable = stream.writable.clone();
    let writer = writable.get_writer().unwrap();
    let reader = readable.get_reader().unwrap();
    assert!(readable.locked());
    assert!(writable.locked());
    assert!(readable.get_byob_reader().is_err());
    assert!(writable.get_writer().is_err());
    assert_eq!(writer.desired_size(), Some(1.0));
    ready(writer.ready()).unwrap();
    let bytes = vec![1, 2, 3];
    let address = bytes.as_ptr();
    let mut first = writer.write(bytes);
    let mut second = writer.write(vec![4, 5]);
    assert_eq!(writer.desired_size(), Some(-1.0));
    assert!(poll(&mut first).is_pending());
    assert!(poll(&mut second).is_pending());
    let mut backpressure = writer.ready();
    assert!(poll(&mut backpressure).is_pending());
    let chunk = ready(reader.read()).unwrap().value.unwrap();
    assert_eq!(chunk.bytes(), [1, 2, 3]);
    assert_eq!(chunk.buffer().as_ptr(), address);
    ready(first).unwrap();
    assert_eq!(writer.desired_size(), Some(0.0));
    assert!(poll(&mut backpressure).is_pending());
    assert_eq!(ready(reader.read()).unwrap().value.unwrap().bytes(), [4, 5]);
    ready(second).unwrap();
    ready(backpressure).unwrap();
    ready(writer.write(Vec::new())).unwrap();
    assert_eq!(writer.desired_size(), Some(1.0));
    assert!(readable.disturbed());
    let mut read = reader.read();
    assert!(poll(&mut read).is_pending());
    ready(writer.close()).unwrap();
    assert!(ready(read).unwrap().done);
    ready(reader.closed()).unwrap();
    ready(writer.closed()).unwrap();
    assert_eq!(writer.desired_size(), Some(0.0));
    assert_eq!(
        ready(writer.close()).unwrap_err().to_string(),
        "Stream already closed/errored"
    );
    assert_eq!(
        ready(writer.write(vec![9])).unwrap_err().to_string(),
        "Cannot write to a closing/closed stream"
    );
    reader.release_lock();
    writer.release_lock();
    assert!(!readable.locked());
    assert!(!writable.locked());
}

#[test]
fn byob_minimum_spans_writes_preserves_offsets_and_returns_partial_on_close() {
    let stream = IdentityTransformStream::new();
    let writer = stream.writable.get_writer().unwrap();
    let reader = stream.readable.get_byob_reader().unwrap();
    let mut first = writer.write(vec![1, 2, 3, 4]);
    let second = writer.write(vec![5, 6]);
    let part = ready(reader.read_into(ByteView::new(vec![9; 8], 2, 2, 1).unwrap(), 1))
        .unwrap()
        .value
        .unwrap();
    assert_eq!(part.bytes(), [1, 2]);
    assert_eq!(part.buffer(), [9, 9, 1, 2, 9, 9, 9, 9]);
    assert!(poll(&mut first).is_pending());
    let part = ready(reader.read_into(ByteView::new(vec![9; 8], 2, 4, 2).unwrap(), 2))
        .unwrap()
        .value
        .unwrap();
    assert_eq!(part.bytes(), [3, 4, 5, 6]);
    assert_eq!(part.element_size(), 2);
    ready(first).unwrap();
    ready(second).unwrap();
    let write = writer.write(vec![7]);
    let mut partial = reader.read_into(vec![9; 4].into(), 3);
    assert!(poll(&mut partial).is_pending());
    ready(write).unwrap();
    assert_eq!(
        ready(reader.read_into(vec![0; 1].into(), 1))
            .unwrap_err()
            .to_string(),
        "This ReadableStream only supports a single pending read request at a time."
    );
    ready(writer.close()).unwrap();
    let partial = ready(partial).unwrap();
    assert!(!partial.done);
    assert_eq!(partial.value.unwrap().bytes(), [7]);
    let end = ready(reader.read_into(ByteView::new(vec![8; 8], 2, 4, 2).unwrap(), 1)).unwrap();
    assert!(end.done);
    let view = end.value.unwrap();
    assert_eq!(view.byte_length(), 0);
    assert_eq!(view.byte_offset(), 2);
    assert_eq!(view.buffer(), [8; 8]);
    assert_eq!(
        ready(reader.read_into(vec![0; 2].into(), 0))
            .unwrap_err()
            .to_string(),
        "invalid min"
    );
}

#[test]
fn released_readers_reject_pending_reads_and_released_writers_keep_only_active_write() {
    let stream = IdentityTransformStream::new();
    let reader = stream.readable.get_reader().unwrap();
    let closed = reader.closed();
    let pending = reader.read();
    let old = reader.clone();
    reader.release_lock();
    assert_eq!(
        ready(pending).unwrap_err().to_string(),
        "This ReadableStream reader has been released."
    );
    assert_eq!(
        ready(closed).unwrap_err().to_string(),
        "This ReadableStream reader has been released."
    );
    assert!(ready(old.read()).is_err());
    let reader = stream.readable.get_reader().unwrap();
    let writer = stream.writable.get_writer().unwrap();
    let active = writer.write(vec![1]);
    let queued = writer.write(vec![2]);
    let closed = writer.closed();
    let ready_before_release = writer.ready();
    writer.release_lock();
    assert_eq!(
        ready(queued).unwrap_err().to_string(),
        "This WritableStream writer has been released."
    );
    assert!(ready(closed).is_err());
    assert!(ready(ready_before_release).is_err());
    let next = stream.writable.get_writer().unwrap();
    assert_eq!(next.desired_size(), Some(0.0));
    assert_eq!(ready(reader.read()).unwrap().value.unwrap().bytes(), [1]);
    ready(active).unwrap();
    ready(next.ready()).unwrap();
    ready(next.close()).unwrap();
    assert!(ready(reader.read()).unwrap().done);
    assert!(ready(writer.write(vec![3])).is_err());
    // A captured BYOB closed settlement belongs to the stream, unlike the
    // default internal reader's per-reader settlement.
    let stream = IdentityTransformStream::new();
    let reader = stream.readable.get_byob_reader().unwrap();
    let mut closed = reader.closed();
    reader.release_lock();
    assert!(poll(&mut closed).is_pending());
    ready(stream.writable.close()).unwrap();
    ready(closed).unwrap();
}

#[test]
fn cancellation_converts_reasons_but_abort_keeps_the_writer_reason_and_signal_identity() {
    for (input, message) in [
        (None, "Stream was cancelled."),
        (Some(Value::Null), "null"),
        (Some(Value::Boolean(false)), "false"),
        (Some(Value::Number(-0.0)), "0"),
        (Some(Value::Number(f64::NAN)), "NaN"),
    ] {
        let stream = IdentityTransformStream::new();
        let reader = stream.readable.get_reader().unwrap();
        let writer = stream.writable.get_writer().unwrap();
        let write = writer.write(vec![1]);
        let queued = writer.write(vec![2]);
        let close = writer.close();
        assert!(stream.readable.cancel(None).is_err());
        reader.cancel(input.map(reason)).unwrap();
        let error = ready(write).unwrap_err();
        assert_eq!(name(&error), "Error");
        assert_eq!(error.to_string(), message);
        assert!(ready(queued).unwrap_err().ptr_eq(&error));
        assert!(ready(close).unwrap_err().ptr_eq(&error));
        assert!(ready(writer.closed()).unwrap_err().ptr_eq(&error));
        ready(reader.closed()).unwrap();
        assert!(ready(reader.read()).unwrap().done);
        assert!(!stream.writable.signal().aborted());
    }
    let stream = IdentityTransformStream::new();
    let reader = stream.readable.get_byob_reader().unwrap();
    let pending = reader.read_into(vec![0; 2].into(), 1);
    reader.cancel(None).unwrap();
    let read = ready(pending).unwrap();
    assert!(read.done);
    assert!(read.value.is_none());
    let stream = IdentityTransformStream::new();
    let reader = stream.readable.get_reader().unwrap();
    let writer = stream.writable.get_writer().unwrap();
    let raw = reason(Value::Null);
    let pending = writer.write(vec![7]);
    let observable = stream.readable.clone();
    let listener = Listener::new(move |_, _| {
        assert!(observable.get_reader().is_err());
        Ok(())
    });
    stream
        .writable
        .signal()
        .add_event_listener(
            "abort",
            Some(&listener),
            ListenerOptions {
                once: true,
                ..Default::default()
            },
        )
        .unwrap();
    let abort = writer.abort(Some(raw.clone()));
    let again = writer.abort(Some(reason("ignored".into())));
    assert!(abort.ptr_eq(&again));
    ready(abort).unwrap();
    ready(again).unwrap();
    assert!(ready(pending).unwrap_err().ptr_eq(&raw));
    assert!(ready(writer.closed()).unwrap_err().ptr_eq(&raw));
    assert!(stream.writable.signal().reason().unwrap().ptr_eq(&raw));
    let converted = ready(reader.read()).unwrap_err();
    assert_eq!(name(&converted), "Error");
    assert_eq!(converted.to_string(), "null");
    assert!(reader.cancel(None).is_err());
    let stream = IdentityTransformStream::new();
    let reader = stream.readable.get_reader().unwrap();
    let writer = stream.writable.get_writer().unwrap();
    let exception = AbortReason::from(DomException::new("why", "AbortError"));
    ready(writer.abort(Some(exception.clone()))).unwrap();
    assert!(ready(reader.read()).unwrap_err().ptr_eq(&exception));
}

#[test]
fn fixed_length_counts_bytes_and_shares_length_failures_with_both_endpoints() {
    for (input, expected) in [
        (0.00001, 0),
        (-0.0, 0),
        (2.9, 2),
        (9_007_199_254_740_991.0, 9_007_199_254_740_991),
    ] {
        assert_eq!(
            FixedLengthStream::new(input)
                .unwrap()
                .readable
                .expected_length(),
            Some(expected)
        );
    }
    for input in [-1.0, f64::INFINITY, f64::NAN, 9_007_199_254_740_992.0] {
        assert!(FixedLengthStream::new(input).is_err());
    }
    let stream = FixedLengthStream::new(2.0).unwrap();
    let writer = stream.writable.get_writer().unwrap();
    let reader = stream.readable.get_reader().unwrap();
    let read = reader.read();
    let over = ready(writer.write(vec![1, 2, 3])).unwrap_err();
    assert_eq!(
        over.to_string(),
        "Attempt to write too many bytes through a FixedLengthStream."
    );
    assert!(ready(read).unwrap_err().ptr_eq(&over));
    assert!(ready(writer.closed()).unwrap_err().ptr_eq(&over));
    let stream = FixedLengthStream::new(2.0).unwrap();
    let writer = stream.writable.get_writer().unwrap();
    let reader = stream.readable.get_reader().unwrap();
    let write = writer.write(vec![1]);
    assert_eq!(ready(reader.read()).unwrap().value.unwrap().bytes(), [1]);
    ready(write).unwrap();
    let under = ready(writer.close()).unwrap_err();
    assert_eq!(
        under.to_string(),
        "FixedLengthStream did not see all expected bytes before close()."
    );
    assert!(ready(reader.read()).unwrap_err().ptr_eq(&under));
    assert!(ready(writer.closed()).unwrap_err().ptr_eq(&under));
    let stream = FixedLengthStream::new(0.0).unwrap();
    ready(stream.writable.close()).unwrap();
    assert!(
        ready(stream.readable.get_reader().unwrap().read())
            .unwrap()
            .done
    );
}

#[test]
fn waiter_drop_releases_wakers_and_a_dropped_read_does_not_steal_next_chunk() {
    let stream = IdentityTransformStream::new();
    let reader = stream.readable.get_reader().unwrap();
    let writer = stream.writable.get_writer().unwrap();
    let count = Arc::new(Counter::default());
    let weak = Arc::downgrade(&count);
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    let mut read = reader.read();
    for _ in 0..100 {
        assert!(Pin::new(&mut read).poll(&mut cx).is_pending());
    }
    assert_eq!(Arc::strong_count(&count), 3);
    drop(read);
    assert_eq!(Arc::strong_count(&count), 2);
    let mut closed = writer.closed();
    for _ in 0..100 {
        assert!(Pin::new(&mut closed).poll(&mut cx).is_pending());
    }
    assert_eq!(Arc::strong_count(&count), 3);
    drop(closed);
    assert_eq!(Arc::strong_count(&count), 2);
    drop(waker);
    drop(count);
    assert!(weak.upgrade().is_none());
    let write = writer.write(vec![4]);
    drop(write);
    assert_eq!(ready(reader.read()).unwrap().value.unwrap().bytes(), [4]);
    ready(writer.close()).unwrap();
    let abandoned = {
        let stream = IdentityTransformStream::new();
        let writer = stream.writable.get_writer().unwrap();
        writer.write(vec![1, 2, 3])
    };
    assert_eq!(
        ready(abandoned).unwrap_err().to_string(),
        "The byte stream was dropped"
    );
}
