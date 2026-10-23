use super::*;
use cellar::engine_api::{HttpResponse, HttpResponseWebSocket, WsTarget};
use futures_util::{FutureExt, Stream};
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context as TaskContext, Poll},
};

#[test]
fn nested_request_body_and_header_limits_have_exact_boundaries() {
    let mut request = json!({"url":"http://cell/", "method":"POST", "headers":[], "body":STANDARD.encode(vec![0; 4096])});
    assert!(input(&request, 4096).is_ok());
    request["body"] = json!(STANDARD.encode(vec![0; 4097]));
    assert!(input(&request, 4096)
        .err()
        .unwrap()
        .is::<RequestBodyLimit>());
    assert!(headers(&vec![("x".into(), "y".into()); 128]).is_ok());
    assert!(headers(&vec![("x".into(), "y".into()); 129]).is_err());
    assert!(headers(&[("x".into(), "y".repeat(32767))]).is_ok());
    assert!(headers(&[("x".into(), "y".repeat(32768))]).is_err());
}

struct ObservedStream {
    chunks: Option<VecDeque<Vec<u8>>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Stream for ObservedStream {
    type Item = Result<Vec<u8>, String>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        match &mut self.chunks {
            Some(chunks) => Poll::Ready(chunks.pop_front().map(Ok)),
            None => Poll::Pending,
        }
    }
}

impl Drop for ObservedStream {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn reply(
    body: Vec<u8>,
    chunks: Option<Vec<Vec<u8>>>,
) -> (HttpResponse, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let response = HttpResponse {
        status: 200,
        body,
        stream: Some(Box::pin(ObservedStream {
            chunks: chunks.map(Into::into),
            polls: polls.clone(),
            drops: drops.clone(),
        })),
        headers: vec![],
        websocket: None,
        write_position: None,
        observed_position: None,
    };
    (response, polls, drops)
}

#[test]
fn known_invalid_responses_refuse_without_polling_and_release_the_stream() {
    for binary in [false, true] {
        for invalid in ["prefix", "header-count", "header-bytes", "upgrade"] {
            // This stream never wakes. Known invalid metadata must not spend
            // the caller's deadline waiting for a body that cannot be accepted.
            let (mut response, polls, drops) = reply(Vec::new(), None);
            match invalid {
                "prefix" => response.body = vec![0; 5],
                "header-count" => response.headers = vec![("x".into(), "y".into()); 129],
                "header-bytes" => response.headers = vec![("x".into(), "y".repeat(32768))],
                "upgrade" => {
                    response.websocket = Some(HttpResponseWebSocket::Cell(WsTarget {
                        id: 1,
                        scope: "fixture".into(),
                        tunnel: None,
                    }))
                }
                _ => unreachable!(),
            }
            let result = async {
                if binary {
                    response_parts(response, 4).await.map(|_| ())
                } else {
                    super::response(response, 4).await.map(|_| ())
                }
            }
            .now_or_never()
            .expect("known invalid response waited on its stream");
            let error = result.expect_err("invalid response accepted");
            assert_eq!(polls.load(Ordering::Relaxed), 0, "{invalid}");
            assert_eq!(drops.load(Ordering::Relaxed), 1, "{invalid}");
            assert_eq!(
                crate::failure::envelope("cell.fetch", &error),
                json!({"ok":false,"error":"Ambiguous","code":"Ambiguous","outcome":"unknown"})
            );
        }
    }
}

#[test]
fn streamed_responses_count_the_buffered_prefix_and_stop_at_first_overflow() {
    let (response, polls, drops) = reply(vec![1, 2], Some(vec![vec![3, 4], vec![5], vec![6]]));
    assert!(response_parts(response, 4).now_or_never().unwrap().is_err());
    assert_eq!(
        polls.load(Ordering::Relaxed),
        2,
        "overflow consumed another chunk"
    );
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    for binary in [false, true] {
        let (response, polls, drops) = reply(vec![1, 2], Some(vec![vec![], vec![3, 4]]));
        let body = async {
            if binary {
                response_parts(response, 4).await.map(|(_, bytes)| bytes)
            } else {
                super::response(response, 4)
                    .await
                    .map(|wire| STANDARD.decode(wire["body"].as_str().unwrap()).unwrap())
            }
        }
        .now_or_never()
        .unwrap()
        .unwrap();
        assert_eq!(body, vec![1, 2, 3, 4]);
        assert_eq!(polls.load(Ordering::Relaxed), 3);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
