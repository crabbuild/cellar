// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Async native content handlers over the shared interactive lol_html host.

use super::{http::Response, Context, HandlerFuture};
use crate::{engine_api::HttpChunkStream, html_rewriter as host};
use anyhow::{anyhow, Result};
use futures_util::{FutureExt, Stream, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, OnceLock,
    },
    task::{Context as TaskContext, Poll},
};

pub use lol_html::html_content::ContentType;

const DEAD: &str = concat!(
    "This content token is no longer valid. ",
    "Content tokens are only valid during the execution of the relevant content handler."
);
const ITER_INVALID: &str = concat!(
    "The attributes of this element have been modified during iteration. ",
    "You must create a new iterator after modifying attributes."
);

#[derive(Debug)]
pub struct Error {
    name: &'static str,
    message: String,
}

impl Error {
    pub fn name(&self) -> &'static str {
        self.name
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

fn type_error(message: impl Into<String>) -> anyhow::Error {
    Error {
        name: "TypeError",
        message: message.into(),
    }
    .into()
}

fn parser_error(message: impl Into<String>) -> anyhow::Error {
    Error {
        name: "Error",
        message: message.into(),
    }
    .into()
}

pub enum Content {
    Text(String),
    Stream(HttpChunkStream),
    Response(Response),
}

impl From<String> for Content {
    fn from(text: String) -> Self {
        Self::Text(text)
    }
}

impl From<&str> for Content {
    fn from(text: &str) -> Self {
        Self::Text(text.into())
    }
}

impl From<Response> for Content {
    fn from(response: Response) -> Self {
        Self::Response(response)
    }
}

impl From<HttpChunkStream> for Content {
    fn from(stream: HttpChunkStream) -> Self {
        Self::Stream(stream)
    }
}

struct Deferred {
    op: &'static str,
    source: Content,
    kind: ContentType,
}

type Callback<T> = Arc<dyn Fn(Context, T) -> HandlerFuture<()> + Send + Sync>;

struct TokenState {
    context: Context,
    id: u64,
    live: AtomicBool,
    attr_epoch: AtomicU64,
    deferred: Mutex<Vec<Deferred>>,
    end_tag: Mutex<Option<Callback<EndTag>>>,
}

impl TokenState {
    fn check(&self) -> Result<()> {
        if !self.live.load(Ordering::Acquire) {
            return Err(type_error(DEAD));
        }
        self.context.check_turn()
    }

    fn command(&self, command: Value) -> Result<Value> {
        self.check()?;
        let raw = host::command(self.id, command.to_string())
            .map_err(type_error)?
            .ok_or_else(|| type_error("HTMLRewriter command had no reply"))?;
        let reply: Value = serde_json::from_str(&raw)?;
        if let Some(message) = reply.get("error") {
            return Err(type_error(
                message.as_str().unwrap_or("HTMLRewriter command failed"),
            ));
        }
        Ok(reply["ok"].clone())
    }

    fn mutation(&self, op: &'static str, content: &str, kind: ContentType) -> Result<()> {
        self.command(json!({"op":op,"content":content,"html":matches!(kind,ContentType::Html)}))
            .map(|_| ())
    }

    fn content(&self, op: &'static str, content: Content, kind: ContentType) -> Result<()> {
        self.check()?;
        match content {
            Content::Text(text) => self.mutation(op, &text, kind),
            source => {
                self.deferred
                    .lock()
                    .unwrap()
                    .push(Deferred { op, source, kind });
                Ok(())
            }
        }
    }
}

struct TokenGuard(Arc<TokenState>);

impl Drop for TokenGuard {
    fn drop(&mut self) {
        self.0.live.store(false, Ordering::Release);
        self.0.deferred.lock().unwrap().clear();
        self.0.end_tag.lock().unwrap().take();
    }
}

#[derive(Clone)]
pub struct Element(Arc<TokenState>);

impl Element {
    pub fn tag_name(&self) -> Result<String> {
        Ok(serde_json::from_value(
            self.0.command(json!({"op":"tagName"}))?,
        )?)
    }

    pub fn set_tag_name(&self, name: &str) -> Result<()> {
        self.0
            .command(json!({"op":"setTagName","name":name}))
            .map(|_| ())
    }

    pub fn namespace_uri(&self) -> Result<String> {
        Ok(serde_json::from_value(
            self.0.command(json!({"op":"namespaceURI"}))?,
        )?)
    }

    pub fn removed(&self) -> Result<bool> {
        Ok(serde_json::from_value(
            self.0.command(json!({"op":"removed"}))?,
        )?)
    }

    pub fn get_attribute(&self, name: &str) -> Result<Option<String>> {
        Ok(serde_json::from_value(
            self.0.command(json!({"op":"getAttribute","name":name}))?,
        )?)
    }

    pub fn has_attribute(&self, name: &str) -> Result<bool> {
        Ok(serde_json::from_value(
            self.0.command(json!({"op":"hasAttribute","name":name}))?,
        )?)
    }

    pub fn set_attribute(&self, name: &str, value: &str) -> Result<()> {
        self.0
            .command(json!({"op":"setAttribute","name":name,"value":value}))?;
        self.0.attr_epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn remove_attribute(&self, name: &str) -> Result<()> {
        self.0
            .command(json!({"op":"removeAttribute","name":name}))?;
        self.0.attr_epoch.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub fn attributes(&self) -> Result<Attributes> {
        let entries: Vec<(String, String)> =
            serde_json::from_value(self.0.command(json!({"op":"attributes"}))?)?;
        Ok(Attributes {
            state: self.0.clone(),
            epoch: self.0.attr_epoch.load(Ordering::Acquire),
            entries: entries.into_iter(),
        })
    }

    pub fn before(&self, content: impl Into<Content>, kind: ContentType) -> Result<()> {
        self.0.content("before", content.into(), kind)
    }

    pub fn after(&self, content: impl Into<Content>, kind: ContentType) -> Result<()> {
        self.0.content("after", content.into(), kind)
    }

    pub fn prepend(&self, content: impl Into<Content>, kind: ContentType) -> Result<()> {
        self.0.content("prepend", content.into(), kind)
    }

    pub fn append(&self, content: impl Into<Content>, kind: ContentType) -> Result<()> {
        self.0.content("append", content.into(), kind)
    }

    pub fn replace(&self, content: impl Into<Content>, kind: ContentType) -> Result<()> {
        self.0.content("replace", content.into(), kind)
    }

    pub fn set_inner_content(&self, content: impl Into<Content>, kind: ContentType) -> Result<()> {
        self.0.content("setInnerContent", content.into(), kind)
    }

    pub fn remove(&self) -> Result<()> {
        self.0.command(json!({"op":"remove"})).map(|_| ())
    }

    pub fn remove_and_keep_content(&self) -> Result<()> {
        self.0
            .command(json!({"op":"removeAndKeepContent"}))
            .map(|_| ())
    }

    pub fn on_end_tag(
        &self,
        handler: impl Fn(Context, EndTag) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Result<()> {
        self.0.check()?;
        *self.0.end_tag.lock().unwrap() = Some(Arc::new(handler));
        Ok(())
    }
}

pub struct Attributes {
    state: Arc<TokenState>,
    epoch: u64,
    entries: std::vec::IntoIter<(String, String)>,
}

impl Attributes {
    pub fn next_pair(&mut self) -> Result<Option<(String, String)>> {
        self.state.check()?;
        if self.epoch != self.state.attr_epoch.load(Ordering::Acquire) {
            return Err(type_error(ITER_INVALID));
        }
        Ok(self.entries.next())
    }
}

impl Iterator for Attributes {
    type Item = Result<(String, String)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_pair().transpose()
    }
}

#[derive(Clone)]
pub struct EndTag {
    state: Arc<TokenState>,
    name: String,
}

impl EndTag {
    pub fn name(&self) -> Result<&str> {
        self.state.check()?;
        Ok(&self.name)
    }

    pub fn before(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("before", content, kind)
    }

    pub fn after(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("after", content, kind)
    }

    pub fn remove(&self) -> Result<()> {
        self.state.command(json!({"op":"remove"})).map(|_| ())
    }
}

#[derive(Clone)]
pub struct Comment {
    state: Arc<TokenState>,
    text: Arc<Mutex<String>>,
}

impl Comment {
    pub fn text(&self) -> Result<String> {
        self.state.check()?;
        Ok(self.text.lock().unwrap().clone())
    }

    pub fn set_text(&self, text: &str) -> Result<()> {
        self.state.command(json!({"op":"setText","text":text}))?;
        *self.text.lock().unwrap() = text.into();
        Ok(())
    }

    pub fn removed(&self) -> Result<bool> {
        Ok(serde_json::from_value(
            self.state.command(json!({"op":"removed"}))?,
        )?)
    }

    pub fn before(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("before", content, kind)
    }

    pub fn after(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("after", content, kind)
    }

    pub fn replace(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("replace", content, kind)
    }

    pub fn remove(&self) -> Result<()> {
        self.state.command(json!({"op":"remove"})).map(|_| ())
    }
}

#[derive(Clone)]
pub struct Text {
    state: Arc<TokenState>,
    text: String,
    last: bool,
}

impl Text {
    pub fn text(&self) -> Result<&str> {
        self.state.check()?;
        Ok(&self.text)
    }

    pub fn last_in_text_node(&self) -> Result<bool> {
        self.state.check()?;
        Ok(self.last)
    }

    pub fn removed(&self) -> Result<bool> {
        Ok(serde_json::from_value(
            self.state.command(json!({"op":"removed"}))?,
        )?)
    }

    pub fn before(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("before", content, kind)
    }

    pub fn after(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("after", content, kind)
    }

    pub fn replace(&self, content: &str, kind: ContentType) -> Result<()> {
        self.state.mutation("replace", content, kind)
    }

    pub fn remove(&self) -> Result<()> {
        self.state.command(json!({"op":"remove"})).map(|_| ())
    }
}

#[derive(Clone)]
pub struct Doctype {
    state: Arc<TokenState>,
    name: Option<String>,
    public_id: Option<String>,
    system_id: Option<String>,
}

impl Doctype {
    pub fn name(&self) -> Result<Option<&str>> {
        self.state.check()?;
        Ok(self.name.as_deref())
    }

    pub fn public_id(&self) -> Result<Option<&str>> {
        self.state.check()?;
        Ok(self.public_id.as_deref())
    }

    pub fn system_id(&self) -> Result<Option<&str>> {
        self.state.check()?;
        Ok(self.system_id.as_deref())
    }
}

#[derive(Clone)]
pub struct DocumentEnd(Arc<TokenState>);

impl DocumentEnd {
    pub fn append(&self, content: &str, kind: ContentType) -> Result<()> {
        self.0.mutation("append", content, kind)
    }
}

#[derive(Clone, Default)]
pub struct ElementHandlers {
    element: Option<Callback<Element>>,
    comments: Option<Callback<Comment>>,
    text: Option<Callback<Text>>,
}

impl ElementHandlers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn element(
        mut self,
        handler: impl Fn(Context, Element) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.element = Some(Arc::new(handler));
        self
    }

    pub fn comments(
        mut self,
        handler: impl Fn(Context, Comment) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.comments = Some(Arc::new(handler));
        self
    }

    pub fn text(
        mut self,
        handler: impl Fn(Context, Text) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.text = Some(Arc::new(handler));
        self
    }
}

#[derive(Clone, Default)]
pub struct DocumentHandlers {
    doctype: Option<Callback<Doctype>>,
    comments: Option<Callback<Comment>>,
    text: Option<Callback<Text>>,
    end: Option<Callback<DocumentEnd>>,
}

impl DocumentHandlers {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn doctype(
        mut self,
        handler: impl Fn(Context, Doctype) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.doctype = Some(Arc::new(handler));
        self
    }

    pub fn comments(
        mut self,
        handler: impl Fn(Context, Comment) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.comments = Some(Arc::new(handler));
        self
    }

    pub fn text(
        mut self,
        handler: impl Fn(Context, Text) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.text = Some(Arc::new(handler));
        self
    }

    pub fn end(
        mut self,
        handler: impl Fn(Context, DocumentEnd) -> HandlerFuture<()> + Send + Sync + 'static,
    ) -> Self {
        self.end = Some(Arc::new(handler));
        self
    }
}

#[derive(Clone, Default)]
pub struct HtmlRewriter {
    selectors: Vec<(String, ElementHandlers)>,
    document: Vec<DocumentHandlers>,
}

impl HtmlRewriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on(mut self, selector: impl Into<String>, handlers: ElementHandlers) -> Self {
        self.selectors.push((selector.into(), handlers));
        self
    }

    pub fn on_document(mut self, handlers: DocumentHandlers) -> Self {
        self.document.push(handlers);
        self
    }

    pub fn transform(&self, context: &Context, response: Response) -> Result<Response> {
        context.check_turn()?;
        let config = host::RewriterConfig {
            encoding: charset(&response.headers()),
            selectors: self
                .selectors
                .iter()
                .map(|(selector, handlers)| host::SelectorConfig {
                    selector: selector.clone(),
                    element: handlers.element.is_some(),
                    comments: handlers.comments.is_some(),
                    text: handlers.text.is_some(),
                })
                .collect(),
            document: self
                .document
                .iter()
                .map(|handlers| host::DocumentConfig {
                    doctype: handlers.doctype.is_some(),
                    comments: handlers.comments.is_some(),
                    text: handlers.text.is_some(),
                    end: handlers.end.is_some(),
                })
                .collect(),
        };
        let id = host::create(config).map_err(parser_error)?;
        let guard = ParserGuard(id);
        let (mut head, source, headers) = response.rewrite_parts(context)?;
        let Some(source) = source else {
            drop(guard);
            let mut response = Response::empty_from_http(head);
            if let Some(headers) = headers {
                response.set_header_list(headers);
            }
            return Ok(response);
        };
        let (writer, reader) = tokio::sync::mpsc::channel(1);
        let (cancel_sender, cancelled) = tokio::sync::oneshot::channel();
        let completed = Arc::new(AtomicBool::new(false));
        let output = OutputStream {
            reader,
            id,
            cancel: Some(cancel_sender),
            completed: completed.clone(),
            closed: false,
        };
        head.stream = Some(Box::pin(output));
        let rewriter = self.clone();
        let context = context.clone();
        let worker = context.clone();
        context.wait_until(async move {
            let result = std::panic::AssertUnwindSafe(
                rewriter.pump(worker, source, &writer, cancelled, guard),
            )
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| {
                Err(anyhow!(
                    "HTMLRewriter handler panicked: {}",
                    panic
                        .downcast_ref::<String>()
                        .map(String::as_str)
                        .or_else(|| panic.downcast_ref::<&str>().copied())
                        .unwrap_or("unknown panic")
                ))
            });
            if let Err(error) = result {
                let _ = writer.send(Err(error.to_string())).await;
            }
            completed.store(true, Ordering::Release);
            Ok(())
        })?;
        let mut response = Response::from_http(head);
        if let Some(headers) = headers {
            response.set_header_list(headers);
        }
        Ok(response)
    }
}

fn charset(headers: &[(String, String)]) -> Option<String> {
    static PATTERN: OnceLock<regex::Regex> = OnceLock::new();

    // Match ECMAScript whitespace and its ASCII-only /i folding, including
    // the refusal to fold a long s into the ASCII spelling of "charset".
    const SPACE: &str =
        r"\t-\r \u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{3000}\u{feff}";
    let pattern = PATTERN.get_or_init(|| {
        regex::Regex::new(&format!(r";[{SPACE}]*(?i-u:charset)=([^;{SPACE}]+)")).unwrap()
    });
    let content_type = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    pattern.captures(&content_type).map(|captures| {
        let label = captures[1].strip_prefix('"').unwrap_or(&captures[1]);
        label.strip_suffix('"').unwrap_or(label).to_string()
    })
}

struct ParserGuard(u64);

impl Drop for ParserGuard {
    fn drop(&mut self) {
        host::free(self.0);
    }
}

struct OutputStream {
    reader: tokio::sync::mpsc::Receiver<std::result::Result<Vec<u8>, String>>,
    id: u64,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
    completed: Arc<AtomicBool>,
    closed: bool,
}

impl Stream for OutputStream {
    type Item = std::result::Result<Vec<u8>, String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        match self.reader.poll_recv(cx) {
            Poll::Ready(None) => {
                self.closed = true;
                if self.completed.load(Ordering::Acquire) {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Err(
                        "HTMLRewriter turn retired before completion".into()
                    )))
                }
            }
            result => result,
        }
    }
}

impl Drop for OutputStream {
    fn drop(&mut self) {
        host::free(self.id);
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

impl HtmlRewriter {
    async fn service(
        &self,
        context: &Context,
        id: u64,
        event: Value,
        end_tags: &mut HashMap<u64, Callback<EndTag>>,
        next_end_tag: &mut u64,
    ) -> Result<()> {
        let state = Arc::new(TokenState {
            context: context.clone(),
            id,
            live: AtomicBool::new(true),
            attr_epoch: AtomicU64::new(0),
            deferred: Mutex::new(Vec::new()),
            end_tag: Mutex::new(None),
        });
        let guard = TokenGuard(state.clone());
        let handler = event["handler"].as_u64().unwrap_or(0) as usize;
        let doc = event["docHandler"].as_u64().unwrap_or(0) as usize;
        let document = event["document"].as_bool().unwrap_or(false);
        match event["kind"].as_str().unwrap_or("") {
            "element" => {
                if let Some(callback) = self.selectors[handler].1.element.as_ref() {
                    callback(context.clone(), Element(state.clone())).await?;
                }
            }
            "comment" => {
                let callback = if document {
                    self.document[doc].comments.as_ref()
                } else {
                    self.selectors[handler].1.comments.as_ref()
                };
                if let Some(callback) = callback {
                    callback(
                        context.clone(),
                        Comment {
                            state: state.clone(),
                            text: Arc::new(Mutex::new(event["text"].as_str().unwrap_or("").into())),
                        },
                    )
                    .await?;
                }
            }
            "text" => {
                let callback = if document {
                    self.document[doc].text.as_ref()
                } else {
                    self.selectors[handler].1.text.as_ref()
                };
                if let Some(callback) = callback {
                    callback(
                        context.clone(),
                        Text {
                            state: state.clone(),
                            text: event["text"].as_str().unwrap_or("").into(),
                            last: event["lastInTextNode"].as_bool().unwrap_or(false),
                        },
                    )
                    .await?;
                }
            }
            "doctype" => {
                if let Some(callback) = self.document[doc].doctype.as_ref() {
                    callback(
                        context.clone(),
                        Doctype {
                            state: state.clone(),
                            name: event["name"].as_str().map(str::to_string),
                            public_id: event["publicId"].as_str().map(str::to_string),
                            system_id: event["systemId"].as_str().map(str::to_string),
                        },
                    )
                    .await?;
                }
            }
            "documentEnd" => {
                if let Some(callback) = self.document[doc].end.as_ref() {
                    callback(context.clone(), DocumentEnd(state.clone())).await?;
                }
            }
            "endTag" => {
                if let Some(callback) = end_tags.remove(&event["token"].as_u64().unwrap_or(0)) {
                    callback(
                        context.clone(),
                        EndTag {
                            state: state.clone(),
                            name: event["name"].as_str().unwrap_or("").into(),
                        },
                    )
                    .await?;
                }
            }
            kind => return Err(anyhow!("unknown HTMLRewriter event {kind}")),
        }
        let deferred = std::mem::take(&mut *state.deferred.lock().unwrap());
        for deferred in deferred {
            let bytes = match deferred.source {
                Content::Response(mut response) => response.bytes(context).await?,
                Content::Stream(mut stream) => {
                    let mut bytes = Vec::new();
                    while let Some(chunk) = stream.next().await {
                        context.check_turn()?;
                        bytes.extend(chunk.map_err(anyhow::Error::msg)?);
                    }
                    bytes
                }
                Content::Text(text) => text.into_bytes(),
            };
            let mut decoder = cellar_native::encoding::TextDecoder::new(
                "utf-8",
                cellar_native::encoding::DecoderOptions {
                    fatal: true,
                    ..Default::default()
                },
            )?;
            let text = decoder
                .decode(&bytes, false)
                .map_err(|_| parser_error("Parser error: Invalid UTF-8"))?;
            state.mutation(deferred.op, &text, deferred.kind)?;
        }
        let mut done = json!({"op":"done"});
        if let Some(callback) = state.end_tag.lock().unwrap().take() {
            done["wantEndTag"] = json!(true);
            done["endTagToken"] = json!(*next_end_tag);
            end_tags.insert(*next_end_tag, callback);
            *next_end_tag += 1;
        }
        drop(guard);
        host::command(id, done.to_string()).map_err(type_error)?;
        Ok(())
    }

    async fn pump(
        self,
        context: Context,
        mut source: HttpChunkStream,
        writer: &tokio::sync::mpsc::Sender<std::result::Result<Vec<u8>, String>>,
        mut cancelled: tokio::sync::oneshot::Receiver<()>,
        guard: ParserGuard,
    ) -> Result<()> {
        let id = guard.0;
        let mut ack: Option<tokio::sync::oneshot::Receiver<std::result::Result<(), String>>> = None;
        let mut buffered = std::collections::VecDeque::new();
        let mut ended = false;
        let mut pending_read = false;
        let mut end_tags = HashMap::new();
        let mut next_end_tag = 1;
        loop {
            context.check_turn()?;
            if ack.is_none() && !buffered.is_empty() {
                let count = buffered.len().min(64 << 10);
                ack = Some(
                    host::write_ready(id, buffered.drain(..count).collect()).map_err(type_error)?,
                );
            }

            enum Next {
                Cancel,
                Event(std::result::Result<String, String>),
                Ack,
                Input(Option<std::result::Result<Vec<u8>, String>>),
            }
            let next = crate::asyncrt::select_biased! {"cancellation and parser events precede feeding the next input chunk";
                _ = &mut cancelled => Next::Cancel,
                event = host::event(id) => Next::Event(event),
                result = async { ack.as_mut().expect("guarded ack").await }, if ack.is_some() => {
                    result.map_err(|_| anyhow!("HTMLRewriter thread exited"))?.map_err(parser_error)?;
                    Next::Ack
                },
                input = std::future::poll_fn(|cx| {
                    let value = source.as_mut().poll_next(cx);
                    pending_read = value.is_pending();
                    value
                }), if ack.is_none() && buffered.is_empty() && !ended => Next::Input(input),
            };
            match next {
                Next::Cancel => {
                    drop(guard);
                    if pending_read {
                        let _ = source.next().await;
                    }
                    return Ok(());
                }
                Next::Ack => ack = None,
                Next::Input(Some(chunk)) => buffered.extend(chunk.map_err(anyhow::Error::msg)?),
                Next::Input(None) => {
                    ended = true;
                    host::end(id).map_err(type_error)?;
                }
                Next::Event(event) => {
                    let event: Value = serde_json::from_str(&event.map_err(anyhow::Error::msg)?)?;
                    match event["kind"].as_str().unwrap_or("") {
                        "output" | "end" => {
                            let bytes = host::take(id);
                            if !bytes.is_empty() {
                                let sent = crate::asyncrt::select_biased! {"a cancelled destination cannot release a held response chunk";
                                    _ = &mut cancelled => false,
                                    result = async {
                                        crate::native_worker::channel_gate(&context,cellar_logic::Channel::Response).await?;
                                        writer.send(Ok(bytes)).await.map_err(|_| anyhow!("HTMLRewriter destination cancelled"))
                                    } => { result?; true },
                                };
                                if !sent {
                                    drop(guard);
                                    if pending_read {
                                        let _ = source.next().await;
                                    }
                                    return Ok(());
                                }
                            }
                            if event["kind"] == "end" {
                                return Ok(());
                            }
                        }
                        "error" => {
                            return Err(parser_error(
                                event["message"].as_str().unwrap_or("HTMLRewriter failed"),
                            ))
                        }
                        "closed" => return Err(anyhow!("HTMLRewriter thread exited")),
                        _ => {
                            let serviced = crate::asyncrt::select_biased! {"destination cancellation releases a parked token even if its handler never wakes";
                                _ = &mut cancelled => None,
                                result = self.service(&context,id,event,&mut end_tags,&mut next_end_tag) => Some(result),
                            };
                            let Some(result) = serviced else {
                                drop(guard);
                                if pending_read {
                                    let _ = source.next().await;
                                }
                                return Ok(());
                            };
                            result?;
                        }
                    }
                }
            }
        }
    }
}
