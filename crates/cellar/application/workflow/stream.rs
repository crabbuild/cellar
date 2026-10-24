// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::cell::*;
use super::replay::Shared;
use super::*;
use crate::application::{
    streams::{ByteReadable, ByteReader, IdentityTransformStream},
    Storage,
};
use cellar_native::{clone::Object, web::AbortReason};
use std::future::Future;

const CHUNK_BYTES: usize = 64 << 10;

#[derive(Clone, Debug)]
pub struct StepSelector {
    pub name: Vec<u16>,
    pub count: u64,
}

impl StepSelector {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.encode_utf16().collect(),
            count: 1,
        }
    }
}

enum Source {
    Local(Storage),
    Remote(Instance),
}

/// A fresh reader over a checkpointed byte stream, bound to its originating event.
pub struct StreamOutput {
    source: Source,
    context: Context,
    generation: String,
    id: String,
    position: u64,
    chunks: u64,
    size: u64,
}

impl StreamOutput {
    pub(super) fn local(shared: &Shared, value: &StructuredValue) -> Result<Self> {
        Self::from_value(
            Source::Local(shared.storage.clone()),
            shared.context.clone(),
            shared.generation.clone(),
            value,
        )
    }

    fn from_value(
        source: Source,
        context: Context,
        generation: String,
        value: &StructuredValue,
    ) -> Result<Self> {
        let manifest = property(value, "__cellarWorkflowStream")?
            .ok_or_else(|| error("step did not return a byte stream"))?;
        Ok(Self {
            source,
            context,
            generation,
            id: text(&manifest, "id")?,
            position: 0,
            chunks: number(&manifest, "chunks")? as u64,
            size: number(&manifest, "size")? as u64,
        })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub async fn next(&mut self, context: &Context) -> Result<Option<Vec<u8>>> {
        context.check_turn()?;
        ensure!(
            Arc::ptr_eq(&context.io, &self.context.io),
            "WORKFLOW_ERROR: stream belongs to another invocation"
        );
        if self.position == self.chunks {
            return Ok(None);
        }
        let value = match &self.source {
            Source::Local(storage) => {
                ensure!(
                    text(&required(storage)?, "generation")? == self.generation,
                    "WORKFLOW_ERROR: stream generation retired"
                );
                read(storage, &chunk_key(&self.id, self.position))?
                    .ok_or_else(|| error("missing workflow stream chunk"))?
            }
            Source::Remote(instance) => instance
                .resource_call(
                    context,
                    "__wfStreamRead",
                    Some(data(
                        json!({"generation":self.generation,"id":self.id,"index":self.position}),
                    )?),
                )
                .await?,
        };
        let bytes = bytes(&value)?;
        self.position += 1;
        Ok(Some(bytes))
    }

    /// The producer remains attached to this event through HTTP delivery and drain.
    pub fn into_readable(mut self, context: &Context) -> Result<ByteReadable> {
        context.check_turn()?;
        ensure!(
            Arc::ptr_eq(&context.io, &self.context.io),
            "WORKFLOW_ERROR: stream belongs to another invocation"
        );
        let pipe = IdentityTransformStream::new();
        let readable = pipe.readable;
        let writer = pipe.writable.get_writer()?;
        let context_copy = context.clone();
        context.wait_until_byte_stream(&readable, async move {
            while let Some(bytes) = self.next(&context_copy).await? {
                writer.write(bytes).await?;
            }
            writer.close().await?;
            Ok(())
        })?;
        Ok(readable)
    }
}

fn chunk_key(id: &str, index: u64) -> String {
    format!("__wf.stream.{id}.chunk.{index:020}")
}

fn bytes(value: &StructuredValue) -> Result<Vec<u8>> {
    if let CloneValue::Reference(index) = value.root {
        if let Some(Object::ArrayBuffer { bytes, .. }) = value.objects.get(index as usize) {
            return Ok(bytes.clone());
        }
    }
    Err(error("invalid workflow stream chunk"))
}

fn value(bytes: Vec<u8>) -> StructuredValue {
    StructuredValue {
        root: CloneValue::Reference(0),
        objects: vec![Object::ArrayBuffer {
            bytes,
            max_length: None,
            immutable: true,
        }],
    }
}

struct ReaderGuard {
    reader: ByteReader,
    complete: bool,
}

impl Drop for ReaderGuard {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.reader.cancel(Some(AbortReason::error(
                "Workflow stream capture cancelled",
            )));
        }
    }
}

impl Step {
    pub fn run_stream<F, Fut>(
        &self,
        name: impl Into<StepName>,
        callback: F,
    ) -> crate::application::HandlerFuture<StreamOutput>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<ByteReadable>> + Send + 'static,
    {
        self.run_stream_with(name, StepConfig::default(), callback)
    }

    pub fn run_stream_with<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
    ) -> crate::application::HandlerFuture<StreamOutput>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<ByteReadable>> + Send + 'static,
    {
        self.run_stream_internal(name, config, callback, None)
    }

    pub fn run_stream_with_rollback<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
        rollback: RollbackOptions,
    ) -> crate::application::HandlerFuture<StreamOutput>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<ByteReadable>> + Send + 'static,
    {
        self.run_stream_internal(name, config, callback, Some(rollback))
    }

    fn run_stream_internal<F, Fut>(
        &self,
        name: impl Into<StepName>,
        config: StepConfig,
        callback: F,
        rollback: Option<RollbackOptions>,
    ) -> crate::application::HandlerFuture<StreamOutput>
    where
        F: FnOnce(Attempt) -> Fut + Send + 'static,
        Fut: Future<Output = Result<ByteReadable>> + Send + 'static,
    {
        let shared = self.0.clone();
        let capturing = shared.clone();
        let result = self.run_internal(
            name,
            config,
            move |attempt| async move {
                let attempt_number = attempt.attempt;
                let key = format!(
                    "{}step.{}.{}",
                    prefix(&capturing.generation),
                    attempt.count,
                    String::from_utf16_lossy(&attempt.name)
                );
                if let Some(record) = capturing.get(&key)? {
                    if let Some(pending) = property(&record, "pendingStream")? {
                        cleanup(
                            &capturing,
                            pending
                                .to_json()?
                                .as_str()
                                .ok_or_else(|| error("invalid pending stream"))?,
                        )
                        .await?;
                    }
                }
                let readable = callback(attempt).await?;
                if readable.locked() || readable.disturbed() {
                    return Err(
                        StepFailure::non_retryable("return a fresh unlocked byte stream").into(),
                    );
                }
                let mut reader = ReaderGuard {
                    reader: readable.get_reader()?,
                    complete: false,
                };
                let id = cellar_native::crypto::random_uuid()?;
                let mut record = capturing
                    .get(&key)?
                    .ok_or_else(|| error("missing stream step"))?;
                ensure!(
                    text(&record, "status")? == "running"
                        && number(&record, "lastAttempt")? == attempt_number as f64,
                    "WORKFLOW_ERROR: stream attempt retired"
                );
                put_field(&mut record, "pendingStream", json!(id))?;
                capturing.put(&key, &record)?;
                let mut chunks = 0_u64;
                let mut size = 0_u64;
                loop {
                    let item = reader.reader.read().await?;
                    if item.done {
                        reader.complete = true;
                        break;
                    }
                    let bytes = item
                        .value
                        .ok_or_else(|| error("stream read returned no bytes"))?
                        .into_bytes();
                    for chunk in bytes.chunks(CHUNK_BYTES) {
                        let length = chunk.len() as u64;
                        capturing.track(capturing.storage.transaction_sync(|transaction| {
                            let mut meta = required(transaction)?;
                            ensure!(
                                text(&meta, "generation")? == capturing.generation
                                    && !terminal(&meta)?,
                                "WORKFLOW_ERROR: stream generation retired"
                            );
                            let record = read(transaction, &key)?
                                .ok_or_else(|| error("missing stream step"))?;
                            ensure!(
                                text(&record, "status")? == "running"
                                    && number(&record, "lastAttempt")? == attempt_number as f64
                                    && text(&record, "pendingStream")? == id,
                                "WORKFLOW_ERROR: stream attempt retired"
                            );
                            let previous = property(&meta, "streamBytes")?
                                .map(|_| number(&meta, "streamBytes"))
                                .transpose()?
                                .unwrap_or(0.0) as u64;
                            let total = previous
                                .checked_add(length)
                                .ok_or_else(|| error("stream size overflow"))?;
                            if total > limits().max_stream_bytes as u64 {
                                return Err(StepFailure::non_retryable(
                                    "streamed state exceeds the host limit",
                                )
                                .into());
                            }
                            transaction
                                .put_clone(&chunk_key(&id, chunks), &value(chunk.to_vec()))?;
                            put_field(&mut meta, "streamBytes", json!(total))?;
                            transaction.put_clone(META, &meta)
                        }))?;
                        chunks += 1;
                        size = size
                            .checked_add(length)
                            .ok_or_else(|| error("stream size overflow"))?;
                        if chunks.is_multiple_of(16) {
                            crate::asyncrt::sleep(std::time::Duration::ZERO).await;
                        }
                    }
                }
                data(json!({"__cellarWorkflowStream":{"id":id,"chunks":chunks,"size":size}}))
            },
            rollback,
            true,
        );
        Box::pin(async move {
            let value = result.await?;
            StreamOutput::from_value(
                Source::Local(shared.storage.clone()),
                shared.context.clone(),
                shared.generation.clone(),
                &value,
            )
        })
    }
}

async fn cleanup(shared: &Shared, id: &str) -> Result<()> {
    loop {
        let entries = shared
            .storage
            .list_clone(&crate::application::ListOptions {
                prefix: Some(format!("__wf.stream.{id}.chunk.")),
                limit: Some(16),
                ..Default::default()
            })?;
        if entries.is_empty() {
            return Ok(());
        }
        let mut removed = 0_u64;
        for (_, v) in &entries {
            removed += bytes(&v.value)?.len() as u64;
        }
        shared.track(shared.storage.transaction_sync(|transaction| {
            let mut meta = required(transaction)?;
            ensure!(
                text(&meta, "generation")? == shared.generation,
                "WORKFLOW_ERROR: stream generation retired"
            );
            transaction.delete_many(
                &entries
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>(),
            )?;
            let previous = number(&meta, "streamBytes")? as u64;
            put_field(
                &mut meta,
                "streamBytes",
                json!(previous
                    .checked_sub(removed)
                    .ok_or_else(|| error("stream accounting corrupted"))?),
            )?;
            transaction.put_clone(META, &meta)
        }))?;
        crate::asyncrt::sleep(std::time::Duration::ZERO).await;
    }
}

impl Instance {
    pub async fn step_stream(
        &self,
        context: &Context,
        selector: StepSelector,
    ) -> Result<StreamOutput> {
        check_step_name(&selector.name)?;
        ensure!(
            selector.count > 0,
            "WORKFLOW_ERROR: invalid stream step occurrence"
        );
        let mut argument = data(json!({"count":selector.count}))?;
        set(
            &mut argument,
            "name",
            &StructuredValue {
                root: CloneValue::String(selector.name),
                objects: vec![],
            },
        )?;
        let reply = self.call(context, "__wfStreamOpen", Some(argument)).await?;
        let generation = text(&reply, "generation")?;
        StreamOutput::from_value(
            Source::Remote(self.clone()),
            context.clone(),
            generation,
            &reply,
        )
    }
}

pub(super) fn control(
    storage: &Storage,
    method: &str,
    argument: &StructuredValue,
) -> Result<StructuredValue> {
    let meta = required(storage)?;
    if method == "__wfStreamOpen" {
        let name = property(argument, "name")?.ok_or_else(|| error("missing stream step name"))?;
        let CloneValue::String(name) = name.root else {
            return Err(error("invalid stream step name"));
        };
        check_step_name(&name)?;
        let count = number(argument, "count")?;
        ensure!(
            count > 0.0 && count.fract() == 0.0,
            "WORKFLOW_ERROR: invalid stream occurrence"
        );
        let key = format!(
            "{}step.{}.{}",
            prefix(&text(&meta, "generation")?),
            count as u64,
            String::from_utf16_lossy(&name)
        );
        let record = read(storage, &key)?.ok_or_else(|| error("stream step does not exist"))?;
        ensure!(
            property(&record, "stream")?.is_some_and(|v| v.root == CloneValue::Boolean(true)),
            "WORKFLOW_ERROR: step did not return a byte stream"
        );
        ensure!(
            text(&record, "status")? == "completed",
            "WORKFLOW_ERROR: stream step has not completed"
        );
        ensure!(
            !property(&record, "sensitive")?.is_some_and(|v| v.root == CloneValue::Boolean(true)),
            "WORKFLOW_ERROR: sensitive stream output is redacted"
        );
        let mut output =
            property(&record, "value")?.ok_or_else(|| error("missing stream output"))?;
        put_field(&mut output, "generation", json!(text(&meta, "generation")?))?;
        Ok(output)
    } else {
        ensure!(
            text(argument, "generation")? == text(&meta, "generation")?,
            "WORKFLOW_ERROR: stream generation retired"
        );
        let id = text(argument, "id")?;
        ensure!(
            name_valid(&id, 36) && id.len() == 36,
            "WORKFLOW_ERROR: invalid stream id"
        );
        let index = number(argument, "index")?;
        ensure!(
            index >= 0.0 && index.fract() == 0.0 && index <= 9_007_199_254_740_991.0,
            "WORKFLOW_ERROR: invalid stream index"
        );
        read(storage, &chunk_key(&id, index as u64))?
            .ok_or_else(|| error("missing workflow stream chunk"))
    }
}
