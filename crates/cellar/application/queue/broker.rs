use super::*;
use crate::application::storage::{SqlRow, SqlValue};
use crate::application::{decode_rpc, Cell, Invocation, Storage};
use crate::engine_api::{HttpResponse, RpcData};
use base64::Engine;
use cellar_logic::queue as policy;
use futures_util::TryStreamExt;
use std::sync::atomic::{AtomicBool, Ordering};

type Reply = std::result::Result<Value, String>;
struct GroupData {
    entries: Vec<Encoded>,
    calls: Vec<tokio::sync::oneshot::Sender<Reply>>,
    bytes: usize,
}
struct Group {
    data: Mutex<Option<GroupData>>,
    flush: tokio::sync::watch::Sender<bool>,
}
struct Broker {
    storage: Storage,
    queue: String,
    config: Arc<Config>,
    ready: AtomicBool,
    producer: Mutex<Option<Arc<Group>>>,
}

pub(crate) fn create_cell(context: &Context) -> Result<Arc<dyn Cell>> {
    let queue = context
        .identity()
        .and_then(|identity| identity.name.clone())
        .ok_or_else(|| error("Queue cell requires its resource name"))?;
    Ok(Arc::new(Broker {
        storage: context.storage()?,
        queue,
        config: context.queue_config.clone(),
        ready: AtomicBool::new(false),
        producer: Mutex::new(None),
    }))
}

fn text(row: &SqlRow, key: &str) -> Result<String> {
    match row.get(key) {
        Some(SqlValue::Text(value)) => Ok(value.clone()),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}
fn number(row: &SqlRow, key: &str) -> Result<i64> {
    match row.get(key) {
        Some(SqlValue::Integer(value)) => Ok(*value),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}
fn optional_number(row: &SqlRow, key: &str) -> Result<Option<i64>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => number(row, key).map(Some),
    }
}
fn optional_text(row: &SqlRow, key: &str) -> Result<Option<String>> {
    match row.get(key) {
        Some(SqlValue::Null) => Ok(None),
        _ => text(row, key).map(Some),
    }
}
fn blob(row: &SqlRow, key: &str) -> Result<Vec<u8>> {
    match row.get(key) {
        Some(SqlValue::Blob(value)) => Ok(value.clone()),
        _ => Err(error(format!("invalid stored {key}"))),
    }
}
fn sql_text(value: Option<String>) -> SqlValue {
    value.map(SqlValue::Text).unwrap_or(SqlValue::Null)
}
fn uuid(version: u8, time: Option<(u64, u16)>) -> Result<String> {
    let mut bytes = [0; 16];
    cellar_native::crypto::random_values(&mut bytes)?;
    if let Some((ms, sequence)) = time {
        bytes[..6].copy_from_slice(&ms.to_be_bytes()[2..]);
        bytes[6] = (sequence >> 8) as u8;
        bytes[7] = sequence as u8;
    }
    bytes[6] = (bytes[6] & 15) | (version << 4);
    bytes[8] = (bytes[8] & 63) | 128;
    let hex = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}
fn message_id(at: i64) -> Result<String> {
    static LAST: Mutex<(u64, u16)> = Mutex::new((0, 0));
    let mut last = LAST.lock().unwrap();
    if at as u64 > last.0 {
        *last = (at as u64, 0);
    } else if last.1 == 0xfff {
        *last = (last.0 + 1, 0);
    } else {
        last.1 += 1;
    }
    uuid(7, Some(*last))
}

impl Broker {
    fn exec(&self, query: &str, params: &[SqlValue]) -> Result<Vec<SqlRow>> {
        self.storage.system_sql(query, params)
    }
    fn open(&self) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            self.storage.system_sql_schema(include_str!("schema.sql"))?;
            self.ready.store(true, Ordering::Release);
        }
        Ok(())
    }
    fn meta(&self, key: &str) -> Result<Option<String>> {
        self.open()?;
        self.exec(
            "SELECT value FROM __sys_queue_meta WHERE name=?",
            &[key.into()],
        )?
        .first()
        .map(|row| text(row, "value"))
        .transpose()
    }
    fn set_meta(&self, key: &str, value: Option<String>) -> Result<()> {
        self.open()?;
        match value {
            Some(value) => {
                self.exec("INSERT INTO __sys_queue_meta(name,value) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value",&[key.into(),value.into()])?;
            }
            None => {
                self.exec("DELETE FROM __sys_queue_meta WHERE name=?", &[key.into()])?;
            }
        }
        Ok(())
    }
    fn paused(&self) -> Result<bool> {
        Ok(self.meta("paused")?.as_deref() == Some("1"))
    }
    fn consumer(&self) -> Option<Consumer> {
        self.config.consumers.get(&self.queue).cloned()
    }
    fn metrics(&self, at: i64, waiting: bool) -> Result<Value> {
        self.open()?;
        let cutoff = at.saturating_sub(self.config.limits.retention_ms);
        let stats=self.exec("SELECT backlog_count,backlog_bytes,oldest_message_timestamp FROM __sys_queue_stats WHERE id=1",&[])?.remove(0);
        let oldest = optional_number(&stats, "oldest_message_timestamp")?;
        let (count, bytes, oldest) = if oldest.is_some_and(|oldest| oldest <= cutoff) {
            let row=self.exec("SELECT COUNT(*) AS backlog_count,COALESCE(SUM(size),0) AS backlog_bytes,MIN(enqueued_at) AS oldest_message_timestamp FROM __sys_queue_messages WHERE purge_on_settle=0 AND enqueued_at>? AND (?=0 OR (lease_id IS NULL AND dlq_target IS NULL AND visible_at<=?))",&[cutoff.into(),i64::from(waiting).into(),at.into()])?.remove(0);
            (
                number(&row, "backlog_count")?,
                number(&row, "backlog_bytes")?,
                optional_number(&row, "oldest_message_timestamp")?,
            )
        } else if waiting {
            let blocked=self.exec("SELECT COALESCE(SUM(blocked_count),0) AS blocked_count,COALESCE(SUM(blocked_bytes),0) AS blocked_bytes FROM (SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_lease WHERE purge_on_settle=0 AND lease_id IS NOT NULL UNION ALL SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_moving WHERE purge_on_settle=0 AND dlq_target IS NOT NULL UNION ALL SELECT COUNT(*) AS blocked_count,COALESCE(SUM(size),0) AS blocked_bytes FROM __sys_queue_messages INDEXED BY __sys_queue_messages_ready WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at>?)",&[at.into()])?.remove(0);
            let oldest=self.exec("SELECT enqueued_at FROM __sys_queue_messages INDEXED BY __sys_queue_messages_retained WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at<=? ORDER BY enqueued_at,seq LIMIT 1",&[at.into()])?.first().map(|row|number(row,"enqueued_at")).transpose()?;
            (
                number(&stats, "backlog_count")? - number(&blocked, "blocked_count")?,
                number(&stats, "backlog_bytes")? - number(&blocked, "blocked_bytes")?,
                oldest,
            )
        } else {
            (
                number(&stats, "backlog_count")?,
                number(&stats, "backlog_bytes")?,
                oldest,
            )
        };
        Ok(json!({"backlogCount":count,"backlogBytes":bytes,"oldestMessageTimestamp":oldest}))
    }
    async fn rearm(&self, context: &Context, at: i64, wait: bool) -> Result<()> {
        self.open()?;
        let paused = self.paused()?;
        let consumer = self.consumer();
        let active=number(&self.exec("SELECT COUNT(DISTINCT lease_id) AS count FROM __sys_queue_messages INDEXED BY __sys_queue_messages_lease WHERE lease_id IS NOT NULL AND leased_until>?",&[at.into()])?.remove(0),"count")? as usize;
        let capacity = !paused
            && consumer.as_ref().is_some_and(|consumer| {
                policy::can_install_lease(
                    active,
                    consumer
                        .max_concurrency
                        .unwrap_or(self.config.limits.max_concurrency),
                )
            });
        let mut ready = Vec::new();
        if capacity {
            let max = consumer.as_ref().unwrap().max_batch_size as i64;
            ready.extend(self.exec("SELECT visible_at AS ready_at,seq FROM __sys_queue_messages INDEXED BY __sys_queue_messages_ready WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at<=? ORDER BY visible_at,seq LIMIT ?",&[at.into(),max.into()])?);
            ready.extend(self.exec("SELECT leased_until AS ready_at,seq FROM __sys_queue_messages INDEXED BY __sys_queue_messages_lease WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NOT NULL AND leased_until<=? ORDER BY leased_until,seq LIMIT ?",&[at.into(),max.into()])?);
            ready.sort_by_key(|row| {
                (
                    number(row, "ready_at").unwrap_or(i64::MAX),
                    number(row, "seq").unwrap_or(i64::MAX),
                )
            });
            ready.truncate(max as usize);
        }
        let visible = if capacity {
            self.exec("SELECT visible_at FROM __sys_queue_messages INDEXED BY __sys_queue_messages_ready WHERE purge_on_settle=0 AND dlq_target IS NULL AND lease_id IS NULL AND visible_at>? ORDER BY visible_at,seq LIMIT 1",&[at.into()])?.first().map(|row|number(row,"visible_at")).transpose()?
        } else {
            None
        };
        let lease = if paused {
            None
        } else {
            self.exec("SELECT leased_until FROM __sys_queue_messages INDEXED BY __sys_queue_messages_lease WHERE lease_id IS NOT NULL ORDER BY leased_until LIMIT 1",&[])?.first().map(|row|number(row,"leased_until")).transpose()?
        };
        let sweep=self.exec("SELECT enqueued_at+? AS next_sweep FROM __sys_queue_messages INDEXED BY __sys_queue_messages_retained WHERE dlq_target IS NULL AND purge_on_settle=0 ORDER BY enqueued_at,seq LIMIT 1",&[self.config.limits.retention_ms.into()])?.first().map(|row|number(row,"next_sweep")).transpose()?;
        let receipts=self.exec("SELECT expires_at AS next_sweep FROM __sys_queue_transfer_receipts ORDER BY expires_at,transfer_id LIMIT 1",&[])?.first().map(|row|number(row,"next_sweep")).transpose()?;
        let pending=!self.exec("SELECT 1 FROM __sys_queue_messages INDEXED BY __sys_queue_messages_moving WHERE dlq_target IS NOT NULL LIMIT 1",&[])?.is_empty();
        let mut batch = if ready.is_empty() {
            None
        } else {
            let consumer = consumer.unwrap();
            Some(if ready.len() >= consumer.max_batch_size {
                at
            } else {
                number(&ready[0], "ready_at")?.saturating_add(consumer.max_batch_timeout_ms)
            })
        };
        if pending {
            batch = Some(batch.unwrap_or(at).min(at));
        }
        let sweep = [sweep, receipts].into_iter().flatten().min();
        match policy::rearm(at, batch, visible, lease, sweep) {
            None => self.storage.delete_alarm(),
            Some(deadline) if wait => {
                crate::native_worker::queue_alarm_wait(context, deadline).await
            }
            Some(deadline) => self.storage.set_alarm(deadline),
        }
    }

    fn enqueue(
        self: &Arc<Self>,
        context: &Context,
        entries: Vec<Encoded>,
    ) -> Result<tokio::sync::oneshot::Receiver<Reply>> {
        context.check_turn()?;
        let bytes = entries.iter().map(|entry| entry.body.len()).sum::<usize>();
        let (reply, receive) = tokio::sync::oneshot::channel();
        let mut pending = self.producer.lock().unwrap();
        if pending.as_ref().is_some_and(|group| {
            let data = group.data.lock().unwrap();
            data.as_ref().is_none_or(|data| {
                data.calls.len() + 1 > 64
                    || data.entries.len() + entries.len() > 100
                    || data.bytes + bytes > 256_000
            })
        }) {
            if let Some(group) = pending.take() {
                let _ = group.flush.send(true);
            }
        }
        let group = match pending.as_ref() {
            Some(group) => group.clone(),
            None => {
                let (flush, mut signal) = tokio::sync::watch::channel(false);
                let group = Arc::new(Group {
                    data: Mutex::new(Some(GroupData {
                        entries: Vec::new(),
                        calls: Vec::new(),
                        bytes: 0,
                    })),
                    flush,
                });
                *pending = Some(group.clone());
                let broker = self.clone();
                let flushed = group.clone();
                let context_copy = context.clone();
                let ms = self.config.limits.producer_group_ms;
                context.wait_until(async move {
                crate::asyncrt::select_biased!{"a full producer group flushes before its owner-local timer";_=signal.changed()=>{},_=crate::asyncrt::sleep(std::time::Duration::from_millis(ms))=>{},}
                broker.flush(&context_copy,flushed).await;Ok(())
            })?;
                group
            }
        };
        let mut data = group.data.lock().unwrap();
        let data = data.as_mut().expect("pending group is not flushing");
        data.entries.extend(entries);
        data.calls.push(reply);
        data.bytes += bytes;
        if data.calls.len() == 64 || data.entries.len() == 100 || data.bytes == 256_000 {
            pending.take();
            let _ = group.flush.send(true);
        }
        Ok(receive)
    }
    async fn flush(self: &Arc<Self>, context: &Context, group: Arc<Group>) {
        let Some(data) = group.data.lock().unwrap().take() else {
            return;
        };
        {
            let mut pending = self.producer.lock().unwrap();
            if pending
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(pending, &group))
            {
                pending.take();
            }
        }
        let at = now();
        let result=async {
            self.open()?;self.storage.transaction_sync(|_| {for entry in &data.entries {self.exec("INSERT INTO __sys_queue_messages(id,body,content_type,size,enqueued_at,visible_at) VALUES (?,?,?,?,?,?)",&[message_id(at)?.into(),SqlValue::Blob(entry.body.clone()),entry.content_type.clone().into(),(entry.body.len() as i64).into(),at.into(),at.saturating_add(i64::from(entry.delay)*1000).into()])?;}Ok(())})?;
            self.rearm(context,at,true).await?;self.metrics(at,false)
        }.await.map_err(|error:anyhow::Error|error.to_string());
        for reply in data.calls {
            let _ = reply.send(result.clone());
        }
    }

    fn sweep(&self, at: i64) -> Result<usize> {
        self.open()?;
        let cutoff = at.saturating_sub(self.config.limits.retention_ms);
        let limit = self.config.limits.sweep_batch_rows;
        let messages=self.storage.transaction_sync(|_| {let rows=self.exec("SELECT seq,lease_id,leased_until FROM __sys_queue_messages WHERE enqueued_at<=? AND (purge_on_settle=0 OR lease_id IS NULL OR leased_until<=?) ORDER BY enqueued_at,seq LIMIT ?",&[cutoff.into(),at.into(),(limit as i64).into()])?;for row in &rows {let seq=number(row,"seq")?;if optional_text(row,"lease_id")?.is_some() && optional_number(row,"leased_until")?.is_some_and(|until|until>at) {self.exec("UPDATE __sys_queue_messages SET purge_on_settle=1 WHERE seq=?",&[seq.into()])?;}else{self.exec("DELETE FROM __sys_queue_messages WHERE seq=?",&[seq.into()])?;}}Ok(rows.len())})?;
        let remaining = limit - messages;
        if remaining == 0 {
            return Ok(messages);
        }
        let receipts=self.storage.transaction_sync(|_| {let rows=self.exec("SELECT transfer_id FROM __sys_queue_transfer_receipts WHERE expires_at<=? ORDER BY expires_at,transfer_id LIMIT ?",&[at.into(),(remaining as i64).into()])?;for row in &rows {self.exec("DELETE FROM __sys_queue_transfer_receipts WHERE transfer_id=?",&[text(row,"transfer_id")?.into()])?;}Ok(rows.len())})?;
        Ok(messages + receipts)
    }
    fn candidates(&self, at: i64, limit: usize) -> Result<Vec<SqlRow>> {
        self.exec("SELECT seq,id,body,content_type,size,enqueued_at,visible_at,attempts,lease_id,lease_generation,leased_until,purge_on_settle FROM __sys_queue_messages WHERE dlq_target IS NULL AND ((purge_on_settle=1 AND (lease_id IS NULL OR leased_until<=?)) OR (purge_on_settle=0 AND visible_at<=? AND (lease_id IS NULL OR leased_until<=?))) ORDER BY visible_at,seq LIMIT ?",&[at.into(),at.into(),at.into(),(limit as i64).into()])
    }
    fn lease_batch(&self, at: i64, consumer: &Consumer) -> Result<Option<Value>> {
        let active=number(&self.exec("SELECT COUNT(DISTINCT lease_id) AS count FROM __sys_queue_messages WHERE lease_id IS NOT NULL AND leased_until>?",&[at.into()])?.remove(0),"count")? as usize;
        if !policy::can_install_lease(
            active,
            consumer
                .max_concurrency
                .unwrap_or(self.config.limits.max_concurrency),
        ) {
            return Ok(None);
        }
        let rows = self.candidates(at, consumer.max_batch_size + 256)?;
        let deliverable = rows
            .iter()
            .filter(|row| number(row, "purge_on_settle").ok() == Some(0))
            .collect::<Vec<_>>();
        if deliverable.is_empty() {
            self.storage.transaction_sync(|_| {
                for row in &rows {
                    self.exec(
                        "DELETE FROM __sys_queue_messages WHERE seq=?",
                        &[number(row, "seq")?.into()],
                    )?;
                }
                Ok(())
            })?;
            return Ok(None);
        }
        let ready = deliverable
            .iter()
            .map(|row| {
                if optional_text(row, "lease_id")?.is_none() {
                    number(row, "visible_at")
                } else {
                    number(row, "leased_until")
                }
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .min()
            .unwrap();
        if deliverable.len() < consumer.max_batch_size
            && ready.saturating_add(consumer.max_batch_timeout_ms) > at
        {
            return Ok(None);
        }
        let facts = rows
            .iter()
            .map(|row| {
                Ok(policy::BatchRow {
                    seq: number(row, "seq")?,
                    visible_at: number(row, "visible_at")?,
                    lease_generation: u64::try_from(number(row, "lease_generation")?)?,
                    leased_until: optional_number(row, "leased_until")?,
                    purge_on_settle: number(row, "purge_on_settle")? != 0,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let plan = policy::batch_plan(at, &facts, consumer.max_batch_size)?;
        let by_seq = rows
            .iter()
            .map(|row| Ok((number(row, "seq")?, row)))
            .collect::<Result<HashMap<_, _>>>()?;
        let mut expiry = HashMap::new();
        let mut transfers = HashMap::new();
        for lease in plan.leases.iter().filter(|lease| lease.reclaimed) {
            let row = by_seq[&lease.seq];
            let decision = policy::expire_lease(
                at,
                u16::try_from(number(row, "attempts")?)?,
                consumer.max_retries,
                consumer.retry_delay_seconds,
                number(row, "purge_on_settle")? != 0,
            );
            if decision.action == policy::ExpiredLeaseAction::Exhausted
                && consumer.dead_letter_queue.is_some()
            {
                transfers.insert(lease.seq, uuid(4, None)?);
            }
            expiry.insert(lease.seq, decision);
        }
        let installs=plan.leases.iter().filter(|lease|expiry.get(&lease.seq).is_none_or(|expiry|matches!(expiry.action,policy::ExpiredLeaseAction::RetryAt(deadline) if deadline<=at))).collect::<Vec<_>>();
        let lease_id = uuid(4, None)?;
        let leased_until = at
            .saturating_add(self.config.lease_duration_ms)
            .min(9_007_199_254_740_991);
        self.storage.transaction_sync(|_| {
            for seq in &plan.delete_purged {self.exec("DELETE FROM __sys_queue_messages WHERE seq=? AND purge_on_settle=1 AND (lease_id IS NULL OR leased_until<=?)",&[(*seq).into(),at.into()])?;}
            for (seq,expiry) in &expiry {match expiry.action {
                policy::ExpiredLeaseAction::DeletePurged=>{self.exec("DELETE FROM __sys_queue_messages WHERE seq=? AND leased_until<=?",&[(*seq).into(),at.into()])?;},
                policy::ExpiredLeaseAction::Exhausted=>{if let Some(target)=&consumer.dead_letter_queue {self.exec("UPDATE __sys_queue_messages SET attempts=?,lease_id=NULL,leased_until=NULL,dlq_target=?,dlq_transfer_id=?,transfer_kind='dead-letter' WHERE seq=? AND leased_until<=?",&[i64::from(expiry.attempt).into(),target.clone().into(),transfers[seq].clone().into(),(*seq).into(),at.into()])?;}else{self.exec("DELETE FROM __sys_queue_messages WHERE seq=? AND leased_until<=?",&[(*seq).into(),at.into()])?;}},
                policy::ExpiredLeaseAction::RetryAt(deadline) if deadline>at=>{self.exec("UPDATE __sys_queue_messages SET attempts=?,visible_at=?,lease_id=NULL,leased_until=NULL WHERE seq=? AND leased_until<=?",&[i64::from(expiry.attempt).into(),deadline.into(),(*seq).into(),at.into()])?;},_=>{},
            }}
            for lease in &installs {ensure!(lease.generation<=9_007_199_254_740_991,"QUEUE_ERROR: message {} exhausted its safe lease generation",lease.seq);let generation=lease.generation as i64;self.exec("UPDATE __sys_queue_messages SET lease_id=?,lease_generation=?,leased_until=?,attempts=attempts+? WHERE seq=? AND lease_generation=? AND (lease_id IS NULL OR leased_until<=?)",&[lease_id.clone().into(),generation.into(),leased_until.into(),i64::from(lease.reclaimed).into(),lease.seq.into(),(generation-1).into(),at.into()])?;}Ok(())
        })?;
        if installs.is_empty() {
            return Ok(None);
        }
        let leased=self.exec("SELECT seq,id,body,content_type,enqueued_at,attempts,lease_generation FROM __sys_queue_messages WHERE lease_id=? ORDER BY visible_at,seq",&[lease_id.clone().into()])?;
        ensure!(
            leased.len() == installs.len(),
            "QUEUE_ERROR: the Queue lease transaction changed fewer rows than its plan"
        );
        let leases=leased.iter().map(|row|Ok(json!({"message_id":text(row,"id")?,"seq":number(row,"seq")?,"generation":number(row,"lease_generation")?}))).collect::<Result<Vec<_>>>()?;
        let messages=leased.iter().map(|row|Ok(json!({"id":text(row,"id")?,"timestampMs":number(row,"enqueued_at")?,"bodyBase64":base64::engine::general_purpose::STANDARD.encode(blob(row,"body")?),"contentType":text(row,"content_type")?,"attempts":number(row,"attempts")?+1}))).collect::<Result<Vec<_>>>()?;
        Ok(Some(
            json!({"leaseId":lease_id,"leases":leases,"messages":messages,"metrics":self.metrics(at,true)?}),
        ))
    }

    async fn accept(
        &self,
        context: &Context,
        graph: &StructuredValue,
        body: &CloneValue,
    ) -> Result<Value> {
        let transfers = wire::array(graph, wire::field(graph, body, "transfers"))?;
        ensure!(
            !transfers.is_empty() && transfers.len() <= policy::MAX_BATCH_MESSAGES,
            "QUEUE_ERROR: a Queue transfer batch has an invalid size"
        );
        let at = now();
        self.open()?;
        self.storage.transaction_sync(|_| {for transfer in &transfers {
            let field=|name|wire::field(graph,transfer,name);let transfer_id=wire::string(field("transferId"))?;ensure!(matches!(field("deadLetterSource"),CloneValue::Null|CloneValue::String(_)),"QUEUE_ERROR: a Queue transfer has an invalid dead-letter source");let source=wire::optional_string(field("deadLetterSource"))?;let entry=field("entry");let field=|name|wire::field(graph,entry,name);let id=wire::string(field("id"))?;
            if let Some(receipt)=self.exec("SELECT message_id,dead_letter_source FROM __sys_queue_transfer_receipts WHERE transfer_id=?",&[transfer_id.clone().into()])?.first() {ensure!(text(receipt,"message_id")?==id && optional_text(receipt,"dead_letter_source")?==source,"QUEUE_ERROR: a DLQ transfer collided with another message identity");continue;}
            self.exec("INSERT INTO __sys_queue_transfer_receipts(transfer_id,message_id,dead_letter_source,expires_at) VALUES (?,?,?,?)",&[transfer_id.clone().into(),id.clone().into(),sql_text(source.clone()),at.saturating_add(self.config.limits.retention_ms).into()])?;
            self.exec("INSERT INTO __sys_queue_messages(id,body,content_type,size,enqueued_at,visible_at,attempts,dlq_transfer_id,dead_letter_source) VALUES (?,?,?,?,?,?,0,?,?)",&[id.into(),SqlValue::Blob(wire::bytes(graph,field("body"))?),wire::string(field("contentType"))?.into(),wire::number(field("size"))?.into(),at.into(),at.into(),transfer_id.into(),sql_text(source)])?;
        }Ok(())})?;
        self.rearm(context, at, false).await?;
        self.metrics(at, false)
    }
    async fn drive_dlq(&self, context: &Context, limit: usize) -> Result<usize> {
        self.open()?;
        let pending=self.exec("SELECT seq,id,body,content_type,size,dlq_target,dlq_transfer_id,transfer_kind FROM __sys_queue_messages WHERE dlq_target IS NOT NULL AND enqueued_at>? ORDER BY seq LIMIT ?",&[now().saturating_sub(self.config.limits.retention_ms).into(),(limit as i64).into()])?;
        let mut groups = IndexMap::<String, Vec<&SqlRow>>::new();
        for row in &pending {
            groups
                .entry(text(row, "dlq_target")?)
                .or_default()
                .push(row);
        }
        for (target, rows) in groups {
            let transfers=rows.iter().map(|row|Ok(json!({"transferId":text(row,"dlq_transfer_id")?,"deadLetterSource":if text(row,"transfer_kind")?=="dead-letter"{Some(self.queue.clone())}else{None},"entry":{"id":text(row,"id")?,"body":null,"contentType":text(row,"content_type")?,"size":number(row,"size")?}}))).collect::<Result<Vec<_>>>()?;
            let mut graph = StructuredValue::from_json(json!([{ "transfers":transfers }]))?;
            let body = wire::array(&graph, &graph.root)?[0].clone();
            let objects = wire::array(&graph, wire::field(&graph, &body, "transfers"))?;
            for (object, row) in objects.iter().zip(&rows) {
                let entry = wire::field(&graph, object, "entry").clone();
                wire::set_buffer(&mut graph, &entry, "body", blob(row, "body")?)?;
            }
            let result = context
                .rpc_cell(
                    crate::engine_api::queue_cell_scope(&target),
                    Some(target.clone()),
                    "__queueDlqAcceptBatch".into(),
                    encode_rpc(&graph)?,
                )
                .await?;
            decode_rpc_result(&result)?;
            self.storage.transaction_sync(|_| {for row in &rows {self.exec("DELETE FROM __sys_queue_messages WHERE seq=? AND dlq_target=? AND dlq_transfer_id=? AND transfer_kind=?",&[number(row,"seq")?.into(),text(row,"dlq_target")?.into(),text(row,"dlq_transfer_id")?.into(),text(row,"transfer_kind")?.into()])?;}Ok(())})?;
        }
        Ok(pending.len())
    }
    async fn alarm(&self, context: &Context) -> Result<()> {
        let at = now();
        if let Err(error) = self.drive_dlq(context, 100).await {
            tracing::error!(%error,"Queue DLQ transfer failed");
        }
        let swept = self.sweep(at)?;
        let mut batches = Vec::new();
        if !self.paused()? {
            if let Some(consumer) = self.consumer() {
                while let Some(batch) = self.lease_batch(at, &consumer)? {
                    batches.push(batch);
                }
            }
        }
        if let Err(error) = self.drive_dlq(context, 100).await {
            tracing::error!(%error,"Queue DLQ transfer failed");
        }
        self.rearm(context, at, false).await?;
        if swept >= self.config.limits.sweep_batch_rows && batches.is_empty() {
            return self.storage.set_alarm(at.saturating_add(1));
        }
        if let Some(consumer) = self.consumer() {
            for batch in batches {
                crate::native_worker::queue_dispatch(
                    context,
                    consumer.script.clone(),
                    self.queue.clone(),
                    batch,
                )
                .await?;
            }
        }
        Ok(())
    }

    fn matching(&self, body: &Value) -> Result<Option<Vec<SqlRow>>> {
        let Some(id) = body["leaseId"].as_str() else {
            return Ok(None);
        };
        let Some(leases) = body["leases"].as_array() else {
            return Ok(None);
        };
        let rows=self.exec("SELECT seq,id,attempts,lease_generation,purge_on_settle FROM __sys_queue_messages WHERE lease_id=? ORDER BY seq",&[id.into()])?;
        let submitted = leases
            .iter()
            .map(|lease| {
                let seq = lease["seq"].as_i64()?;
                let generation = lease["generation"].as_u64()?;
                if seq.unsigned_abs() > 9_007_199_254_740_991 || generation > 9_007_199_254_740_991
                {
                    return None;
                }
                Some(policy::LeaseMember {
                    seq,
                    message_id: lease["message_id"].as_str()?,
                    generation,
                })
            })
            .collect::<Option<Vec<_>>>();
        let Some(submitted) = submitted else {
            return Ok(None);
        };
        let names = rows
            .iter()
            .map(|row| text(row, "id"))
            .collect::<Result<Vec<_>>>()?;
        let current = rows
            .iter()
            .zip(&names)
            .map(|(row, id)| {
                Ok(policy::LeaseMember {
                    seq: number(row, "seq")?,
                    message_id: id,
                    generation: u64::try_from(number(row, "lease_generation")?)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(policy::settlement_matches(&current, &submitted).then_some(rows))
    }
    async fn settle(&self, context: &Context, body: &Value) -> Result<bool> {
        self.open()?;
        let Some(rows) = self.matching(body)? else {
            return Ok(false);
        };
        let Some(consumer) = self.consumer() else {
            return Ok(false);
        };
        let at = now();
        let acks = body["explicitAcks"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect::<std::collections::HashSet<_>>();
        let retries = body["retryMessages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|retry| {
                retry["msgId"]
                    .as_str()
                    .map(|id| (id, retry.get("delaySeconds")))
            })
            .collect::<HashMap<_, _>>();
        let retry_batch = body["retryBatch"]["retry"] == true;
        let exception = body["outcome"] == "exception";
        let ack_all = body["ackAll"] == true;
        self.storage.transaction_sync(|_| {for row in &rows {
            let id=text(row,"id")?;let seq=number(row,"seq")?;let generation=number(row,"lease_generation")?;let lease=body["leaseId"].as_str().unwrap();
            let retry=!ack_all && !acks.contains(id.as_str()) && (retries.contains_key(id.as_str()) || retry_batch || exception);let purged=number(row,"purge_on_settle")?!=0;
            let attempt=u16::try_from(number(row,"attempts")?+1)?;let exhausted=policy::exhausted(attempt,consumer.max_retries);
            if purged || !retry || (exhausted && consumer.dead_letter_queue.is_none()) {self.exec("DELETE FROM __sys_queue_messages WHERE seq=? AND lease_id=? AND lease_generation=?",&[seq.into(),lease.into(),generation.into()])?;continue;}
            let explicit=if let Some(delay)=retries.get(id.as_str()){*delay}else if retry_batch {body["retryBatch"].get("delaySeconds")}else{None};
            let explicit=explicit.filter(|value|!value.is_null()).map(|value|value.as_u64().and_then(|value|u32::try_from(value).ok()).ok_or_else(||error("Queue retry explicitSeconds is out of range"))).transpose()?;
            if exhausted {self.exec("UPDATE __sys_queue_messages SET attempts=attempts+1,lease_id=NULL,leased_until=NULL,dlq_target=?,dlq_transfer_id=?,transfer_kind='dead-letter' WHERE seq=? AND lease_id=? AND lease_generation=?",&[consumer.dead_letter_queue.clone().unwrap().into(),uuid(4,None)?.into(),seq.into(),lease.into(),generation.into()])?;}
            else {self.exec("UPDATE __sys_queue_messages SET attempts=attempts+1,visible_at=?,lease_id=NULL,leased_until=NULL WHERE seq=? AND lease_id=? AND lease_generation=?",&[policy::retry_at(at,explicit,consumer.retry_delay_seconds).into(),seq.into(),lease.into(),generation.into()])?;}
        }Ok(())})?;
        if let Err(error) = self.drive_dlq(context, 100).await {
            tracing::error!(%error,"Queue DLQ transfer deferred");
        }
        self.rearm(context, now(), false).await?;
        Ok(true)
    }
    async fn purge(&self, context: &Context) -> Result<Value> {
        self.open()?;
        let at = now();
        let (deleted,leased)=self.storage.transaction_sync(|_| {let mut after=0;let mut deleted=0;let mut leased=0;loop {
            let rows=self.exec("SELECT seq,lease_id,leased_until FROM __sys_queue_messages WHERE seq>? ORDER BY seq LIMIT 256",&[after.into()])?;if rows.is_empty(){break;}after=number(rows.last().unwrap(),"seq")?;
            let facts=rows.iter().map(|row|Ok(policy::PurgeRow{seq:number(row,"seq")?,lease_id_present:optional_text(row,"lease_id")?.is_some(),leased_until:optional_number(row,"leased_until")?})).collect::<Result<Vec<_>>>()?;let plan=policy::purge_plan(at,&facts);
            for seq in plan.delete {self.exec("DELETE FROM __sys_queue_messages WHERE seq=? AND (lease_id IS NULL OR leased_until IS NULL OR leased_until<=?)",&[seq.into(),at.into()])?;deleted+=1;}
            for seq in plan.mark_for_settle {self.exec("UPDATE __sys_queue_messages SET purge_on_settle=1 WHERE seq=? AND lease_id IS NOT NULL AND leased_until>?",&[seq.into(),at.into()])?;leased+=1;}
        }Ok((deleted,leased))})?;
        self.rearm(context, at, false).await?;
        Ok(json!({"deleted":deleted,"leased":leased,"metrics":self.metrics(at,false)?}))
    }
    fn peek(&self, limit: i64) -> Result<Value> {
        self.open()?;
        let at = now();
        let rows=self.exec("SELECT id,body,content_type,size,enqueued_at,visible_at,attempts,lease_id,leased_until,purge_on_settle,dlq_target,dead_letter_source FROM __sys_queue_messages WHERE enqueued_at>? ORDER BY visible_at,seq LIMIT ?",&[at.saturating_sub(self.config.limits.retention_ms).into(),limit.into()])?;
        let messages=rows.iter().map(|row| {let lease=optional_text(row,"lease_id")?.is_some() && optional_number(row,"leased_until")?.is_some_and(|until|until>at);let target=optional_text(row,"dlq_target")?;
            Ok(json!({"id":text(row,"id")?,"bodyBase64":base64::engine::general_purpose::STANDARD.encode(blob(row,"body")?),"contentType":text(row,"content_type")?,"size":number(row,"size")?,"enqueuedAt":number(row,"enqueued_at")?,"visibleAt":number(row,"visible_at")?,"attempts":number(row,"attempts")?,"state":if target.is_some(){"moving"}else if number(row,"purge_on_settle")?!=0{"purging"}else if lease{"leased"}else if number(row,"visible_at")?<=at{"visible"}else{"delayed"},"leasedUntil":if lease{optional_number(row,"leased_until")?}else{None},"transferTarget":target,"deadLetterSource":optional_text(row,"dead_letter_source")?}))
        }).collect::<Result<Vec<_>>>()?;
        Ok(json!({"messages":messages}))
    }
    async fn redrive(&self, context: &Context, limit: i64) -> Result<Value> {
        self.drive_dlq(context, 100).await?;
        let at = now();
        let rows=self.exec("SELECT seq,dead_letter_source FROM __sys_queue_messages WHERE dead_letter_source IS NOT NULL AND dlq_target IS NULL AND purge_on_settle=0 AND enqueued_at>? AND (lease_id IS NULL OR leased_until<=?) ORDER BY visible_at,seq LIMIT ?",&[at.saturating_sub(self.config.limits.retention_ms).into(),at.into(),limit.into()])?;
        self.storage.transaction_sync(|_| {for row in &rows {let source=text(row,"dead_letter_source")?;self.exec("UPDATE __sys_queue_messages SET dlq_target=?,dlq_transfer_id=?,transfer_kind='redrive',lease_id=NULL,leased_until=NULL WHERE seq=? AND dead_letter_source=? AND dlq_target IS NULL AND purge_on_settle=0 AND (lease_id IS NULL OR leased_until<=?)",&[source.clone().into(),uuid(4,None)?.into(),number(row,"seq")?.into(),source.into(),at.into()])?;}Ok(())})?;
        self.rearm(context, at, false).await?;
        self.drive_dlq(context, 100).await?;
        self.rearm(context, now(), false).await?;
        Ok(json!({"redriven":rows.len(),"metrics":self.metrics(now(),false)?}))
    }
    async fn rpc(
        self: &Arc<Self>,
        context: &Context,
        method: &str,
        args: &RpcData,
    ) -> Result<RpcData> {
        let graph = decode_rpc(args)?;
        let arguments = wire::array(&graph, &graph.root)?;
        let body = arguments.first().unwrap_or(&CloneValue::Undefined);
        let result = match method {
            "__queueMetrics" => self.metrics(now(), false)?,
            "__queueSend" => {
                let entries = wire::array(&graph, wire::field(&graph, body, "entries"))?
                    .iter()
                    .map(|entry| {
                        let field = |name| wire::field(&graph, entry, name);
                        Ok(Encoded {
                            body: wire::bytes(&graph, field("body"))?,
                            content_type: wire::string(field("contentType"))?,
                            delay: u32::try_from(wire::number(field("delaySeconds"))?)?,
                        })
                    })
                    .collect::<Result<_>>()?;
                self.enqueue(context, entries)?
                    .await
                    .map_err(|_| error("Queue producer group was retired"))?
                    .map_err(anyhow::Error::msg)?
            }
            "__queueDlqAcceptBatch" => self.accept(context, &graph, body).await?,
            _ => return Err(error(format!("Queue cell has no RPC method {method}"))),
        };
        match args {
            RpcData::Json(_) => Ok(RpcData::Json(result.to_string())),
            RpcData::Structured(_) => encode_rpc(&StructuredValue::from_json(result)?),
        }
    }
    async fn fetch(&self, context: &Context, request: crate::application::Request) -> HttpResponse {
        if request.method != "POST" {
            return http(404, b"Queue route not found".to_vec(), false);
        }
        let body = async {
            let mut stream = context.take_body(request.body)?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await.map_err(anyhow::Error::msg)? {
                bytes.extend(chunk);
            }
            Ok::<_, anyhow::Error>(serde_json::from_slice::<Value>(&bytes)?)
        }
        .await;
        let Ok(body) = body else {
            return http(400, b"invalid Queue request".to_vec(), false);
        };
        let result = async {
            if url::Url::parse(&request.url)?.path() == "/__qSettle" {
                return Ok(if self.settle(context, &body).await? {
                    http(204, Vec::new(), false)
                } else {
                    http(409, b"stale Queue settlement".to_vec(), false)
                });
            }
            let result = match body["op"].as_str() {
                Some("info") => {
                    let mut metrics = self.metrics(now(), false)?;
                    metrics["stored"] = json!(number(
                        &self
                            .exec("SELECT stored_count FROM __sys_queue_stats WHERE id=1", &[])?
                            .remove(0),
                        "stored_count"
                    )?);
                    metrics["paused"] = json!(self.paused()?);
                    metrics
                }
                Some("purge") => self.purge(context).await?,
                Some(op @ ("pause" | "resume")) => {
                    let paused = op == "pause";
                    self.set_meta("paused", paused.then(|| "1".into()))?;
                    self.rearm(context, now(), false).await?;
                    let mut metrics = self.metrics(now(), false)?;
                    metrics["paused"] = json!(paused);
                    metrics
                }
                Some(op @ ("peek" | "redrive")) => {
                    let limit = match body.get("limit").filter(|value| !value.is_null()) {
                        None => {
                            if op == "peek" {
                                10
                            } else {
                                100
                            }
                        }
                        Some(value) => value
                            .as_i64()
                            .ok_or_else(|| error("limit must be an integer from 1 through 100"))?,
                    };
                    ensure!(
                        (1..=100).contains(&limit),
                        "QUEUE_ERROR: limit must be an integer from 1 through 100"
                    );
                    if op == "peek" {
                        self.peek(limit)?
                    } else {
                        self.redrive(context, limit).await?
                    }
                }
                _ => {
                    return Err(error(format!(
                        "unknown operation {}",
                        body.get("op")
                            .map(Value::to_string)
                            .unwrap_or_else(|| "undefined".into())
                    )))
                }
            };
            Ok(http(
                200,
                serde_json::to_vec(&json!({"result":result}))?,
                true,
            ))
        }
        .await;
        match result {
            Ok(reply) => reply,
            Err(error) => http(
                400,
                serde_json::to_vec(&json!({"error":error.to_string()})).expect("Queue error JSON"),
                true,
            ),
        }
    }
}
fn http(status: u16, body: Vec<u8>, json: bool) -> HttpResponse {
    HttpResponse {
        status,
        body,
        stream: None,
        headers: if json {
            vec![("content-type".into(), "application/json".into())]
        } else {
            Vec::new()
        },
        websocket: None,
        write_position: None,
        observed_position: None,
    }
}
impl Cell for Broker {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(request) => Ok(Output::Http(self.fetch(&context, request).await)),
                Invocation::Alarm { .. } => {
                    self.alarm(&context).await?;
                    Ok(Output::Done)
                }
                Invocation::CellRpc { method, args } => match self
                    .rpc(&context, &method, &args)
                    .await
                {
                    Ok(reply) => Ok(Output::Rpc(reply)),
                    Err(error) if matches!(args, RpcData::Structured(_)) => {
                        Ok(Output::Rpc(RpcData::Structured(
                            cellar_native::rpc::encode_error(&error.to_string(), "Error", None)?
                                .into(),
                        )))
                    }
                    Err(error) => Err(error),
                },
                _ => Err(error("Queue cell received an unsupported event")),
            }
        })
    }
}
