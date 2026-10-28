// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    config::{digest, resource_id},
    transport::{self, CONTROL_BYTES},
    Config,
};
use crate::Validation;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    containers::{
        BindingCheckpoint, BindingIntent as Intent, BindingJournal,
        BindingObservation as Observation, BindingRecord,
    },
    Context,
};
use rand::RngCore;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub(super) struct Sandbox {
    pub id: String,
    pub token: String,
}

impl Config {
    fn journal(&self, context: &Context) -> Result<BindingJournal> {
        BindingJournal::open(context, &self.binding)
    }

    fn scope(&self, context: &Context) -> Result<String> {
        Ok(digest(&json!([
            self.namespace,
            context
                .identity()
                .context("E2B requires a root cell")?
                .scope
        ])))
    }

    fn check_record(&self, record: &BindingRecord) -> Result<()> {
        ensure!(record.provider=="e2b" && record.configuration_digest==self.digest(),"E2B binding belongs to another provider/account/configuration; retire it with its original provider");
        Ok(())
    }

    fn tags(&self, context: &Context, record: &BindingRecord) -> Result<Map<String, Value>> {
        Ok(json!({"cellar.scope":self.scope(context)?,"cellar.binding":self.binding,"cellar.request":record.request_id,"cellar.generation":record.generation.to_string(),"cellar.configuration":record.configuration_digest,"cellar.parameters":record.parameters_digest}).as_object().unwrap().clone())
    }

    pub(super) async fn control(
        &self,
        context: &Context,
        method: &str,
        path: &str,
        body: Option<Value>,
        request_id: Option<&str>,
    ) -> Result<transport::Reply> {
        let mut headers = vec![
            ("X-API-Key".into(), self.key.clone()),
            ("Content-Type".into(), "application/json".into()),
        ];
        if let Some(id) = request_id {
            headers.push(("Idempotency-Key".into(), id.into()));
        }
        transport::request(
            context,
            self.api.join(path)?.to_string(),
            method,
            headers,
            body.map(|v| serde_json::to_vec(&v)).transpose()?,
            CONTROL_BYTES,
            self.timeout_ms,
        )
        .await
    }

    fn verify(&self, context: &Context, record: &BindingRecord, info: &Value) -> Result<()> {
        let id = info["sandboxID"]
            .as_str()
            .context("E2B sandbox identity missing")?;
        resource_id(id)?;
        ensure!(
            record
                .resource_id
                .as_deref()
                .is_none_or(|known| known == id),
            "E2B resource identity changed"
        );
        let metadata = info["metadata"]
            .as_object()
            .context("E2B sandbox metadata missing")?;
        for (key, value) in self.tags(context, record)? {
            ensure!(
                metadata.get(&key) == Some(&value),
                "E2B resource metadata does not match the durable binding"
            );
        }
        if let Some(domain) = info["domain"].as_str() {
            ensure!(
                domain == self.domain,
                "E2B sandbox domain differs from the host-selected domain"
            );
        }
        Ok(())
    }

    async fn info(&self, context: &Context, record: &BindingRecord) -> Result<Option<Value>> {
        self.check_record(record)?;
        let id = record
            .resource_id
            .as_deref()
            .context("E2B create outcome is unresolved")?;
        resource_id(id)?;
        let reply = self
            .control(context, "GET", &format!("sandboxes/{id}"), None, None)
            .await?;
        if reply.status == 404 {
            return Ok(None);
        }
        ensure!(
            reply.status == 200,
            "E2B inspect failed: HTTP {}",
            reply.status
        );
        let info: Value = serde_json::from_slice(&reply.body)?;
        self.verify(context, record, &info)?;
        Ok(Some(info))
    }

    async fn reconcile(&self, context: &Context, record: &BindingRecord) -> Result<Value> {
        self.check_record(record)?;
        let tags = self.tags(context, record)?;
        let metadata = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs(tags.iter().map(|(k, v)| (k.as_str(), v.as_str().unwrap())))
            .finish();
        let mut next: Option<String> = None;
        let mut found = None;
        for _ in 0..16 {
            let path = {
                let mut query = url::form_urlencoded::Serializer::new(String::new());
                query
                    .append_pair("metadata", &metadata)
                    .append_pair("limit", "10")
                    .append_pair("state", "running,paused");
                if let Some(token) = next.as_deref() {
                    query.append_pair("nextToken", token);
                }
                format!("v2/sandboxes?{}", query.finish())
            };
            let reply = self.control(context, "GET", &path, None, None).await?;
            ensure!(
                reply.status == 200,
                "E2B reconciliation failed: HTTP {}",
                reply.status
            );
            let entries: Vec<Value> = serde_json::from_slice(&reply.body)?;
            for info in entries {
                self.verify(context, record, &info)?;
                ensure!(
                    found.is_none(),
                    "multiple E2B resources match one creation; refusing ambiguous ownership"
                );
                found = Some(info);
            }
            let token = reply
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("x-next-token"))
                .map(|(_, v)| v.clone())
                .filter(|v| !v.is_empty());
            if token.is_none() {
                return found.context("E2B creation remains unresolved; an empty listing does not authorize another create");
            }
            ensure!(token != next, "E2B pagination cursor did not advance");
            next = token;
        }
        anyhow::bail!("E2B reconciliation exceeds page budget")
    }

    async fn confirm(
        &self,
        context: &Context,
        journal: &BindingJournal,
        checkpoint: BindingCheckpoint,
        info: &Value,
    ) -> Result<BindingCheckpoint> {
        self.verify(context, &checkpoint.record, info)?;
        envd_version(
            info["envdVersion"]
                .as_str()
                .context("E2B envd version missing")?,
        )?;
        let token = info["envdAccessToken"]
            .as_str()
            .context("E2B secure envd token missing")?;
        ensure!(
            !token.is_empty()
                && token.len() <= 4096
                && token.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "invalid E2B envd token encoding"
        );
        ensure!(
            info["state"] == "running",
            "E2B resource was not running at acknowledgement"
        );
        let mut record = checkpoint.record;
        record.resource_id = Some(info["sandboxID"].as_str().unwrap().into());
        record.observation = Observation::Confirmed;
        journal
            .compare_exchange(context, Some(checkpoint.revision), record)
            .await
    }

    pub(super) async fn connected(&self, context: &Context) -> Result<Sandbox> {
        let journal = self.journal(context)?;
        let checkpoint = journal
            .load(context)?
            .context("E2B sandbox has not been started")?;
        self.check_record(&checkpoint.record)?;
        ensure!(
            checkpoint.record.intent == Intent::Running
                && checkpoint.record.observation == Observation::Confirmed,
            "E2B lifecycle outcome requires reconciliation"
        );
        let info = self
            .info(context, &checkpoint.record)
            .await?
            .context("E2B sandbox expired or was removed")?;
        ensure!(info["state"] == "running", "E2B sandbox is not running");
        envd_version(
            info["envdVersion"]
                .as_str()
                .context("E2B envd version missing")?,
        )?;
        let token = info["envdAccessToken"]
            .as_str()
            .context("E2B secure envd token missing")?;
        ensure!(
            !token.is_empty()
                && token.len() <= 4096
                && token.bytes().all(|b| (0x21..=0x7e).contains(&b)),
            "invalid E2B envd token encoding"
        );
        journal.check_retention(context)?;
        ensure!(
            journal.load(context)?.as_ref() == Some(&checkpoint),
            "E2B binding changed while inspecting the resource"
        );
        Ok(Sandbox {
            id: info["sandboxID"].as_str().unwrap().into(),
            token: token.into(),
        })
    }

    /// Observe the retained generation. Provider mutations are never replayed.
    pub(super) async fn reconcile_binding(&self, context: &Context) -> Result<Value> {
        use crate::{Reconciliation, ReconciliationState as State};
        let journal = self.journal(context)?;
        let Some(checkpoint) = journal.load(context)? else {
            return Ok(Reconciliation::value(State::Unbound, None));
        };
        let generation = Some(checkpoint.record.generation);
        if checkpoint.record.is_retired() {
            return Ok(Reconciliation::value(State::Deleted, generation));
        }
        self.check_record(&checkpoint.record)?;
        let info = if checkpoint.record.resource_id.is_none() {
            let found = self.reconcile(context, &checkpoint.record).await?;
            let reply = self
                .control(
                    context,
                    "GET",
                    &format!("sandboxes/{}", found["sandboxID"].as_str().unwrap()),
                    None,
                    None,
                )
                .await?;
            ensure!(
                reply.status == 200,
                "E2B reconciled resource could not be inspected"
            );
            Some(serde_json::from_slice::<Value>(&reply.body)?)
        } else {
            self.info(context, &checkpoint.record).await?
        };
        let Some(info) = info else {
            // An absent observation cannot resolve an unacknowledged create.
            ensure!(
                checkpoint.record.observation == Observation::Confirmed
                    || checkpoint.record.intent == Intent::Deleted,
                "E2B creation remains unresolved; absence does not authorize replacement"
            );
            let mut retired = checkpoint.record;
            retired.intent = Intent::Deleted;
            retired.observation = Observation::Confirmed;
            journal
                .compare_exchange(context, Some(checkpoint.revision), retired)
                .await?;
            return Ok(Reconciliation::value(State::Deleted, generation));
        };
        self.verify(context, &checkpoint.record, &info)?;
        if checkpoint.record.intent == Intent::Deleted {
            return Ok(Reconciliation::value(State::Deleting, generation));
        }
        if checkpoint.record.observation == Observation::Pending {
            self.confirm(context, &journal, checkpoint, &info).await?;
            return Ok(Reconciliation::value(State::Running, generation));
        }
        let state = if info["state"] == "running" {
            State::Running
        } else {
            ensure!(info["state"] == "paused", "unknown E2B resource state");
            State::Stopped
        };
        Ok(Reconciliation::value(state, generation))
    }

    pub(super) async fn running(&self, context: &Context) -> Result<Value> {
        let Some(checkpoint) = self.journal(context)?.load(context)? else {
            return Ok(json!(false));
        };
        if checkpoint.record.is_retired() {
            return Ok(json!(false));
        }
        self.check_record(&checkpoint.record)?;
        ensure!(
            checkpoint.record.observation == Observation::Confirmed,
            "E2B lifecycle outcome requires explicit reconciliation"
        );
        let Some(info) = self.info(context, &checkpoint.record).await? else {
            return Ok(json!(false));
        };
        let state = info["state"].as_str().context("E2B state missing")?;
        ensure!(
            matches!(state, "running" | "paused"),
            "unsupported E2B sandbox state"
        );
        Ok(json!(state == "running"))
    }

    pub(super) async fn start(&self, context: &Context, input: Value) -> Result<Value> {
        let options = &input["options"];
        let (env, labels, internet) = (|| {
            ensure!(
                options.get("entrypoint").is_none_or(Value::is_null),
                "E2B uses the host-selected template entrypoint"
            );
            let env = pairs(&options["env"])?;
            let labels = pairs(&options["labels"])?;
            ensure!(
                labels.len() <= 16
                    && serde_json::to_vec(&labels)?.len() <= 4096
                    && !labels.keys().any(|key| key.starts_with("cellar.")),
                "E2B labels exceed limits or use reserved cellar. metadata"
            );
            let internet = options["enable_internet"].as_bool().unwrap_or(false);
            ensure!(
                !internet || self.allow_internet,
                "E2B internet access is not permitted by host configuration"
            );
            Ok::<_, anyhow::Error>((env, labels, internet))
        })()
        .map_err(|error| error.context(Validation))?;
        let parameters = digest(&json!({"env":env,"labels":labels,"internet":internet}));
        let journal = self.journal(context)?;
        let mut previous = journal.load(context)?;
        // Configuration changes require proven retirement of the old generation.
        if let Some(checkpoint) = previous.as_ref().filter(|c| !c.record.is_retired()) {
            self.check_record(&checkpoint.record)?;
            if checkpoint.record.intent == Intent::Running
                && checkpoint.record.observation == Observation::Pending
            {
                ensure!(
                    checkpoint.record.parameters_digest == parameters,
                    "pending E2B create has different parameters"
                );
                let info = self.reconcile(context, &checkpoint.record).await?;
                let full = self
                    .control(
                        context,
                        "GET",
                        &format!("sandboxes/{}", info["sandboxID"].as_str().unwrap()),
                        None,
                        None,
                    )
                    .await?;
                ensure!(
                    full.status == 200,
                    "E2B reconciled resource could not be inspected"
                );
                let full: Value = serde_json::from_slice(&full.body)?;
                let checkpoint = self
                    .confirm(context, &journal, checkpoint.clone(), &full)
                    .await?;
                return Ok(json!(checkpoint.record.generation.to_string()));
            }
            if checkpoint.record.intent == Intent::Running {
                ensure!(self.info(context,&checkpoint.record).await?.is_none(),"E2B sandbox already exists; destroy it before replacing a running or paused instance");
                let mut retired = checkpoint.record.clone();
                retired.intent = Intent::Deleted;
                retired.observation = Observation::Confirmed;
                previous = Some(
                    journal
                        .compare_exchange(context, Some(checkpoint.revision), retired)
                        .await?,
                );
            } else {
                ensure!(
                    checkpoint.record.observation == Observation::Confirmed,
                    "E2B deletion is unresolved"
                );
            }
        }
        let generation = previous.as_ref().map_or(Ok(1), |c| {
            c.record
                .generation
                .checked_add(1)
                .context("E2B generation exhausted")
        })?;
        let mut nonce = [0u8; 16];
        cellar::asyncrt::rng("e2b.create").fill_bytes(&mut nonce);
        let request_id = nonce.iter().map(|b| format!("{b:02x}")).collect();
        let record = BindingRecord {
            provider: "e2b".into(),
            configuration_digest: self.digest(),
            parameters_digest: parameters,
            generation,
            request_id,
            resource_id: None,
            intent: Intent::Running,
            observation: Observation::Pending,
        };
        let mut metadata = self.tags(context, &record)?;
        metadata.extend(labels.into_iter().map(|(k, v)| (k, Value::String(v))));
        let checkpoint = journal
            .compare_exchange(context, previous.as_ref().map(|c| c.revision), record)
            .await?;
        if let Err(error) = journal.check_retention(context) {
            if error.is::<cellar::sdk::containers::RetentionExpired>() {
                // This invocation owns the new reservation and has not sent
                // its create. Only this proven non-submission may retire it.
                let mut retired = checkpoint.record;
                retired.intent = Intent::Deleted;
                retired.observation = Observation::Confirmed;
                journal
                    .compare_exchange(context, Some(checkpoint.revision), retired)
                    .await?;
            }
            return Err(error);
        }
        let reply=self.control(context,"POST","v2/sandboxes",Some(json!({"templateID":self.template,"timeout":self.lifetime,"autoPause":false,"autoResume":{"enabled":false},"allow_internet_access":internet,"network":{"allowPublicTraffic":false},"metadata":metadata,"envVars":env})),Some(&checkpoint.record.request_id)).await?;
        ensure!(
            reply.status == 201,
            "E2B create failed: HTTP {}; retained reservation requires reconciliation",
            reply.status
        );
        let created: Value = serde_json::from_slice(&reply.body)?;
        let id = created["sandboxID"]
            .as_str()
            .context("E2B create response has no sandbox ID")?;
        resource_id(id)?;
        let mut known = checkpoint.record;
        known.resource_id = Some(id.into());
        let checkpoint = journal
            .compare_exchange(context, Some(checkpoint.revision), known)
            .await?;
        let info = self
            .info(context, &checkpoint.record)
            .await?
            .context("E2B resource disappeared before acknowledgement")?;
        let checkpoint = self.confirm(context, &journal, checkpoint, &info).await?;
        Ok(json!(checkpoint.record.generation.to_string()))
    }

    pub(super) async fn destroy(&self, context: &Context) -> Result<Value> {
        let journal = self.journal(context)?;
        let Some(mut checkpoint) = journal.load(context)? else {
            return Ok(Value::Null);
        };
        if checkpoint.record.is_retired() {
            return Ok(Value::Null);
        }
        self.check_record(&checkpoint.record)?;
        if checkpoint.record.observation == Observation::Pending
            && checkpoint.record.resource_id.is_none()
        {
            let info = self.reconcile(context, &checkpoint.record).await?;
            let mut known = checkpoint.record;
            known.resource_id = Some(info["sandboxID"].as_str().unwrap().into());
            checkpoint = journal
                .compare_exchange(context, Some(checkpoint.revision), known)
                .await?;
        }
        let info = self.info(context, &checkpoint.record).await?;
        ensure!(
            info.is_some()
                || checkpoint.record.observation == Observation::Confirmed
                || checkpoint.record.intent == Intent::Deleted,
            "an unconfirmed E2B ID cannot prove the original create absent"
        );
        if checkpoint.record.intent != Intent::Deleted {
            let mut deleting = checkpoint.record;
            deleting.intent = Intent::Deleted;
            deleting.observation = Observation::Pending;
            checkpoint = journal
                .compare_exchange(context, Some(checkpoint.revision), deleting)
                .await?;
        }
        if info.is_some() {
            let id = checkpoint.record.resource_id.as_deref().unwrap();
            let reply = self
                .control(context, "DELETE", &format!("sandboxes/{id}"), None, None)
                .await?;
            ensure!(
                matches!(reply.status, 204 | 404),
                "E2B delete failed: HTTP {}; deletion remains unresolved",
                reply.status
            );
        }
        ensure!(
            self.info(context, &checkpoint.record).await?.is_none(),
            "E2B deletion has not been observed; retry reconciliation"
        );
        let mut retired = checkpoint.record;
        retired.observation = Observation::Confirmed;
        journal
            .compare_exchange(context, Some(checkpoint.revision), retired)
            .await?;
        Ok(Value::Null)
    }
}

pub(super) fn pairs(value: &Value) -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    if value.is_null() {
        return Ok(map);
    }
    for pair in value.as_array().context("invalid E2B string pairs")? {
        let key = pair[0].as_str().unwrap();
        ensure!(
            map.insert(key.into(), pair[1].as_str().unwrap().into())
                .is_none(),
            "duplicate E2B environment/label key"
        );
    }
    Ok(map)
}

pub(super) fn envd_version(value: &str) -> Result<()> {
    let fields = value
        .split('.')
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        fields.len() == 3 && fields[0] == 0 && (fields[1], fields[2]) >= (5, 2),
        "E2B template requires compatible envd >=0.5.2 and <1.0"
    );
    Ok(())
}
