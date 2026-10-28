// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    config::{digest, identifier},
    Config,
};
use crate::{
    remote_http::{self, CONTROL_BYTES},
    Validation,
};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    containers::{
        BindingCheckpoint, BindingIntent as Intent, BindingJournal,
        BindingObservation as Observation, BindingRecord,
    },
    Context,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Resource {
    pub name: String,
    pub session: String,
}

pub(super) fn pairs(value: &Value) -> Result<BTreeMap<String, String>> {
    let mut pairs = BTreeMap::new();
    if value.is_null() {
        return Ok(pairs);
    }
    for pair in value.as_array().context("Vercel string pairs missing")? {
        ensure!(
            pairs
                .insert(
                    pair[0].as_str().unwrap().into(),
                    pair[1].as_str().unwrap().into()
                )
                .is_none(),
            "duplicate Vercel environment/label key"
        );
    }
    Ok(pairs)
}

impl Config {
    pub(super) async fn control(
        &self,
        context: &Context,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> Result<remote_http::Reply> {
        remote_http::request(
            context,
            self.url(path)?,
            method,
            self.headers("application/json"),
            body.map(|value| serde_json::to_vec(&value)).transpose()?,
            CONTROL_BYTES,
            self.timeout_ms,
        )
        .await
    }

    fn journal(&self, context: &Context) -> Result<BindingJournal> {
        BindingJournal::open(context, &self.binding)
    }

    fn check_record(&self, record: &BindingRecord) -> Result<()> {
        ensure!(record.provider=="vercel" && record.configuration_digest==self.digest(),"Vercel binding belongs to another team/project/configuration; retire it using its original provider");
        Ok(())
    }

    fn name(record: &BindingRecord) -> String {
        format!("cellar-{}", record.request_id)
    }

    fn tag(&self, context: &Context, record: &BindingRecord) -> Result<String> {
        Ok(digest(&json!([
            self.namespace,
            context
                .identity()
                .context("Vercel requires a root cell")?
                .scope,
            self.binding,
            record.generation,
            record.request_id,
            record.configuration_digest,
            record.parameters_digest
        ])))
    }

    fn resource(record: &BindingRecord) -> Result<Option<Resource>> {
        record
            .resource_id
            .as_ref()
            .map(|encoded| {
                let resource: Resource = serde_json::from_str(encoded)?;
                identifier(&resource.name)?;
                identifier(&resource.session)?;
                ensure!(
                    resource.name == Self::name(record),
                    "Vercel journal resource name changed"
                );
                Ok(resource)
            })
            .transpose()
    }

    fn verify(&self, context: &Context, record: &BindingRecord, info: &Value) -> Result<Resource> {
        let sandbox = &info["sandbox"];
        let session = &info["session"];
        let name = sandbox["name"]
            .as_str()
            .context("Vercel sandbox name missing")?;
        let id = session["id"]
            .as_str()
            .context("Vercel session ID missing")?;
        identifier(name)?;
        identifier(id)?;
        ensure!(
            name == Self::name(record) && sandbox["tags"]["cellar"] == self.tag(context, record)?,
            "Vercel resource metadata does not match the durable binding"
        );
        ensure!(
            sandbox["persistent"] == false,
            "Vercel resource persistence was changed"
        );
        ensure!(
            sandbox["currentSessionId"] == id,
            "Vercel named resource/session association mismatch"
        );
        if let Some(known) = Self::resource(record)? {
            ensure!(
                known.session == id,
                "Vercel session changed; implicit resume is forbidden"
            );
        }
        Ok(Resource {
            name: name.into(),
            session: id.into(),
        })
    }

    async fn info(&self, context: &Context, record: &BindingRecord) -> Result<Option<Value>> {
        self.check_record(record)?;
        let mut url = url::Url::parse(&self.url(&format!("v2/sandboxes/{}", Self::name(record)))?)?;
        url.query_pairs_mut()
            .append_pair("projectId", &self.project)
            .append_pair("resume", "false");
        let reply = remote_http::request(
            context,
            url.into(),
            "GET",
            self.headers("application/json"),
            None,
            CONTROL_BYTES,
            self.timeout_ms,
        )
        .await?;
        if reply.status == 404 {
            return Ok(None);
        }
        ensure!(
            reply.status == 200,
            "Vercel inspect failed: HTTP {}",
            reply.status
        );
        let value: Value = serde_json::from_slice(&reply.body)?;
        self.verify(context, record, &value)?;
        Ok(Some(value))
    }

    async fn confirm(
        &self,
        context: &Context,
        journal: &BindingJournal,
        checkpoint: BindingCheckpoint,
        info: &Value,
    ) -> Result<BindingCheckpoint> {
        let resource = self.verify(context, &checkpoint.record, info)?;
        ensure!(
            info["sandbox"]["status"] == "running" && info["session"]["status"] == "running",
            "Vercel initial session is not ready"
        );
        let mut record = checkpoint.record;
        record.resource_id = Some(serde_json::to_string(&resource)?);
        record.observation = Observation::Confirmed;
        journal
            .compare_exchange(context, Some(checkpoint.revision), record)
            .await
    }

    pub(super) async fn connected(&self, context: &Context) -> Result<Resource> {
        let journal = self.journal(context)?;
        let checkpoint = journal
            .load(context)?
            .context("Vercel sandbox has not been started")?;
        self.check_record(&checkpoint.record)?;
        ensure!(
            checkpoint.record.intent == Intent::Running
                && checkpoint.record.observation == Observation::Confirmed,
            "Vercel lifecycle requires explicit reconciliation"
        );
        let info = self
            .info(context, &checkpoint.record)
            .await?
            .context("Vercel sandbox expired or was removed")?;
        ensure!(
            info["sandbox"]["status"] == "running" && info["session"]["status"] == "running",
            "Vercel session is not running; implicit resume is forbidden"
        );
        journal.check_retention(context)?;
        ensure!(
            journal.load(context)?.as_ref() == Some(&checkpoint),
            "Vercel binding changed while inspecting the resource"
        );
        self.verify(context, &checkpoint.record, &info)
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
        let info = self.info(context, &checkpoint.record).await?;
        let Some(info) = info else {
            // An absent observation cannot resolve an unacknowledged create.
            ensure!(
                checkpoint.record.observation == Observation::Confirmed
                    || checkpoint.record.intent == Intent::Deleted,
                "Vercel creation remains unresolved; absence does not authorize replacement"
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
        let states = [
            info["sandbox"]["status"].as_str(),
            info["session"]["status"].as_str(),
        ];
        ensure!(
            states.iter().all(|state| matches!(
                state,
                Some(
                    "pending"
                        | "running"
                        | "stopping"
                        | "stopped"
                        | "failed"
                        | "aborted"
                        | "snapshotting"
                )
            )),
            "unknown Vercel resource state"
        );
        let state = if states == [Some("running"); 2] {
            State::Running
        } else {
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
            "Vercel lifecycle requires explicit reconciliation"
        );
        let Some(info) = self.info(context, &checkpoint.record).await? else {
            return Ok(json!(false));
        };
        let state = info["session"]["status"]
            .as_str()
            .context("Vercel session status missing")?;
        ensure!(
            matches!(
                state,
                "pending"
                    | "running"
                    | "stopping"
                    | "stopped"
                    | "failed"
                    | "aborted"
                    | "snapshotting"
            ),
            "unknown Vercel session state"
        );
        Ok(json!(state == "running"))
    }

    pub(super) async fn start(&self, context: &Context, input: Value) -> Result<Value> {
        let options = &input["options"];
        let (env, labels, internet) = (|| {
            ensure!(
                options.get("entrypoint").is_none_or(Value::is_null),
                "Vercel uses the host-selected image entrypoint"
            );
            let env = pairs(&options["env"])?;
            let labels = pairs(&options["labels"])?;
            ensure!(
                labels.len() <= 4
                    && serde_json::to_vec(&labels)?.len() <= 4096
                    && !labels
                        .keys()
                        .any(|key| key == "cellar" || key.starts_with("cellar.")),
                "Vercel tag limits/reserved keys exceeded"
            );
            let internet = options["enable_internet"].as_bool().unwrap_or(false);
            ensure!(
                !internet || self.allow_internet,
                "Vercel internet access is denied by host configuration"
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
                    "Vercel pending create has different parameters"
                );
                let info = self.info(context, &checkpoint.record).await?.context(
                    "Vercel creation remains unresolved; absence does not authorize a new create",
                )?;
                let confirmed = self
                    .confirm(context, &journal, checkpoint.clone(), &info)
                    .await?;
                return Ok(json!(confirmed.record.generation.to_string()));
            }
            if checkpoint.record.intent == Intent::Running {
                ensure!(
                    self.info(context, &checkpoint.record).await?.is_none(),
                    "Vercel sandbox already exists; destroy it before replacement"
                );
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
                    "Vercel deletion remains unresolved"
                );
            }
        }
        let generation = previous.as_ref().map_or(Ok(1), |c| {
            c.record
                .generation
                .checked_add(1)
                .context("Vercel generation exhausted")
        })?;
        let mut nonce = [0u8; 16];
        cellar::asyncrt::rng("vercel.create").fill_bytes(&mut nonce);
        let record = BindingRecord {
            provider: "vercel".into(),
            configuration_digest: self.digest(),
            parameters_digest: parameters,
            generation,
            request_id: nonce.iter().map(|b| format!("{b:02x}")).collect(),
            resource_id: None,
            intent: Intent::Running,
            observation: Observation::Pending,
        };
        let name = Self::name(&record);
        let mut tags = labels;
        tags.insert("cellar".into(), self.tag(context, &record)?);
        let checkpoint = journal
            .compare_exchange(context, previous.as_ref().map(|c| c.revision), record)
            .await?;
        if let Err(error) = journal.check_retention(context) {
            if error.is::<cellar::sdk::containers::RetentionExpired>() {
                // No create was submitted for this invocation's reservation.
                // Never apply this retirement to an earlier uncertain create.
                let mut retired = checkpoint.record;
                retired.intent = Intent::Deleted;
                retired.observation = Observation::Confirmed;
                journal
                    .compare_exchange(context, Some(checkpoint.revision), retired)
                    .await?;
            }
            return Err(error);
        }
        let mut create = json!({"projectId":self.project,"name":name,"image":self.image,"persistent":false,"timeout":self.lifetime_ms,"resources":{"vcpus":self.vcpus},"env":env,"tags":tags,"ports":[],"networkPolicy":{"mode":if internet{"allow-all"}else{"deny-all"}}});
        if let Some(region) = &self.region {
            create["region"] = json!(region);
        }
        let reply = self
            .control(context, "POST", "v3/sandboxes", Some(create))
            .await?;
        ensure!(
            matches!(reply.status, 200 | 201),
            "Vercel create failed: HTTP {}; retained intent requires reconciliation",
            reply.status
        );
        let info: Value = serde_json::from_slice(&reply.body)?;
        // Pin the returned session durably before acknowledging readiness.
        let resource = self.verify(context, &checkpoint.record, &info)?;
        let mut known = checkpoint.record;
        known.resource_id = Some(serde_json::to_string(&resource)?);
        let checkpoint = journal
            .compare_exchange(context, Some(checkpoint.revision), known)
            .await?;
        let full = self
            .info(context, &checkpoint.record)
            .await?
            .context("Vercel resource disappeared before acknowledgement")?;
        let confirmed = self.confirm(context, &journal, checkpoint, &full).await?;
        Ok(json!(confirmed.record.generation.to_string()))
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
        let info = self.info(context, &checkpoint.record).await?;
        ensure!(
            info.is_some()
                || checkpoint.record.observation == Observation::Confirmed
                || checkpoint.record.intent == Intent::Deleted,
            "unresolved Vercel create cannot be forgotten after an absent observation"
        );
        if checkpoint.record.resource_id.is_none() {
            let resource = self.verify(
                context,
                &checkpoint.record,
                info.as_ref()
                    .context("Vercel resource identity is unresolved")?,
            )?;
            let mut known = checkpoint.record;
            known.resource_id = Some(serde_json::to_string(&resource)?);
            checkpoint = journal
                .compare_exchange(context, Some(checkpoint.revision), known)
                .await?;
        }
        if checkpoint.record.intent != Intent::Deleted {
            let mut deleting = checkpoint.record;
            deleting.intent = Intent::Deleted;
            deleting.observation = Observation::Pending;
            checkpoint = journal
                .compare_exchange(context, Some(checkpoint.revision), deleting)
                .await?;
        }
        if info.is_some() {
            let mut path = url::Url::parse(
                &self.url(&format!("v2/sandboxes/{}", Self::name(&checkpoint.record)))?,
            )?;
            path.query_pairs_mut()
                .append_pair("projectId", &self.project)
                .append_pair("deleteOrphanSnapshots", "true");
            let reply = remote_http::request(
                context,
                path.into(),
                "DELETE",
                self.headers("application/json"),
                None,
                CONTROL_BYTES,
                self.timeout_ms,
            )
            .await?;
            ensure!(
                matches!(reply.status, 200 | 204 | 404),
                "Vercel delete failed: HTTP {}; deletion remains unresolved",
                reply.status
            );
            ensure!(
                self.info(context, &checkpoint.record).await?.is_none(),
                "Vercel deletion has not been observed; retry reconciliation"
            );
        }
        let mut retired = checkpoint.record;
        retired.observation = Observation::Confirmed;
        journal
            .compare_exchange(context, Some(checkpoint.revision), retired)
            .await?;
        Ok(Value::Null)
    }
}
