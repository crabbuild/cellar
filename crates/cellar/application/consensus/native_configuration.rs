// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable deployment values admitted before ordinary Raft cell construction.

use super::RegistrationError;
use crate::engine_api::{Compat, WorkerConfigOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeQueueBinding {
    environment: String,
    queue: String,
    #[serde(default)]
    delivery_delay: u32,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeWorkflowBinding {
    pub environment: String,
    pub workflow: String,
    pub class: String,
    #[serde(default)]
    pub settings: crate::application::workflow::BindingSettings,
}

/// Provisioning carries the same values as the daemon's deployment. Node-local
/// identity is deliberately excluded from the namespace's immutable definition.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct NativeCellConfiguration {
    pub(crate) workflow_bindings: Vec<NativeWorkflowBinding>,
    pub(crate) vars: Vec<(String, String)>,
    pub(crate) compat: Compat,
    pub(crate) do_classes: Vec<String>,
    pub(crate) bindings: Vec<(String, String)>,
    pub(crate) sql_bindings: Vec<(String, String)>,
    pub(crate) kv_bindings: Vec<(String, String)>,
    pub(crate) queue_bindings: Vec<NativeQueueBinding>,
    pub(crate) queue_consumers: Vec<crate::protocol::QueueConsumerConfig>,
    pub(crate) crons: Vec<String>,
    pub(crate) cron_history: Option<crate::application::cron::HistoryConfiguration>,
}

impl NativeCellConfiguration {
    pub(crate) fn options(&self, script: &str) -> WorkerConfigOptions {
        WorkerConfigOptions {
            script_name: script.into(),
            src: String::new(),
            do_classes: self.do_classes.clone(),
            bindings: self.bindings.clone(),
            blob_bindings: Vec::new(),
            sql_bindings: self.sql_bindings.clone(),
            kv_bindings: self.kv_bindings.clone(),
            queue_bindings: self
                .queue_bindings
                .iter()
                .map(|b| crate::engine_api::QueueBinding {
                    environment: b.environment.clone(),
                    queue: b.queue.clone(),
                    delivery_delay: b.delivery_delay,
                })
                .collect(),
            stream_bindings: Vec::new(),
            queue_consumers: self.queue_consumers.clone(),
            workflow_bindings: self
                .workflow_bindings
                .iter()
                .map(|b| crate::engine_api::WorkflowBinding {
                    environment: b.environment.clone(),
                    workflow: b.workflow.clone(),
                    class: b.class.clone(),
                    settings: b.settings.clone(),
                })
                .collect(),
            vars: self.vars.clone(),
            node: String::new(),
            modules: Vec::new(),
            compat: self.compat,
        }
    }
}

#[derive(Clone)]
pub(in crate::application) struct AdmittedCellConfiguration {
    pub(in crate::application) workflow_bindings: Arc<BTreeMap<String, NativeWorkflowBinding>>,
    pub(in crate::application) script: String,
    pub(in crate::application) vars: Arc<BTreeMap<String, String>>,
    pub(in crate::application) compat: Compat,
    pub(in crate::application) classes: Arc<BTreeSet<String>>,
    pub(in crate::application) bindings: Arc<BTreeMap<String, String>>,
    pub(in crate::application) sql_bindings: Arc<BTreeMap<String, String>>,
    pub(in crate::application) kv_bindings: Arc<BTreeMap<String, String>>,
    pub(in crate::application) queue_bindings: Arc<BTreeMap<String, (String, u32)>>,
    pub(in crate::application) queue_consumers:
        Arc<BTreeMap<String, crate::protocol::QueueConsumerConfig>>,
}

impl AdmittedCellConfiguration {
    pub(in crate::application) fn from_options(
        options: &WorkerConfigOptions,
    ) -> Result<Self, RegistrationError> {
        // Do not turn a declared provider into an empty binding map. Each
        // adapter must first acquire shared execution and settlement authority.
        if !options.src.is_empty()
            || !options.modules.is_empty()
            || !options.blob_bindings.is_empty()
            || !options.stream_bindings.is_empty()
        {
            return Err(RegistrationError::Invalid(
                "unimplemented Raft cell bindings",
            ));
        }
        if options.compat.sqlite_vec {
            return Err(RegistrationError::Invalid(
                "unimplemented Raft SQLite extension",
            ));
        }
        if options.script_name.is_empty() || options.script_name.chars().any(char::is_control) {
            return Err(RegistrationError::Invalid("Raft cell script"));
        }
        if options.vars.len() > 256 {
            return Err(RegistrationError::Limit);
        }
        let mut bytes = options.script_name.len();
        let mut vars = BTreeMap::new();
        for (name, value) in &options.vars {
            if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
                return Err(RegistrationError::Invalid("Raft cell variable name"));
            }
            bytes = bytes
                .checked_add(name.len())
                .and_then(|bytes| bytes.checked_add(value.len()))
                .ok_or(RegistrationError::Limit)?;
            if bytes > 64 * 1024 {
                return Err(RegistrationError::Limit);
            }
            if vars.insert(name.clone(), value.clone()).is_some() {
                // Variable values may contain secrets. Never print them in
                // admission errors or retain a second, ambiguous definition.
                return Err(RegistrationError::Invalid("duplicate Raft cell variable"));
            }
        }
        if bytes > 64 * 1024 {
            return Err(RegistrationError::Limit);
        }
        if options.do_classes.len() > 256
            || options.bindings.len() > 256
            || options.sql_bindings.len() > 256
            || options.kv_bindings.len() > 256
            || options.queue_bindings.len() > 256
            || options.queue_consumers.len() > 256
            || options.workflow_bindings.len() > 256
        {
            return Err(RegistrationError::Limit);
        }
        let mut classes = BTreeSet::new();
        let mut bindings = BTreeMap::new();
        for class in &options.do_classes {
            super::class::validate_name(class)?;
            bytes = bytes
                .checked_add(class.len())
                .ok_or(RegistrationError::Limit)?;
            if !classes.insert(class.clone()) {
                return Err(RegistrationError::Invalid("duplicate Raft exported class"));
            }
        }
        for (name, class) in &options.bindings {
            super::class::validate_name(class)?;
            if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
                return Err(RegistrationError::Invalid("Raft cell binding name"));
            }
            bytes = bytes
                .checked_add(name.len())
                .and_then(|n| n.checked_add(class.len()))
                .ok_or(RegistrationError::Limit)?;
            if bindings.insert(name.clone(), class.clone()).is_some() {
                return Err(RegistrationError::Invalid("duplicate Raft cell binding"));
            }
        }
        let mut sql_bindings = BTreeMap::new();
        for (name, identity) in &options.sql_bindings {
            if name.is_empty()
                || name.len() > 256
                || name.chars().any(char::is_control)
                || identity.is_empty()
                || identity.len() > 1024
                || identity.chars().any(char::is_control)
            {
                return Err(RegistrationError::Invalid("Raft SQL binding"));
            }
            bytes = bytes
                .checked_add(name.len())
                .and_then(|n| n.checked_add(identity.len()))
                .ok_or(RegistrationError::Limit)?;
            if sql_bindings
                .insert(name.clone(), identity.clone())
                .is_some()
            {
                return Err(RegistrationError::Invalid("duplicate Raft SQL binding"));
            }
        }
        if bytes > 64 * 1024 {
            return Err(RegistrationError::Limit);
        }
        let valid_name = |name: &str| {
            !name.is_empty() && name.len() <= 256 && !name.chars().any(char::is_control)
        };
        let mut queue_bindings = BTreeMap::new();
        let mut kv_bindings = BTreeMap::new();
        for (name, identity) in &options.kv_bindings {
            if !valid_name(name)
                || identity.is_empty()
                || identity.len() > cellar_logic::cell_group::MAX_CELL_ID_BYTES - 2
                || identity.chars().any(char::is_control)
                || kv_bindings.insert(name.clone(), identity.clone()).is_some()
            {
                return Err(RegistrationError::Invalid("Raft KV binding"));
            }
            bytes = bytes
                .checked_add(name.len() + identity.len())
                .ok_or(RegistrationError::Limit)?;
        }
        for binding in &options.queue_bindings {
            if !valid_name(&binding.environment)
                || !cellar_logic::cell::valid_cell_scope(&binding.queue)
                || binding.queue.len() > 1024
                || binding.delivery_delay > cellar_logic::queue::Limits::default().max_delay_seconds
                || queue_bindings
                    .insert(
                        binding.environment.clone(),
                        (binding.queue.clone(), binding.delivery_delay),
                    )
                    .is_some()
            {
                return Err(RegistrationError::Invalid("Raft Queue binding"));
            }
            bytes = bytes
                .checked_add(binding.environment.len() + binding.queue.len() + 4)
                .ok_or(RegistrationError::Limit)?;
        }
        let mut queue_consumers = BTreeMap::new();
        for consumer in &options.queue_consumers {
            let config = cellar_logic::queue::QueueConfig {
                max_batch_size: consumer.max_batch_size,
                max_batch_timeout_seconds: consumer.max_batch_timeout,
                max_retries: consumer.max_retries,
                max_concurrency: consumer.max_concurrency,
                delivery_delay_seconds: 0,
                retry_delay_seconds: consumer.retry_delay,
            };
            if !cellar_logic::cell::valid_cell_scope(&consumer.queue)
                || consumer.queue.len() > 1024
                || consumer.dead_letter_queue.as_ref().is_some_and(|q| {
                    q == &consumer.queue
                        || q.len() > 1024
                        || !cellar_logic::cell::valid_cell_scope(q)
                })
                || cellar_logic::queue::validate_config_with_limits(
                    &config,
                    &cellar_logic::queue::Limits::default(),
                )
                .is_err()
                || queue_consumers
                    .insert(consumer.queue.clone(), consumer.clone())
                    .is_some()
            {
                return Err(RegistrationError::Invalid("Raft Queue consumer"));
            }
            bytes = bytes
                .checked_add(
                    serde_json::to_vec(consumer)
                        .expect("Queue configuration")
                        .len(),
                )
                .ok_or(RegistrationError::Limit)?;
        }
        let mut workflow_bindings = BTreeMap::new();
        let mut workflow_classes = BTreeMap::new();
        for binding in &options.workflow_bindings {
            if !valid_name(&binding.environment)
                || !valid_name(&binding.workflow)
                || !valid_name(&binding.class)
                || binding.settings.validate().is_err()
                || !binding.settings.schedules.is_empty()
            {
                return Err(RegistrationError::Invalid(
                    "Raft Workflow binding or schedule",
                ));
            }
            let admitted = NativeWorkflowBinding {
                environment: binding.environment.clone(),
                workflow: binding.workflow.clone(),
                class: binding.class.clone(),
                settings: binding.settings.clone(),
            };
            let identity = serde_json::to_vec(&(&admitted.class, &admitted.settings))
                .map_err(|_| RegistrationError::Invalid("Workflow configuration"))?;
            if workflow_classes
                .insert(binding.workflow.clone(), identity.clone())
                .is_some_and(|old| old != identity)
                || workflow_bindings
                    .insert(binding.environment.clone(), admitted)
                    .is_some()
            {
                return Err(RegistrationError::Invalid(
                    "conflicting Raft Workflow binding",
                ));
            }
            bytes = bytes
                .checked_add(binding.environment.len() + binding.workflow.len() + identity.len())
                .ok_or(RegistrationError::Limit)?;
        }
        if bytes > 64 * 1024 {
            return Err(RegistrationError::Limit);
        }
        Ok(Self {
            workflow_bindings: Arc::new(workflow_bindings),
            script: options.script_name.clone(),
            vars: Arc::new(vars),
            compat: options.compat,
            classes: Arc::new(classes),
            bindings: Arc::new(bindings),
            sql_bindings: Arc::new(sql_bindings),
            kv_bindings: Arc::new(kv_bindings),
            queue_bindings: Arc::new(queue_bindings),
            queue_consumers: Arc::new(queue_consumers),
        })
    }

    pub(in crate::application) fn digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"cellar-raft-cell-configuration-1");
        // BTree order and all compatibility switches participate, independent
        // of declaration order. Serialization of these bounded values is total.
        hash.update(
            serde_json::to_vec(&(&self.script, self.vars.as_ref(), self.compat))
                .expect("serializable cell configuration"),
        );
        if self.has_cell_bindings() {
            hash.update(b"cell-bindings-claimed-recovery-1");
            hash.update(
                serde_json::to_vec(&(self.classes.as_ref(), self.bindings.as_ref()))
                    .expect("serializable cell bindings"),
            );
        }
        if !self.sql_bindings.is_empty() {
            hash.update(b"sql-bindings-1");
            hash.update(
                serde_json::to_vec(self.sql_bindings.as_ref()).expect("serializable SQL bindings"),
            );
        }
        if !self.queue_bindings.is_empty() || !self.queue_consumers.is_empty() {
            hash.update(b"queue-bindings-callbacks-1");
            hash.update(
                serde_json::to_vec(&(self.queue_bindings.as_ref(), self.queue_consumers.as_ref()))
                    .expect("Queue configuration"),
            );
        }
        if !self.kv_bindings.is_empty() {
            hash.update(b"kv-bindings-payloads-callbacks-1");
            hash.update(serde_json::to_vec(self.kv_bindings.as_ref()).expect("KV configuration"));
        }
        if !self.workflow_bindings.is_empty() {
            hash.update(b"workflow-bindings-quorum-journal-1");
            hash.update(
                serde_json::to_vec(self.workflow_bindings.as_ref())
                    .expect("Workflow configuration"),
            );
        }
        hash.finalize().into()
    }

    pub(in crate::application) fn has_cell_bindings(&self) -> bool {
        !self.classes.is_empty()
            || !self.bindings.is_empty()
            || !self.sql_bindings.is_empty()
            || !self.kv_bindings.is_empty()
            || !self.queue_bindings.is_empty()
            || !self.queue_consumers.is_empty()
            || !self.workflow_bindings.is_empty()
    }

    pub(in crate::application) fn options(&self) -> WorkerConfigOptions {
        NativeCellConfiguration {
            workflow_bindings: self.workflow_bindings.values().cloned().collect(),
            vars: self
                .vars
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            compat: self.compat,
            do_classes: self.classes.iter().cloned().collect(),
            bindings: self
                .bindings
                .iter()
                .map(|(name, class)| (name.clone(), class.clone()))
                .collect(),
            queue_bindings: self
                .queue_bindings
                .iter()
                .map(
                    |(environment, (queue, delivery_delay))| NativeQueueBinding {
                        environment: environment.clone(),
                        queue: queue.clone(),
                        delivery_delay: *delivery_delay,
                    },
                )
                .collect(),
            queue_consumers: self.queue_consumers.values().cloned().collect(),
            crons: Vec::new(),
            cron_history: None,
            sql_bindings: self
                .sql_bindings
                .iter()
                .map(|(name, identity)| (name.clone(), identity.clone()))
                .collect(),
            kv_bindings: self
                .kv_bindings
                .iter()
                .map(|(name, identity)| (name.clone(), identity.clone()))
                .collect(),
        }
        .options(&self.script)
    }
}
