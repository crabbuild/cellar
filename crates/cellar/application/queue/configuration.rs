//! Queue consumer bindings and broker settings captured by a deployment.
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerConfig {
    pub consumers: HashMap<String, ConsumerConfig>,
    pub lease_duration_ms: i64,
    pub limits: BrokerLimits,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerConfig {
    pub script: String,
    pub max_batch_size: usize,
    pub max_batch_timeout_ms: i64,
    pub max_retries: u16,
    pub max_concurrency: Option<u16>,
    pub retry_delay_seconds: Option<u32>,
    pub dead_letter_queue: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerLimits {
    pub producer_group_ms: u64,
    pub max_concurrency: u16,
    pub retention_ms: Option<i64>,
    pub max_admin_batch_size: usize,
    pub max_message_bytes: usize,
    pub max_batch_bytes: usize,
    pub max_batch_messages: u16,
    pub sweep_batch_rows: usize,
}
