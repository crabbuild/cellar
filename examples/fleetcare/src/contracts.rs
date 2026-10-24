// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Shared participant and egress identity for an order within its authority.
/// The bounded digest lets maximum-length public order names remain valid
/// participant command IDs without colliding across books or tenants.
pub fn assignment_id(tenant: usize, book: usize, order: &str) -> String {
    format!("{:x}", Sha256::digest(format!("{tenant}/{book}/{order}")))
}

pub fn reservation_id(tenant: usize, book: usize, order: &str) -> String {
    format!("order-{}", assignment_id(tenant, book, order))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub command_id: String,
    pub action: String,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub expected_version: Option<u64>,
}

impl Command {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.command_id.is_empty()
                && self.command_id.len() <= 128
                && self
                    .command_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)),
            "invalid command ID"
        );
        ensure!(self.action.len() <= 64, "invalid action");
        ensure!(
            serde_json::to_vec(&self.payload)?.len() <= 65_536,
            "payload exceeds 64 KiB"
        );
        Ok(())
    }

    pub fn digest(&self, scope: &str) -> Result<String> {
        self.validate()?;
        let bytes = serde_json::to_vec(&json!({
            "protocol": 1, "scope": scope, "action": self.action,
            "expected_version": self.expected_version, "payload": self.payload
        }))?;
        Ok(format!("{:x}", Sha256::digest(bytes)))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Receipt {
    pub command_id: String,
    pub digest: String,
    pub ordinal: u64,
    pub version: u64,
    pub accepted: bool,
    pub result: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Effect {
    pub effect_id: String,
    pub tenant: usize,
    pub source_scope: String,
    pub kind: String,
    pub version: u64,
    pub snapshot: Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Open,
    Reserving,
    Reserved,
    Dispatched,
    InProgress,
    Completed,
    Compensating,
    Cancelled,
    NeedsReview,
}

impl OrderStatus {
    pub fn transition(self, next: Self) -> Result<Self> {
        use OrderStatus::*;
        ensure!(
            matches!(
                (self, next),
                (Open, Reserving)
                    | (Reserving, Reserved)
                    | (Reserved, Dispatched)
                    | (Dispatched, InProgress)
                    | (InProgress, Completed)
                    | (
                        Open | Reserving | Reserved | Dispatched | InProgress,
                        Compensating
                    )
                    | (Compensating, Cancelled | NeedsReview)
            ),
            "invalid order transition"
        );
        Ok(next)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Order {
    pub order_id: String,
    pub status: OrderStatus,
    pub inventory_id: usize,
    pub reservation_id: String,
    pub version: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReservationStatus {
    Held,
    Consumed,
    Released,
    Expired,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Reservation {
    pub id: String,
    pub qty: u64,
    pub status: ReservationStatus,
    pub expires_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn participant_identity_is_stable_and_scoped_to_tenant_and_book() {
        let order = "x".repeat(128);
        let identity = assignment_id(0, 0, &order);
        assert_eq!(identity, assignment_id(0, 0, &order));
        assert_ne!(identity, assignment_id(1, 0, &order));
        assert_ne!(identity, assignment_id(0, 1, &order));
        assert!(reservation_id(0, 0, &order).len() <= 128);
    }

    #[test]
    fn terminal_orders_cannot_consume_or_restart() {
        assert!(OrderStatus::Completed
            .transition(OrderStatus::InProgress)
            .is_err());
        assert!(OrderStatus::Cancelled
            .transition(OrderStatus::Reserving)
            .is_err());
        assert!(OrderStatus::Reserved
            .transition(OrderStatus::Completed)
            .is_err());
        assert_eq!(
            OrderStatus::Compensating
                .transition(OrderStatus::NeedsReview)
                .unwrap(),
            OrderStatus::NeedsReview
        );
    }

    #[test]
    fn digest_binds_identity_expected_version_and_canonical_payload() {
        let a = Command {
            command_id: "one".into(),
            action: "reserve".into(),
            payload: json!({"qty": 1, "id": "r"}),
            expected_version: None,
        };
        let mut b = a.clone();
        b.payload = serde_json::from_str(r#"{"id":"r","qty":1}"#).unwrap();
        assert_eq!(a.digest("cell-a").unwrap(), b.digest("cell-a").unwrap());
        assert_ne!(a.digest("cell-a").unwrap(), a.digest("cell-b").unwrap());
        b.expected_version = Some(1);
        assert_ne!(a.digest("cell-a").unwrap(), b.digest("cell-a").unwrap());
    }
}
