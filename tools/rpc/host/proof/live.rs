// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe and resume only the original handler retained across a real fence.
use super::{request, Composite, Controls, Returned};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{sync::atomic::Ordering, time::Duration};

pub(super) struct Watch<'a> {
    pub app: &'a Composite,
    pub client: &'a reqwest::Client,
    pub url: &'a str,
    pub provider: &'a str,
    pub class: &'a str,
    pub id: &'a str,
    pub expected: i64,
    pub delta: i64,
    pub invoked_cursor: usize,
    pub returned_cursor: usize,
}

impl Watch<'_> {
    async fn snapshot(&self) -> Result<Value> {
        request(self.client, self.url, &format!("/{}/probe", self.provider)).await
    }

    pub async fn waiting(
        &self,
        controls: &Controls,
        peer_before: usize,
        data_before: usize,
    ) -> Result<(Returned, Value)> {
        let expected_text = self.expected.to_string();
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                ensure!(
                    self.app
                        .audit
                        .returned_since(self.returned_cursor)
                        .is_empty(),
                    "sync handler returned before its proof was released"
                );
                let snapshot = self.snapshot().await?;
                if snapshot["ready"] == true
                    && snapshot["committed"] == true
                    && snapshot["waiting"] == true
                    && controls.peers.entered.load(Ordering::Acquire) > peer_before
                    && controls.data.entered.load(Ordering::Acquire) > data_before
                {
                    ensure!(
                        snapshot["id"] == self.id
                            && snapshot["active_before"] == true
                            && snapshot["cancelled"] == false
                            && snapshot["late_done"] == false,
                        "live probe was already expired or observed another command"
                    );
                    ensure!(
                        snapshot["count"].as_str() == Some(expected_text.as_str())
                            && snapshot["receipt"]
                                == json!({
                                    "delta":self.delta.to_string(),
                                    "value":self.expected.to_string()
                                }),
                        "live handler did not observe its atomic count/receipt batch"
                    );
                    let entries: Vec<_> = self
                        .app
                        .audit
                        .since(self.invoked_cursor)
                        .into_iter()
                        .filter(|visit| visit.class.is_some())
                        .collect();
                    ensure!(entries.len() == 1, "live command entered multiple cells");
                    let visit = entries.into_iter().next().unwrap();
                    ensure!(
                        visit.node == "proof-owner"
                            && visit.class.as_deref() == Some(self.class)
                            && visit.method == "example.counter.v1.Counter/Increment"
                            && snapshot["node"] == visit.node
                            && snapshot["class"] == self.class
                            && snapshot["scope"].as_str() == visit.scope.as_deref()
                            && snapshot["epoch"]
                                .as_str()
                                .and_then(|value| value.parse::<u64>().ok())
                                == visit.epoch,
                        "live guest metadata differs from the actual admitted native cell"
                    );
                    return Ok((
                        Returned {
                            visit,
                            count_matching_storage: Some(self.expected.to_string()),
                            transaction_open: None,
                            write_position: None,
                        },
                        snapshot,
                    ));
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("original handler did not suspend with its live context in sync")?
    }

    pub async fn resume(&self) -> Result<Value> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = self.snapshot().await?;
                ensure!(
                    snapshot["id"] == self.id,
                    "live probe changed command during fencing"
                );
                if snapshot["cancelled"] == true {
                    ensure!(
                        snapshot["waiting"] == false && snapshot["late_done"] == false,
                        "live handler resumed before its controlled release"
                    );
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("fencing did not revoke/cancel the original live handler")??;
        request(
            self.client,
            self.url,
            &format!("/{}/probe-release", self.provider),
        )
        .await?;
        let snapshot = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let snapshot = self.snapshot().await?;
                if snapshot["late_done"] == true {
                    return Ok::<_, anyhow::Error>(snapshot);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("old live handler did not finish after controlled resume")??;
        ensure!(
            snapshot["id"] == self.id
                && snapshot["cancelled"] == true
                && snapshot["storage_refused"] == true
                && snapshot["read_refused"] == true
                && snapshot["call_refused"] == true,
            "retired live authority admitted storage or typed RPC work: {snapshot}"
        );
        if self.provider == "python" {
            ensure!(
                snapshot["raw_refused"] == true && snapshot["bridge_inactive"] == true,
                "Python's native sender retained authority after its owner fenced"
            );
        }
        if self.provider == "rust" {
            ensure!(
                snapshot["authority_revoked"] == true,
                "native authority was not revoked independently of the control invocation"
            );
        }
        ensure!(
            self.app
                .audit
                .since(self.invoked_cursor)
                .iter()
                .filter(|visit| visit.method == "example.counter.v1.Counter/Increment")
                .count()
                == 1,
            "resumed stale context submitted a business RPC"
        );
        ensure!(
            request(
                self.client,
                self.url,
                &format!("/{}/probe-clear", self.provider)
            )
            .await?
                == json!({"cleared":true}),
            "live probe did not release retained contexts"
        );
        Ok(snapshot)
    }
}
