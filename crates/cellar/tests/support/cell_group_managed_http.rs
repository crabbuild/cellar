// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A joined application-owned Axum listener for managed SDK qualifications.

use super::*;
use std::sync::Mutex;

#[derive(Default)]
pub struct Owner(Mutex<Vec<tokio::task::JoinHandle<std::io::Result<()>>>>);

impl Owner {
    pub fn serve(&self, listener: tokio::net::TcpListener, ingress: IngressHandle) {
        let router =
            axum::Router::new().fallback_service(cellar_axum::CellarService::new(ingress.clone()));
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    while !ingress.is_closed() {
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
        });
        self.0.lock().unwrap().push(task);
    }

    pub async fn close(&self) -> Result<()> {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        let mut failures = Vec::new();
        for mut task in tasks {
            let joined = tokio::time::timeout(TIMEOUT, &mut task).await;
            match joined {
                Ok(Ok(Ok(()))) => (),
                Ok(result) => failures.push(format!("Axum SQL host: {result:?}")),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    failures.push("Axum SQL host did not join after node closure".into());
                }
            }
        }
        ensure!(failures.is_empty(), "{}", failures.join("; "));
        Ok(())
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        for task in self.0.get_mut().unwrap().drain(..) {
            task.abort();
        }
    }
}
