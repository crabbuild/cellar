#![doc = include_str!("README.md")]

pub mod blob;
pub mod cron;
pub mod kv;
pub mod limits;
pub mod queue;
pub mod sql;
pub mod workflow;

/// An owned application callback; the host supplies its execution and lifetime.
pub type HandlerFuture<T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<T>> + Send + 'static>>;
