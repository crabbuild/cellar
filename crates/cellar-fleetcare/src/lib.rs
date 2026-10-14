//! A native field-service application shared by embedded and daemon hosts.
mod application;
pub mod assets;
mod asynchronous;
mod attachments;
pub mod config;
pub mod contracts;
pub mod deployment;
mod diagnostics;
pub mod embedded;
pub mod state;
mod subscriptions;

pub use application::application;
