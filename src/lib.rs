#![forbid(unsafe_code)]

pub mod client;
pub mod command;
pub mod config;
pub mod error;
pub mod metrics;
pub mod persistence;
pub mod protocol;
pub mod server;
pub mod store;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
