use anyhow::Result;
use std::future::Future;

pub mod cli;
pub mod metric;
pub mod metrics_store;
pub mod service_manager;
pub mod udp_server;

/// Defines the behavior of a service when spawned.
/// Services can have marker traits that depend on this trait so that
///   service types can be enforced at compile-time.
pub trait Service {
    fn service(&mut self) -> impl Future<Output = Result<()>> + Send + 'static;
    fn is_ready(&self) -> bool;
}
