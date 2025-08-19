use anyhow::Result;
use std::future::Future;
use tokio_util::sync::CancellationToken;

pub mod cli;
pub mod metric;
pub mod metrics_store;
pub mod redb_table_provider;
pub mod startup;
pub mod udp_server;

/// Defines the behavior of a service when spawned.
/// Services can have marker traits that depend on this trait so that
///   service types can be enforced at compile-time.
pub trait Service {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static;
    fn is_ready(&self) -> bool;
}
