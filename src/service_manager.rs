use crate::{metrics_store::MetricsStoreService, udp_server::UdpServerService};
use anyhow::{Result, bail};
use log::info;
use std::time::Duration;
use tokio::{spawn, time::sleep, try_join};

pub struct ServiceManagerConfig {
    max_retries: u8,
    retry_delay: Duration,
}

impl ServiceManagerConfig {
    pub fn new(max_retries: u8, retry_delay: Duration) -> Self {
        Self {
            max_retries,
            retry_delay,
        }
    }
}

impl Default for ServiceManagerConfig {
    fn default() -> Self {
        Self::new(5, Duration::from_secs_f64(0.5))
    }
}

pub struct ServiceManager<M, U>
where
    M: MetricsStoreService,
    U: UdpServerService,
{
    metrics_store: M,
    udp_server: U,
    config: ServiceManagerConfig,
}

impl<M, U> ServiceManager<M, U>
where
    M: MetricsStoreService,
    U: UdpServerService,
{
    pub fn new(metrics_store: M, udp_server: U, config: ServiceManagerConfig) -> Self {
        Self {
            metrics_store,
            udp_server,
            config,
        }
    }

    /// Start the required services.
    /// The metrics store should be started before everything else because
    ///   it's required for reading and writing metrics.
    pub async fn start_services(&mut self) -> Result<()> {
        info!("Starting metrics store...");
        let metrics_store_handle = spawn(self.metrics_store.service());

        let mut retry_counter: u8 = 0;
        while !self.metrics_store.is_ready() {
            if retry_counter >= self.config.max_retries {
                metrics_store_handle.abort();
                bail!("Failed to start metrics store.");
            }
            sleep(self.config.retry_delay).await;
            retry_counter += 1;
        }

        info!("Metrics store is ready, starting UDP server...");
        let udp_server_handle = spawn(self.udp_server.service());

        let mut retry_counter: u8 = 0;
        while !self.udp_server.is_ready() {
            if retry_counter >= self.config.max_retries {
                udp_server_handle.abort();
                bail!("Failed to start UDP server.");
            }
            sleep(self.config.retry_delay).await;
            retry_counter += 1;
        }

        info!("All services started!");
        try_join!(metrics_store_handle, udp_server_handle).map(|_| ())?;

        Ok(())
    }
}
