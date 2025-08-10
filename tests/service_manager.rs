use anyhow::{Result, bail};
use statsdfusion::{
    Service, metrics_store::MetricsStoreService, service_manager::ServiceManagerConfig,
    udp_server::UdpServerService,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::sleep;

const STANDARD_STARTUP_DELAY_SEC: f64 = 0.01;

struct TestMetricsStore {
    is_ready: Arc<AtomicBool>,
    should_fail: bool,
    startup_delay: Duration,
}

impl TestMetricsStore {
    pub fn new(should_fail: bool, startup_delay: Duration) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            should_fail,
            startup_delay,
        }
    }

    pub fn new_working() -> Self {
        Self::new(false, Duration::from_secs_f64(STANDARD_STARTUP_DELAY_SEC))
    }

    pub fn new_failing() -> Self {
        Self::new(true, Duration::from_secs_f64(STANDARD_STARTUP_DELAY_SEC))
    }
}

impl Service for TestMetricsStore {
    fn service(&mut self) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let should_fail = self.should_fail;
        let startup_delay = self.startup_delay;

        async move {
            sleep(startup_delay).await;

            if !should_fail {
                is_ready.store(true, Ordering::SeqCst);
                loop {
                    sleep(Duration::from_secs(1)).await;
                }
            } else {
                bail!("failure");
            }
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

impl MetricsStoreService for TestMetricsStore {}

struct TestUdpServer {
    is_ready: Arc<AtomicBool>,
    should_fail: bool,
    startup_delay: Duration,
}

impl TestUdpServer {
    pub fn new(should_fail: bool, startup_delay: Duration) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            should_fail,
            startup_delay,
        }
    }

    pub fn new_working() -> Self {
        Self::new(false, Duration::from_secs_f64(STANDARD_STARTUP_DELAY_SEC))
    }

    pub fn new_failing() -> Self {
        Self::new(true, Duration::from_secs_f64(STANDARD_STARTUP_DELAY_SEC))
    }
}

impl Service for TestUdpServer {
    fn service(&mut self) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let should_fail = self.should_fail;
        let startup_delay = self.startup_delay;

        async move {
            sleep(startup_delay).await;

            if !should_fail {
                is_ready.store(true, Ordering::SeqCst);
                loop {
                    sleep(Duration::from_secs(1)).await;
                }
            } else {
                bail!("failure");
            }
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

impl UdpServerService for TestUdpServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use statsdfusion::service_manager::ServiceManager;
    use tokio::time::{error::Elapsed, timeout};

    fn service_manager_config() -> ServiceManagerConfig {
        ServiceManagerConfig::new(3, Duration::from_millis(10))
    }

    /// Both services should start up and run "indefinitely".
    /// For the purposes of this test, assume that "running indefinitely"
    ///   means that the services run for some amount of time without failing
    #[tokio::test]
    async fn test_both_services_start() {
        const INDEFINITE_RUN_SEC: f64 = 0.5;
        let metrics_store = TestMetricsStore::new_working();
        let udp_server = TestUdpServer::new_working();

        let mut service_manager =
            ServiceManager::new(metrics_store, udp_server, service_manager_config());

        let result = timeout(
            Duration::from_secs_f64(INDEFINITE_RUN_SEC),
            service_manager.start_services(),
        )
        .await;

        // If there was a timeout, that means the services successfully started.
        // Otherwise, there would be some other error.
        assert!(matches!(result, Err(Elapsed { .. })));
    }

    /// Should error if the UDP Server does not start after the Metrics Store
    #[tokio::test]
    async fn test_metrics_store_starts_but_udp_server_fails() {
        let metrics_store = TestMetricsStore::new_working();
        let udp_server = TestUdpServer::new_failing();

        let mut service_manager =
            ServiceManager::new(metrics_store, udp_server, service_manager_config());

        let result = service_manager.start_services().await;

        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start UDP server"));
    }

    /// Should error if the Metrics Store does not start immediately
    #[tokio::test]
    async fn test_metrics_store_fails_to_start() {
        let metrics_store = TestMetricsStore::new_failing();
        let udp_server = TestUdpServer::new_working();

        let mut service_manager =
            ServiceManager::new(metrics_store, udp_server, service_manager_config());

        let result = service_manager.start_services().await;

        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start metrics store"));
    }

    /// Should error if the Metrics Store does not start before the max retry count has been reached
    #[tokio::test]
    async fn test_metrics_store_insufficient_retries_for_slow_startup() {
        let metrics_store = TestMetricsStore::new(false, Duration::from_millis(200)); // startup delay > total retry duration
        let udp_server = TestUdpServer::new_working();

        let mut service_manager =
            ServiceManager::new(metrics_store, udp_server, service_manager_config());

        let result = service_manager.start_services().await;

        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start metrics store"));
    }
}
