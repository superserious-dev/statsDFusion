use anyhow::{Result, bail};
use statsdfusion::{
    Service, http_server::HttpServerService, metrics_store::MetricsStoreService,
    startup::StartupConfig, udp_server::UdpServerService,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;

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
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let should_fail = self.should_fail;
        let startup_delay = self.startup_delay;

        async move {
            sleep(startup_delay).await;

            if !should_fail {
                is_ready.store(true, Ordering::SeqCst);
                loop {
                    select! {
                       _ = cancellation_token.cancelled() => {
                           break Ok(())
                       }
                       _ = sleep(Duration::from_secs(1)) => {

                       }
                    }
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
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let should_fail = self.should_fail;
        let startup_delay = self.startup_delay;

        async move {
            sleep(startup_delay).await;

            if !should_fail {
                is_ready.store(true, Ordering::SeqCst);
                sleep(Duration::from_secs(1)).await;
                cancellation_token.cancel(); // Cancel without error after one iteration
                Ok(())
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

struct TestHttpServer {
    is_ready: Arc<AtomicBool>,
    should_fail: bool,
    startup_delay: Duration,
}

impl TestHttpServer {
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

impl Service for TestHttpServer {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let should_fail = self.should_fail;
        let startup_delay = self.startup_delay;

        async move {
            sleep(startup_delay).await;

            if !should_fail {
                is_ready.store(true, Ordering::SeqCst);
                sleep(Duration::from_secs(1)).await;
                cancellation_token.cancel(); // Cancel without error after one iteration
                Ok(())
            } else {
                bail!("failure");
            }
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

impl HttpServerService for TestHttpServer {}

#[cfg(test)]
mod tests {
    use super::*;
    use statsdfusion::startup::start_services;

    fn startup_config() -> StartupConfig {
        StartupConfig::new(3, Duration::from_millis(10))
    }

    /// For the purposes of this test, the services should start up, then gracefully
    /// shutdown and return with no errors.
    #[tokio::test]
    async fn test_both_services_start() {
        let mut metrics_store = TestMetricsStore::new_working();
        let mut udp_server = TestUdpServer::new_working();
        let mut http_server = TestHttpServer::new_working();

        let running_services = start_services(
            &mut metrics_store,
            &mut udp_server,
            &mut http_server,
            startup_config(),
        )
        .await
        .unwrap();
        let result = running_services.wait_until_shutdown().await;
        assert!(matches!(result, Ok(())));
    }

    /// Should error if the UDP Server does not start after the Metrics Store
    #[tokio::test]
    async fn test_metrics_store_starts_but_udp_server_fails() {
        let mut metrics_store = TestMetricsStore::new_working();
        let mut udp_server = TestUdpServer::new_failing();
        let mut http_server = TestHttpServer::new_working();

        let running_services = start_services(
            &mut metrics_store,
            &mut udp_server,
            &mut http_server,
            startup_config(),
        )
        .await;

        assert!(running_services.is_err());
        let error_message = running_services.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start UDP server"));
    }

    /// Should error if the Http Server does not start after the Udp Server
    #[tokio::test]
    async fn test_udp_server_starts_but_http_server_fails() {
        let mut metrics_store = TestMetricsStore::new_working();
        let mut udp_server = TestUdpServer::new_working();
        let mut http_server = TestHttpServer::new_failing();

        let running_services = start_services(
            &mut metrics_store,
            &mut udp_server,
            &mut http_server,
            startup_config(),
        )
        .await;

        assert!(running_services.is_err());
        let error_message = running_services.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start Http server"));
    }

    /// Should error if the Metrics Store does not start immediately
    #[tokio::test]
    async fn test_metrics_store_fails_to_start() {
        let mut metrics_store = TestMetricsStore::new_failing();
        let mut udp_server = TestUdpServer::new_working();
        let mut http_server = TestHttpServer::new_working();

        let running_services = start_services(
            &mut metrics_store,
            &mut udp_server,
            &mut http_server,
            startup_config(),
        )
        .await;

        assert!(running_services.is_err());
        let error_message = running_services.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start metrics store"));
    }

    /// Should error if the Metrics Store does not start before the max retry count has been reached
    #[tokio::test]
    async fn test_metrics_store_insufficient_retries_for_slow_startup() {
        let mut metrics_store = TestMetricsStore::new(false, Duration::from_millis(200)); // startup delay > total retry duration
        let mut udp_server = TestUdpServer::new_working();
        let mut http_server = TestHttpServer::new_working();

        let running_services = start_services(
            &mut metrics_store,
            &mut udp_server,
            &mut http_server,
            startup_config(),
        )
        .await;

        assert!(running_services.is_err());
        let error_message = running_services.unwrap_err().to_string();
        assert!(error_message.contains("Failed to start metrics store"));
    }
}
