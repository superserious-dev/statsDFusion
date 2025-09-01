use crate::{
    http_server::HttpServerService, metrics_store::MetricsStoreService,
    udp_server::UdpServerService,
};
use anyhow::{Result, bail};
use futures::{FutureExt, future::select_all};
use log::info;
use std::time::Duration;
use tokio::{select, signal, task::JoinHandle, time::sleep};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

#[derive(Debug)]
pub struct StartupConfig {
    max_retries: u8,
    retry_delay: Duration,
}

impl StartupConfig {
    pub fn new(max_retries: u8, retry_delay: Duration) -> Self {
        Self {
            max_retries,
            retry_delay,
        }
    }
}

impl Default for StartupConfig {
    fn default() -> Self {
        Self::new(5, Duration::from_secs_f64(0.5))
    }
}

#[derive(Debug)]
pub struct RunningServices {
    pub cancellation_token: CancellationToken,
    pub tracker: TaskTracker,
    pub tasks: Vec<JoinHandle<Result<()>>>,
}

impl RunningServices {
    pub async fn wait_until_shutdown(self) -> Result<()> {
        let RunningServices {
            cancellation_token,
            tracker,
            tasks,
        } = self;

        let result = select! {
            _ = signal::ctrl_c() => {
                info!("CTRL-C signal received... shutting down");
                cancellation_token.cancel();
                tracker.close();
                Ok(())
            },
            (result, _index, _remaining_tasks) = select_all(tasks).fuse()
            => {
                info!("A task has shut down early. Shutting down remaining tasks...");

                // Initiate the shut down of remaining tasks
                cancellation_token.cancel();
                tracker.close();

                // Capture why the task shut down
                match result {
                    Ok(Ok(())) => {
                        // Task exited successfully
                        Ok(())
                    },
                    Ok(Err(e)) => {
                        // Task failed with a handled Err
                        Err(e)
                    },
                    Err(join_error) => {
                        // Task panicked
                        Err(join_error.into())
                    }
                }
            }
        };

        // Wait until all tasks have shut down and return the cause
        tracker.wait().await;
        result
    }
}

/// Starts the required services and waits for them to be ready.
/// The metrics store should be started before everything else because
///   it's required for reading and writing metrics.
pub async fn start_services<M, U, H>(
    metrics_store: &mut M,
    udp_server: &mut U,
    http_server: &mut H,
    config: StartupConfig,
) -> Result<RunningServices>
where
    M: MetricsStoreService,
    U: UdpServerService,
    H: HttpServerService,
{
    let cancellation_token = CancellationToken::new();
    let tracker = TaskTracker::new();

    info!("Starting metrics store...");
    let metrics_store_task = tracker.spawn(metrics_store.service(cancellation_token.clone()));

    let mut retry_counter: u8 = 0;
    while !metrics_store.is_ready() {
        if retry_counter >= config.max_retries {
            cancellation_token.cancel();
            bail!("Failed to start metrics store.");
        }
        sleep(config.retry_delay).await;
        retry_counter += 1;
    }

    info!("Metrics store is ready, starting UDP server...");
    let udp_server_task = tracker.spawn(udp_server.service(cancellation_token.clone()));

    let mut retry_counter: u8 = 0;
    while !udp_server.is_ready() {
        if retry_counter >= config.max_retries {
            cancellation_token.cancel();
            bail!("Failed to start UDP server.");
        }
        sleep(config.retry_delay).await;
        retry_counter += 1;
    }

    info!("UDP server is ready, starting Http server...");
    let http_server_task = tracker.spawn(http_server.service(cancellation_token.clone()));

    let mut retry_counter: u8 = 0;
    while !http_server.is_ready() {
        if retry_counter >= config.max_retries {
            cancellation_token.cancel();
            bail!("Failed to start Http server.");
        }
        sleep(config.retry_delay).await;
        retry_counter += 1;
    }

    info!("All services are ready!");
    Ok(RunningServices {
        cancellation_token,
        tracker,
        tasks: vec![metrics_store_task, udp_server_task, http_server_task],
    })
}
