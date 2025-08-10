use crate::{Service, metrics_store};
use anyhow::{Context as _, Result};
use chrono::{Timelike as _, Utc};
use log::info;
use std::time::Duration;
use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{net::UdpSocket, select, sync::mpsc::UnboundedSender, task::spawn_blocking, time};

const MAX_RECEIVE_BUFFER_BYTES: usize = 10_000;

pub struct UdpServer {
    is_ready: Arc<AtomicBool>,
    port: u16,
    flush_interval: u64,
    metrics_store_tx: UnboundedSender<metrics_store::Message>,
}

impl UdpServer {
    pub fn new(
        port: u16,
        flush_interval: u64,
        metrics_store_tx: UnboundedSender<metrics_store::Message>,
    ) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            port,
            flush_interval,
            metrics_store_tx,
        }
    }
}

impl Service for UdpServer {
    fn service(&mut self) -> impl Future<Output = Result<()>> + Send + 'static {
        let port = self.port;
        let is_ready = Arc::clone(&self.is_ready);
        let mut flush_interval = time::interval(Duration::from_secs(self.flush_interval));

        async move {
            let socket_addr = SocketAddr::from(([127, 0, 0, 1], port));
            let socket = UdpSocket::bind(socket_addr).await?;
            info!(
                "UDP server listening on {}:{}",
                socket_addr.ip(),
                socket_addr.port()
            );

            let mut buf = [0; MAX_RECEIVE_BUFFER_BYTES];
            is_ready.store(true, Ordering::SeqCst);
            loop {
                select! {
                    // Receive bytes from UDP
                    Ok((len, _addr)) = socket.recv_from(&mut buf)
                    => {
                        // Convert bytes to string
                        let message = str::from_utf8(&buf[..len])?;

                        info!("Received message: {message:#?}");
                    }

                    _ = flush_interval.tick() => {
                        let flushed_at = Utc::now().with_nanosecond(0).context("Failed to truncate flush datetime to seconds-level precision")?;
                        info!("Flush @ {flushed_at}");

                        // Spawn a new task for sending data to the Metrics Store without blocking the receipt of UDP packets
                        spawn_blocking(move || {
                            info!("Sending metrics to the Metrics Store");
                        });
                    }
                }
            }
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

/// Service type marker trait
pub trait UdpServerService: Service {}
impl UdpServerService for UdpServer {}
