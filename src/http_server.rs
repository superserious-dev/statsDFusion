use crate::Service;
use anyhow::{Result, anyhow};
use axum::{Router, routing::get};

use log::info;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::select;
use tokio_util::sync::CancellationToken;

pub struct HttpServer {
    is_ready: Arc<AtomicBool>,
    http_port: u16,
}

impl HttpServer {
    pub fn new(http_port: u16) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            http_port,
        }
    }

    pub fn serve(&self) {}
}

impl Service for HttpServer {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let http_port = self.http_port;

        async move {
            let socket_addr = SocketAddr::from(([127, 0, 0, 1], http_port));
            let listener = tokio::net::TcpListener::bind(socket_addr).await?;
            let router = Router::new().route("/", get(|| async { "Hello, World!" }));

            info!("Starting Http server @ {socket_addr}");

            let server_future = axum::serve(listener, router);

            is_ready.store(true, Ordering::SeqCst);
            select! {
                _ = cancellation_token.cancelled() => {
                    info!("Shutting down Http server...");
                }
                result = server_future => {
                    match result {
                        Ok(()) => info!("Http server ran successfully"),
                        Err(e) => return Err(anyhow!("Http server failure: {}", e)),
                    }
                }
            }

            Ok(())
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

/// Service type marker trait
pub trait HttpServerService: Service {}
impl HttpServerService for HttpServer {}
