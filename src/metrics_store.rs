use crate::Service;
use anyhow::Result;
use log::info;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{select, sync::mpsc::UnboundedReceiver};
use tokio_util::sync::CancellationToken;

pub enum Message {}

pub struct MetricsStore {
    is_ready: Arc<AtomicBool>,
    data_dir: PathBuf,
    rx: UnboundedReceiver<Message>,
}

impl MetricsStore {
    pub fn new(data_dir: PathBuf, rx: UnboundedReceiver<Message>) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            data_dir,
            rx,
        }
    }
}

impl Service for MetricsStore {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let data_dir = self.data_dir.to_string_lossy().to_string();
        async move {
            is_ready.store(true, Ordering::SeqCst);
            select! {
                _ = cancellation_token.cancelled() => {
                    info!("Shutting down MetricsStore...");
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
pub trait MetricsStoreService: Service {}
impl MetricsStoreService for MetricsStore {}
