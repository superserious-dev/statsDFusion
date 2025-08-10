use crate::Service;
use anyhow::Result;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::mpsc::UnboundedReceiver;

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
    fn service(&mut self) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let data_dir = self.data_dir.to_string_lossy().to_string();
        async move {
            is_ready.store(true, Ordering::SeqCst);
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                println!("data dir is: `{data_dir}`");
            }
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

/// Service type marker trait
pub trait MetricsStoreService: Service {}
impl MetricsStoreService for MetricsStore {}
