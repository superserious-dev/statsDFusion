use crate::Service;
use anyhow::Result;
use arrow::ipc::reader::StreamReader;
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

#[derive(Debug)]
pub enum Message {
    Counters(Vec<u8>),
    Gauges(Vec<u8>),
}

pub struct MetricsStore {
    is_ready: Arc<AtomicBool>,
    data_dir: PathBuf,
    rx: Option<UnboundedReceiver<Message>>,
}

impl MetricsStore {
    pub fn new(data_dir: PathBuf, rx: UnboundedReceiver<Message>) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            data_dir,
            rx: Some(rx),
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
        let mut rx = std::mem::take(&mut self.rx).unwrap();

        async move {
            is_ready.store(true, Ordering::SeqCst);
            loop {
                select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Shutting down MetricsStore...");
                        break Ok(())
                    }
                    message = rx.recv()
                    => {
                        if let Some(message) = message {
                           match message{
                               Message::Counters(bytes) | Message::Gauges(bytes) => {
                                   let cursor = std::io::Cursor::new(bytes);
                                   let reader = StreamReader::try_new(cursor, None)?;
                                   for batch in reader {
                                       let batch = batch?;
                                   }
                               }
                           }
                        }
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
pub trait MetricsStoreService: Service {}
impl MetricsStoreService for MetricsStore {}
