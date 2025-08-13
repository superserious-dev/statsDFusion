use crate::metric::{Metric, MetricValue, Tags, parse_packet};
use crate::{Service, metrics_store};
use anyhow::{Context as _, Result};
use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, MapBuilder, RecordBatch, StringBuilder,
    TimestampSecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use chrono::{DateTime, Timelike as _, Utc};
use log::info;
use std::collections::HashMap;
use std::time::Duration;
use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{net::UdpSocket, select, sync::mpsc::UnboundedSender, time};
use tokio_util::sync::CancellationToken;

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

/// Stores aggregated data in a FlushInterval
/// Note that metrics are uniquely identified by the combination
///   of type, name, and tags
#[derive(Default, Debug)]
struct FlushIntervalAggregates {
    counters: HashMap<(String, Tags), i64>,
    gauges: HashMap<(String, Tags), f64>,
}

impl FlushIntervalAggregates {
    fn new() -> Self {
        Self::default()
    }

    /// Aggregates incoming metrics
    fn push_metrics(&mut self, incoming_metrics: Vec<Metric>) {
        for metric in incoming_metrics {
            match metric {
                Metric::CounterMetric {
                    name,
                    tags,
                    value: incoming_value,
                } => {
                    let key = (name, tags);
                    let new_raw_value = if self.counters.contains_key(&key) {
                        let old_raw_value = self.counters.get(&key).unwrap();
                        match incoming_value {
                            MetricValue::Constant(rv) => rv,
                            // Counts can go as low as 0; counts that exceed the type bounds will saturate
                            MetricValue::Delta(rv) => (old_raw_value.saturating_add(rv)).max(0),
                        }
                    } else {
                        match incoming_value {
                            MetricValue::Constant(rv) => rv,
                            MetricValue::Delta(rv) => rv, // adding to an unset counter creates a new counter with value=delta
                        }
                    };
                    self.counters.insert(key, new_raw_value);
                }
                Metric::GaugeMetric {
                    name,
                    tags,
                    value: incoming_value,
                } => {
                    let key = (name, tags);
                    let new_raw_value = if self.gauges.contains_key(&key) {
                        let old_raw_value = self.gauges.get(&key).unwrap();
                        match incoming_value {
                            MetricValue::Constant(rv) => rv,
                            MetricValue::Delta(rv) => old_raw_value + rv,
                        }
                    } else {
                        match incoming_value {
                            MetricValue::Constant(rv) => rv,
                            MetricValue::Delta(rv) => rv, // adding to an unset gauge creates a new gauge with value=delta
                        }
                    };
                    self.gauges.insert(key, new_raw_value);
                }
            }
        }
    }

    fn reset_counters(&mut self) {
        for (_key, value) in self.counters.iter_mut() {
            *value = 0;
        }
    }

    fn serialize_counters(&self, flushed_at: DateTime<Utc>) -> Result<Vec<u8>> {
        let names_schema = Field::new("name", DataType::Utf8, false);
        let tags_schema = Field::new_map(
            "tags",
            "entries",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
            false,
            false,
        );
        let flushed_at_schema = Field::new(
            "flushed_at",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())), // UTC timezone offset
            false,
        );

        let mut names_builder = StringBuilder::new();
        let mut values_builder = Int64Builder::new();
        let mut tags_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let mut flushed_at_builder =
            TimestampSecondBuilder::new().with_data_type(flushed_at_schema.data_type().clone());

        flushed_at_builder.append_value_n(flushed_at.timestamp(), self.counters.len());
        for ((name, tags), value) in &self.counters {
            names_builder.append_value(name);
            values_builder.append_value(*value);

            let (kb, vb) = tags_builder.entries();
            for (tag_key, tag_value) in tags {
                kb.append_value(tag_key);
                vb.append_option(tag_value.clone());
            }
            tags_builder.append(true)?;
        }

        let schema = Schema::new(vec![
            names_schema.clone(),
            Field::new("value", DataType::Int64, false),
            tags_schema.clone(),
            flushed_at_schema.clone(),
        ]);
        let data: Vec<ArrayRef> = vec![
            Arc::new(names_builder.finish()),
            Arc::new(values_builder.finish()),
            Arc::new(tags_builder.finish()),
            Arc::new(flushed_at_builder.finish()),
        ];
        let batch = RecordBatch::try_new(schema.into(), data)?;

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        Ok(buffer)
    }

    fn serialize_gauges(&self, flushed_at: DateTime<Utc>) -> Result<Vec<u8>> {
        let names_schema = Field::new("name", DataType::Utf8, false);
        let tags_schema = Field::new_map(
            "tags",
            "entries",
            Field::new("keys", DataType::Utf8, false),
            Field::new("values", DataType::Utf8, true),
            false,
            false,
        );
        let flushed_at_schema = Field::new(
            "flushed_at",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())), // UTC timezone offset
            false,
        );

        let mut names_builder = StringBuilder::new();
        let mut values_builder = Float64Builder::new();
        let mut tags_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let mut flushed_at_builder =
            TimestampSecondBuilder::new().with_data_type(flushed_at_schema.data_type().clone());

        flushed_at_builder.append_value_n(flushed_at.timestamp(), self.gauges.len());
        for ((name, tags), value) in &self.gauges {
            names_builder.append_value(name);
            values_builder.append_value(*value);

            let (kb, vb) = tags_builder.entries();
            for (tag_key, tag_value) in tags {
                kb.append_value(tag_key);
                vb.append_option(tag_value.clone());
            }
            tags_builder.append(true)?;
        }

        let schema = Schema::new(vec![
            names_schema.clone(),
            Field::new("value", DataType::Float64, false),
            tags_schema.clone(),
            flushed_at_schema.clone(),
        ]);
        let data: Vec<ArrayRef> = vec![
            Arc::new(names_builder.finish()),
            Arc::new(values_builder.finish()),
            Arc::new(tags_builder.finish()),
            Arc::new(flushed_at_builder.finish()),
        ];
        let batch = RecordBatch::try_new(schema.into(), data)?;

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        Ok(buffer)
    }

    /// Exports metrics as Messages containing serialized RecordBatches and
    ///   sets up for the next Flush Interval
    /// Messages are expoerted in multiple batches...
    ///   - ... 1 batch per type
    ///   - ... batch order is consistent across flush intervals
    pub fn flush(&mut self, flushed_at: DateTime<Utc>) -> Result<Vec<metrics_store::Message>> {
        let messages = vec![
            metrics_store::Message::Counters(self.serialize_counters(flushed_at)?),
            metrics_store::Message::Gauges(self.serialize_gauges(flushed_at)?),
        ];
        // Reset counters to 0 each flush interval
        self.reset_counters();
        Ok(messages)
    }
}

impl Service for UdpServer {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let port = self.port;
        let is_ready = Arc::clone(&self.is_ready);
        let mut flush_interval = time::interval(Duration::from_secs(self.flush_interval));
        let mut aggregates = FlushIntervalAggregates::new();
        let metrics_store_tx = self.metrics_store_tx.clone();

        async move {
            flush_interval.tick().await; // The first tick is immediate, so skip

            // FIXME allow non-localhost IPs
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
                    _ = cancellation_token.cancelled() => {
                        info!("Shutting down UdpServer...");
                        break
                    }

                    // Receive bytes from UDP
                    Ok((len, _addr)) = socket.recv_from(&mut buf)
                    => {

                        // Convert bytes to string
                        let message = str::from_utf8(&buf[..len])?;

                        let incoming_metrics = parse_packet(message);
                        aggregates.push_metrics(incoming_metrics);
                    }

                    _ = flush_interval.tick() => {
                        let flushed_at = Utc::now().with_nanosecond(0).context("Failed to truncate flush datetime to seconds-level precision")?;
                        info!("Flush @ {flushed_at}");

                        let outgoing_messages = aggregates.flush(flushed_at)?;

                        for message in outgoing_messages {
                            metrics_store_tx.send(message)?;
                        }

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
pub trait UdpServerService: Service {}
impl UdpServerService for UdpServer {}
