use crate::{
    Service,
    metric::{Metric, MetricValue, Tags, parse_packet, schema_fields},
    metrics_store::{
        HOT_METRICS_TABLE_COUNTERS, HOT_METRICS_TABLE_GAUGES, HOT_METRICS_TABLE_HEARTBEAT,
    },
};
use anyhow::{Context as _, Result};
use arrow_flight::{
    FlightClient, FlightDescriptor, PutResult,
    encode::{FlightDataEncoder, FlightDataEncoderBuilder},
};
use chrono::{DateTime, Timelike as _, Utc};
use datafusion::arrow::{
    array::{
        ArrayRef, Float64Builder, Int64Builder, MapBuilder, RecordBatch, StringBuilder,
        TimestampSecondBuilder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use futures::{TryStreamExt, stream};
use log::info;
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{net::UdpSocket, select, time};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

const MAX_RECEIVE_BUFFER_BYTES: usize = 10_000;

pub struct UdpServer {
    is_ready: Arc<AtomicBool>,
    udp_port: u16,
    flight_port: u16,
    flush_interval: u64,
}

impl UdpServer {
    pub fn new(udp_port: u16, flight_port: u16, flush_interval: u64) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            udp_port,
            flight_port,
            flush_interval,
        }
    }
}

#[derive(Debug)]
enum MetricData<'m> {
    Counters(&'m HashMap<(String, Tags), i64>),
    Gauges(&'m HashMap<(String, Tags), f64>),
}

impl<'m> MetricData<'m> {
    fn len(&self) -> usize {
        match self {
            MetricData::Counters(data) => data.len(),
            MetricData::Gauges(data) => data.len(),
        }
    }

    fn metric_type_name(&self) -> &'static str {
        match self {
            MetricData::Counters(_) => HOT_METRICS_TABLE_COUNTERS,
            MetricData::Gauges(_) => HOT_METRICS_TABLE_GAUGES,
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            MetricData::Counters(_) => DataType::Int64,
            MetricData::Gauges(_) => DataType::Float64,
        }
    }

    fn build_values_array(&self) -> Result<ArrayRef> {
        match self {
            MetricData::Counters(data) => {
                let mut builder = Int64Builder::new();
                for (_, value) in data.iter() {
                    builder.append_value(*value);
                }
                Ok(Arc::new(builder.finish()))
            }
            MetricData::Gauges(data) => {
                let mut builder = Float64Builder::new();
                for (_, value) in data.iter() {
                    builder.append_value(*value);
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }

    fn iter_keys(&self) -> Box<dyn Iterator<Item = &(String, Tags)> + '_> {
        match self {
            MetricData::Counters(data) => Box::new(data.keys()),
            MetricData::Gauges(data) => Box::new(data.keys()),
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
                            // In the case where there is no previous value and the delta is negative, saturate at 0.
                            MetricValue::Delta(rv) => rv.max(0), // adding to an unset counter creates a new counter with value=delta
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

    fn serialize_metrics(
        &self,
        metric_data: MetricData,
        flushed_at: DateTime<Utc>,
    ) -> Result<FlightDataEncoder> {
        let names_field = schema_fields::name_field();
        let tags_field = schema_fields::tags_field();
        let flushed_at_field = schema_fields::flushed_at_field();

        let mut names_builder = StringBuilder::new();
        let mut tags_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        let mut flushed_at_builder =
            TimestampSecondBuilder::new().with_data_type(flushed_at_field.data_type().clone());

        flushed_at_builder.append_value_n(flushed_at.timestamp(), metric_data.len());
        for (name, tags) in metric_data.iter_keys() {
            names_builder.append_value(name);

            let (kb, vb) = tags_builder.entries();
            for (tag_key, tag_value) in tags {
                kb.append_value(tag_key);
                vb.append_option(tag_value.clone());
            }
            tags_builder.append(true)?;
        }

        let values_array = metric_data.build_values_array()?;

        let schema = Schema::new(vec![
            names_field.clone(),
            Field::new("value", metric_data.data_type(), false),
            tags_field.clone(),
            flushed_at_field.clone(),
        ]);
        let data: Vec<ArrayRef> = vec![
            Arc::new(names_builder.finish()),
            values_array,
            Arc::new(tags_builder.finish()),
            Arc::new(flushed_at_builder.finish()),
        ];

        let batch = RecordBatch::try_new(schema.into(), data)?;

        Ok(FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![
                flushed_at.to_rfc3339(),
                metric_data.metric_type_name().to_string(),
            ])))
            .build(stream::iter([Ok(batch)])))
    }

    fn serialize_counters(&self, flushed_at: DateTime<Utc>) -> Result<FlightDataEncoder> {
        self.serialize_metrics(MetricData::Counters(&self.counters), flushed_at)
    }

    fn serialize_gauges(&self, flushed_at: DateTime<Utc>) -> Result<FlightDataEncoder> {
        self.serialize_metrics(MetricData::Gauges(&self.gauges), flushed_at)
    }

    fn serialize_heartbeat(&self, flushed_at: DateTime<Utc>) -> Result<FlightDataEncoder> {
        let field = Field::new(
            "flushed_at",
            DataType::Timestamp(TimeUnit::Second, Some("+00:00".into())),
            false,
        );

        let mut flushed_at_builder =
            TimestampSecondBuilder::new().with_data_type(field.data_type().clone());
        let schema = Schema::new(vec![field.clone()]);

        flushed_at_builder.append_value(flushed_at.timestamp());

        let data: Vec<ArrayRef> = vec![Arc::new(flushed_at_builder.finish())];

        let batch = RecordBatch::try_new(schema.into(), data)?;

        Ok(FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(FlightDescriptor::new_path(vec![
                flushed_at.to_rfc3339(),
                HOT_METRICS_TABLE_HEARTBEAT.to_string(),
            ])))
            .build(stream::iter([Ok(batch)])))
    }

    /// Exports metrics as FlightData and sets up for the next Flush Interval
    pub fn flush(&mut self, flushed_at: DateTime<Utc>) -> Result<Vec<FlightDataEncoder>> {
        // Encode heartbeat as a stream of FlightData
        let heartbeat_flight_stream = self.serialize_heartbeat(flushed_at)?;

        // Encode metrics as a stream of FlightData
        let counters_flight_stream = self.serialize_counters(flushed_at)?;
        let gauges_flight_stream = self.serialize_gauges(flushed_at)?;

        // Delete counters after each flush interval
        self.counters = HashMap::new();

        Ok(vec![
            heartbeat_flight_stream,
            counters_flight_stream,
            gauges_flight_stream,
        ])
    }
}

impl Service for UdpServer {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let udp_port = self.udp_port;
        let flight_port = self.flight_port;
        let is_ready = Arc::clone(&self.is_ready);
        let mut flush_interval = time::interval(Duration::from_secs(self.flush_interval));
        let mut aggregates = FlushIntervalAggregates::new();

        async move {
            let addr = format!("http://localhost:{flight_port}");
            let channel = Channel::from_shared(addr)
                .expect("invalid address for flight server")
                .connect()
                .await
                .expect("error connecting to flight server");
            let mut flight_client = FlightClient::new(channel);

            let socket_addr = SocketAddr::from(([127, 0, 0, 1], udp_port));
            let socket = UdpSocket::bind(socket_addr).await?;
            info!(
                "UDP server listening on {}:{}",
                socket_addr.ip(),
                socket_addr.port()
            );

            flush_interval.tick().await; // The first tick is immediate, so skip

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

                        // Send Flights to server
                        let outgoing_flights = aggregates.flush(flushed_at)?;
                        for flight in outgoing_flights {
                            let _response: Vec<PutResult> = flight_client
                                .do_put(flight)
                                .await?
                                .try_collect()
                                .await?;
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
