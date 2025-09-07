use crate::{
    Service,
    metric::schema_fields::{counters_schema, gauges_schema, heartbeat_schema},
    redb_table_provider::RedbTable,
};
use anyhow::{Result, anyhow};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
};
use chrono::{DateTime, Utc};
use datafusion::{
    arrow::{array::RecordBatch, ipc::writer::StreamWriter},
    datasource::TableProvider,
    prelude::SessionContext,
    scalar::ScalarValue,
};
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream},
};
use log::info;
use redb::{Database, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    io::Cursor,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, transport::Server};

const HOT_METRICS_DB: &str = "hot_metrics.redb";
pub const HOT_METRICS_TABLE_HEARTBEAT: &str = "heartbeat";
pub const HOT_METRICS_TABLE_COUNTERS: &str = "counters";
pub const HOT_METRICS_TABLE_GAUGES: &str = "gauges";

const HOT_METRICS_TABLES: [&str; 3] = [
    HOT_METRICS_TABLE_HEARTBEAT,
    HOT_METRICS_TABLE_COUNTERS,
    HOT_METRICS_TABLE_GAUGES,
];

#[derive(Debug, Deserialize, Serialize)]
pub enum DoGetQuery {
    NameRegexWithFlushedAtInterval(String, (DateTime<Utc>, DateTime<Utc>)),
    ListMetrics,
}

#[derive(Clone)]
struct InnerFlightServer {
    database: Arc<Mutex<Database>>,
    ctx: SessionContext,
}

impl InnerFlightServer {
    fn batch_to_bytes(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        let cursor = Cursor::new(&mut buffer);

        let mut writer = StreamWriter::try_new(cursor, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;

        Ok(buffer)
    }

    fn write_batch(
        &self,
        table_definition: TableDefinition<i64, Vec<u8>>,
        flushed_at: DateTime<Utc>,
        batch: &RecordBatch,
    ) -> Result<()> {
        let bytes = Self::batch_to_bytes(batch).map_err(|e| Status::unknown(e.to_string()))?;

        if let Ok(db) = self.database.lock() {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(table_definition)?;
                table.insert(flushed_at.timestamp(), bytes)?;
            }
            write_txn.commit()?;
        }
        Ok(())
    }

    fn write_heartbeat(&self, flushed_at: DateTime<Utc>, batch: &RecordBatch) -> Result<()> {
        const HEARTBEAT_TABLE: TableDefinition<i64, Vec<u8>> =
            TableDefinition::new(HOT_METRICS_TABLE_HEARTBEAT);
        self.write_batch(HEARTBEAT_TABLE, flushed_at, batch)
    }

    fn write_counters_batch(&self, flushed_at: DateTime<Utc>, batch: &RecordBatch) -> Result<()> {
        const COUNTERS_TABLE: TableDefinition<i64, Vec<u8>> =
            TableDefinition::new(HOT_METRICS_TABLE_COUNTERS);
        self.write_batch(COUNTERS_TABLE, flushed_at, batch)
    }

    fn write_gauges_batch(&self, flushed_at: DateTime<Utc>, batch: &RecordBatch) -> Result<()> {
        const GAUGES_TABLE: TableDefinition<i64, Vec<u8>> =
            TableDefinition::new(HOT_METRICS_TABLE_GAUGES);
        self.write_batch(GAUGES_TABLE, flushed_at, batch)
    }
}

// TODO Review returned Errs and select the most accurate Status variant for each case
#[tonic::async_trait]
impl FlightService for InnerFlightServer {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Implement handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Implement get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Implement poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let bytes = &request.get_ref().ticket;
        let query: DoGetQuery = serde_json::from_slice(bytes).unwrap();

        let df = match query {
            DoGetQuery::NameRegexWithFlushedAtInterval(
                mut name_regex,
                (flushed_at_start, flushed_at_end),
            ) => {
                // Anchor regex if it's not anchored
                if !name_regex.starts_with("^") {
                    name_regex.insert(0, '^');
                }
                if !name_regex.ends_with("$") {
                    name_regex.push('$');
                }

                self
                    .ctx
                    .sql(&format!(
                        "
                        WITH heartbeat AS (
                            SELECT flushed_at
                            FROM {HOT_METRICS_TABLE_HEARTBEAT}
                            WHERE
                                flushed_at >= $flushed_at_start
                                AND flushed_at <= $flushed_at_end
                        ),
                        raw_metrics AS (
                            SELECT *
                            FROM (
                                SELECT flushed_at, name, tags, value
                                FROM {HOT_METRICS_TABLE_COUNTERS}

                                UNION ALL

                                SELECT flushed_at, name, tags, value
                                FROM {HOT_METRICS_TABLE_GAUGES}
                            )
                        ),
                        matched_metrics AS (
                            SELECT flushed_at, NAMED_STRUCT('name', name, 'tags', tags, 'value', value) as data
                            FROM raw_metrics
                            WHERE name ~ $name_regex
                        )
                        SELECT
                            heartbeat.flushed_at, array_agg(matched_metrics.data) AS metrics
                        FROM
                            heartbeat
                        LEFT JOIN
                            matched_metrics
                        ON
                            heartbeat.flushed_at = matched_metrics.flushed_at
                        GROUP BY
                            heartbeat.flushed_at
                        ORDER BY
                            heartbeat.flushed_at ASC
                        "
                    ))
                    .await
                    .map_err(|e| Status::unknown(e.to_string()))?
                    .with_param_values(vec![
                        (
                            "flushed_at_start",
                            ScalarValue::TimestampSecond(
                                Some(flushed_at_start.timestamp()),
                                Some("+00:00".into()),
                            ),
                        ),
                        (
                            "flushed_at_end",
                            ScalarValue::TimestampSecond(
                                Some(flushed_at_end.timestamp()),
                                Some("+00:00".into()),
                            ),
                        ),
                        ("name_regex", ScalarValue::from(name_regex)),
                    ])
                    .map_err(|e| Status::unknown(e.to_string()))?
            }
            DoGetQuery::ListMetrics => {
                // FIXME this currently does a full-history scan; implement an indexing strategy for this
                self
                    .ctx
                    .sql(&format!(
                        "
                        WITH metrics AS (
                            SELECT *
                            FROM (
                                SELECT flushed_at, name, tags
                                FROM {HOT_METRICS_TABLE_COUNTERS}

                                UNION ALL

                                SELECT flushed_at, name, tags
                                FROM {HOT_METRICS_TABLE_GAUGES}
                            )
                        )
                        SELECT name, tags, min_flushed_at, max_flushed_at, measurement_count
                        FROM (
                            -- NOTE tried a few different approaches to grouping by (name, tags) and this worked
                            SELECT name, MAP_KEYS(tags), MAP_VALUES(tags), FIRST_VALUE(tags) AS tags, MIN(flushed_at) AS min_flushed_at, MAX(flushed_at) AS max_flushed_at, COUNT(*) AS measurement_count
                            FROM metrics
                            GROUP BY name, MAP_KEYS(tags), MAP_VALUES(tags)
                        )
                        ORDER BY name, MAP_KEYS(tags), MAP_VALUES(tags)
                        "
                    ))
                    .await
                    .map_err(|e| Status::unknown(e.to_string()))?
            }
        };

        let record_batch_stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map(|s| s.map_err(|e| FlightError::ExternalError(e.into())));

        let flight_stream = FlightDataEncoderBuilder::new()
            .build(record_batch_stream)
            .map_err(|e| Status::internal(e.to_string()));

        let response = Response::new(Box::pin(flight_stream) as Self::DoGetStream);
        Ok(response)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut input_stream = request.into_inner();
        let mut output = vec![];

        // Get flight descriptor from first message
        let flight_data = input_stream.next().await.ok_or(Status::invalid_argument(
            "No flight data received".to_string(),
        ))??;
        let flight_descriptor =
            flight_data
                .flight_descriptor
                .as_ref()
                .ok_or(Status::invalid_argument(
                    "No flight descriptor found".to_string(),
                ))?;

        let (flushed_at, table) = match flight_descriptor.r#type() {
            DescriptorType::Path => {
                if flight_descriptor.path.len() != 2 {
                    return Err(Status::invalid_argument(format!(
                        "Path must have exactly 2 elements, got {}",
                        flight_descriptor.path.len()
                    )));
                } else {
                    let flushed_at = DateTime::parse_from_rfc3339(&flight_descriptor.path[0])
                        .map_err(|_| {
                            Status::invalid_argument("Unable to parse flushed_at time".to_string())
                        })?
                        .to_utc();

                    let table = flight_descriptor.path[1].clone();
                    if !HOT_METRICS_TABLES.contains(&table.as_str()) {
                        return Err(Status::invalid_argument(format!(
                            "Table `{}` does not exist",
                            table
                        )));
                    }

                    (flushed_at, table)
                }
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unexpected flight descriptor found: {flight_descriptor}"
                )));
            }
        };

        // Prepend the first message back on the stream
        let stream_with_descriptor =
            stream::once(futures::future::ready(Ok(flight_data))).chain(input_stream);

        // Extract record batches from the flight data stream
        let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            stream_with_descriptor.map_err(|e| e.into()),
        );
        while let Some(batch) = record_batch_stream.next().await {
            match batch {
                Ok(batch) => match table.as_str() {
                    HOT_METRICS_TABLE_HEARTBEAT => {
                        self.write_heartbeat(flushed_at, &batch)
                            .map_err(|e| Status::unknown(e.to_string()))?;
                    }
                    HOT_METRICS_TABLE_COUNTERS => {
                        self.write_counters_batch(flushed_at, &batch)
                            .map_err(|e| Status::unknown(e.to_string()))?;
                    }
                    HOT_METRICS_TABLE_GAUGES => {
                        self.write_gauges_batch(flushed_at, &batch)
                            .map_err(|e| Status::unknown(e.to_string()))?;
                    }
                    _ => {
                        eprintln!("Unknown table type: {}", table);
                    }
                },
                Err(e) => {
                    eprintln!("Error processing batch: {:?}", e);
                }
            }
        }

        output.push(Ok(PutResult {
            app_metadata: Default::default(),
        }));

        Ok(Response::new(Box::pin(stream::iter(output))))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}

pub struct MetricsStore {
    is_ready: Arc<AtomicBool>,
    data_dir: PathBuf,
    flight_port: u16,
}

impl MetricsStore {
    pub fn new(data_dir: PathBuf, flight_port: u16) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            data_dir,
            flight_port,
        }
    }
}

impl Service for MetricsStore {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let data_dir = self.data_dir.clone();
        let flight_port = self.flight_port;

        async move {
            let hot_metrics_path = &data_dir.join(HOT_METRICS_DB);
            let database = Arc::new(Mutex::new(Database::create(hot_metrics_path)?));
            let ctx = SessionContext::new();

            let heartbeat_schema = heartbeat_schema();
            let counters_schema = counters_schema();
            let gauges_schema = gauges_schema();

            let redb_heartbeat_table = Arc::new(RedbTable::new(
                database.clone(),
                HOT_METRICS_TABLE_HEARTBEAT,
                heartbeat_schema,
            )?);
            let _ = ctx.register_table(
                HOT_METRICS_TABLE_HEARTBEAT,
                Arc::clone(&redb_heartbeat_table) as Arc<dyn TableProvider>,
            );
            let redb_counters_table = Arc::new(RedbTable::new(
                database.clone(),
                HOT_METRICS_TABLE_COUNTERS,
                counters_schema,
            )?);
            let _ = ctx.register_table(
                HOT_METRICS_TABLE_COUNTERS,
                Arc::clone(&redb_counters_table) as Arc<dyn TableProvider>,
            );
            let redb_gauges_table = Arc::new(RedbTable::new(
                database.clone(),
                HOT_METRICS_TABLE_GAUGES,
                gauges_schema,
            )?);
            let _ = ctx.register_table(
                HOT_METRICS_TABLE_GAUGES,
                Arc::clone(&redb_gauges_table) as Arc<dyn TableProvider>,
            );

            let socket_addr = SocketAddr::from(([127, 0, 0, 1], flight_port));
            let service = InnerFlightServer { database, ctx };
            let svc = FlightServiceServer::new(service);

            info!("Starting Flight server @ {socket_addr}");

            let server_future = Server::builder().add_service(svc).serve(socket_addr);

            is_ready.store(true, Ordering::SeqCst);
            select! {
                _ = cancellation_token.cancelled() => {
                    info!("Shutting down MetricsStore...");
                }
                result = server_future => {
                    match result {
                        Ok(()) => info!("Flight server ran successfully"),
                        Err(e) => return Err(anyhow!("Flight server failure: {}", e)),
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
pub trait MetricsStoreService: Service {}
impl MetricsStoreService for MetricsStore {}
