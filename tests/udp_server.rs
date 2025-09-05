use anyhow::Result;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
};
use datafusion::arrow::array::{
    Array, Float64Array, Int64Array, MapArray, RecordBatch, StringArray,
};
use futures::stream::{self, BoxStream};
use futures::{StreamExt as _, TryStreamExt as _};
use statsdfusion::{
    Service,
    http_server::HttpServerService,
    metric::Tags,
    metrics_store::{
        HOT_METRICS_TABLE_COUNTERS, HOT_METRICS_TABLE_GAUGES, HOT_METRICS_TABLE_HEARTBEAT,
        MetricsStoreService,
    },
    startup::{StartupConfig, start_services},
    udp_server,
};
use std::{
    collections::BTreeMap,
    future::Future,
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, transport::Server};

struct TestMetricsStore {
    is_ready: Arc<AtomicBool>,
    flight_port: u16,
    heartbeat_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
    counter_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
    gauge_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
}

impl TestMetricsStore {
    pub fn new(flight_port: u16) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            flight_port,
            heartbeat_record_batches: Arc::new(RwLock::new(vec![])),
            counter_record_batches: Arc::new(RwLock::new(vec![])),
            gauge_record_batches: Arc::new(RwLock::new(vec![])),
        }
    }
}

impl Service for TestMetricsStore {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let heartbeat_record_batches = Arc::clone(&self.heartbeat_record_batches);
        let counter_record_batches = Arc::clone(&self.counter_record_batches);
        let gauge_record_batches = Arc::clone(&self.gauge_record_batches);
        let flight_port = self.flight_port;

        async move {
            let socket_addr = SocketAddr::from(([127, 0, 0, 1], flight_port));
            let service = InnerFlightServer {
                heartbeat_record_batches: Arc::clone(&heartbeat_record_batches),
                counter_record_batches: Arc::clone(&counter_record_batches),
                gauge_record_batches: Arc::clone(&gauge_record_batches),
            };
            let svc = FlightServiceServer::new(service);

            let server_future = Server::builder().add_service(svc).serve(socket_addr);

            is_ready.store(true, Ordering::SeqCst);
            select! {
                _ = cancellation_token.cancelled() => { }
                _ = server_future => { }
            }

            Ok(())
        }
    }

    fn is_ready(&self) -> bool {
        self.is_ready.load(Ordering::SeqCst)
    }
}

impl MetricsStoreService for TestMetricsStore {}

#[derive(Clone, Debug)]
struct InnerFlightServer {
    heartbeat_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
    counter_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
    gauge_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
}

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
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut input_stream = request.into_inner();
        let mut output = vec![];

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

        let table = match flight_descriptor.r#type() {
            DescriptorType::Path => {
                if flight_descriptor.path.len() != 2 {
                    return Err(Status::invalid_argument(format!(
                        "Path must have exactly 2 elements, got {}",
                        flight_descriptor.path.len()
                    )));
                } else {
                    flight_descriptor.path[1].clone()
                }
            }
            _ => {
                return Err(Status::invalid_argument(format!(
                    "Unexpected flight descriptor found: {flight_descriptor}"
                )));
            }
        };

        let stream_with_descriptor =
            stream::once(futures::future::ready(Ok(flight_data))).chain(input_stream);

        let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            stream_with_descriptor.map_err(|e| e.into()),
        );

        while let Some(batch) = record_batch_stream.next().await {
            if let Ok(batch) = batch {
                match table.as_str() {
                    HOT_METRICS_TABLE_HEARTBEAT => {
                        self.heartbeat_record_batches.write().unwrap().push(batch)
                    }
                    HOT_METRICS_TABLE_COUNTERS => {
                        self.counter_record_batches.write().unwrap().push(batch)
                    }
                    HOT_METRICS_TABLE_GAUGES => {
                        self.gauge_record_batches.write().unwrap().push(batch)
                    }
                    _ => unimplemented!(),
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

struct TestHttpServer {}

impl TestHttpServer {
    pub fn new() -> Self {
        Self {}
    }
}

impl Service for TestHttpServer {
    #[allow(clippy::manual_async_fn)]
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        async move {
            cancellation_token.cancelled().await;
            Ok(())
        }
    }

    fn is_ready(&self) -> bool {
        true
    }
}

impl HttpServerService for TestHttpServer {}

macro_rules! impl_record_batch_to_map {
    ($func:ident, $value_type:ty, $array_type:ty) => {
        fn $func(batch: &RecordBatch) -> BTreeMap<(String, Tags), $value_type> {
            let mut map: BTreeMap<(String, Tags), $value_type> = BTreeMap::new();

            let names = batch
                .column_by_name("name")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<$array_type>()
                .unwrap();

            let tags = batch
                .column_by_name("tags")
                .unwrap()
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();

            for row_idx in 0..batch.num_rows() {
                let row_tags = tags.value(row_idx);

                let keys_array = row_tags
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                let values_array = row_tags
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();

                let mut tags_set = Tags::new();
                for tag_idx in 0..keys_array.len() {
                    let key = keys_array.value(tag_idx).to_string();
                    let value = if values_array.is_null(tag_idx) {
                        None
                    } else {
                        Some(values_array.value(tag_idx).to_string())
                    };
                    tags_set.insert((key, value));
                }

                map.insert(
                    (names.value(row_idx).to_string(), tags_set),
                    values.value(row_idx),
                );
            }
            map
        }
    };
}
impl_record_batch_to_map!(record_batch_to_hashmap_i64, i64, Int64Array);
impl_record_batch_to_map!(record_batch_to_hashmap_f64, f64, Float64Array);

mod tests {

    use std::{
        collections::BTreeMap,
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use super::*;
    use tokio::{net::UdpSocket, spawn, time::sleep};

    struct MetricsBehaviorTest {
        client_socket: UdpSocket,
        counter_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
        gauge_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
        flush_interval: Duration,
    }

    impl MetricsBehaviorTest {
        async fn new(flush_interval_sec: u64) -> Self {
            let (udp_server_addr, flight_server_addr) = {
                // Create a localhost addr w/ a wildcard port ,
                //   bind to get an available port,
                //   then drop the bind and return the addr w/ available port
                let udp_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
                let flight_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
                (
                    UdpSocket::bind(udp_addr)
                        .await
                        .unwrap()
                        .local_addr()
                        .unwrap(),
                    UdpSocket::bind(flight_addr)
                        .await
                        .unwrap()
                        .local_addr()
                        .unwrap(),
                )
            };

            let mut udp_server = udp_server::UdpServer::new(
                udp_server_addr.port(),
                flight_server_addr.port(),
                flush_interval_sec,
            );
            let mut metrics_store = TestMetricsStore::new(flight_server_addr.port());
            let mut http_server = TestHttpServer::new();

            let counter_record_batches = metrics_store.counter_record_batches.clone();
            let gauge_record_batches = metrics_store.gauge_record_batches.clone();

            // Start services and wait for them to be ready
            let running_services = start_services(
                &mut metrics_store,
                &mut udp_server,
                &mut http_server,
                StartupConfig::default(),
            )
            .await
            .unwrap();

            // Connect client to the server
            let client_socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .unwrap();

            client_socket.connect(udp_server_addr).await.unwrap();

            // Spawn a background tasks that waits for the services
            spawn(async move {
                running_services.wait_until_shutdown().await.unwrap();
            });

            Self {
                client_socket,
                counter_record_batches,
                gauge_record_batches,
                flush_interval: Duration::from_secs(flush_interval_sec),
            }
        }

        async fn send_metrics(&self, packet: &str) {
            self.client_socket.send(packet.as_bytes()).await.unwrap();
        }

        async fn wait_for_flush(&self) {
            sleep(self.flush_interval).await;
        }

        fn get_counter_batches(&self) -> Vec<RecordBatch> {
            self.counter_record_batches.read().unwrap().clone()
        }

        fn get_gauge_batches(&self) -> Vec<RecordBatch> {
            self.gauge_record_batches.read().unwrap().clone()
        }

        fn clear_batches(&self) {
            self.counter_record_batches.write().unwrap().clear();
            self.gauge_record_batches.write().unwrap().clear();
        }
    }

    #[tokio::test]
    async fn test_tag_behavior() {
        let metrics_behavior_test = MetricsBehaviorTest::new(1).await;

        metrics_behavior_test
            .send_metrics("metric1:+1|c\nmetric1:+10|c|#tk1\nmetric1:+100|c|#tk1:tv1")
            .await;
        metrics_behavior_test
            .send_metrics("metric1:+2|c\nmetric1:+20|c|#tk1\nmetric1:+200|c|#tk1:tv1")
            .await;

        metrics_behavior_test.wait_for_flush().await;
        let batches = metrics_behavior_test.get_counter_batches();

        let expected = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 3),
            (
                (
                    "metric1".into(),
                    Tags::from_iter(vec![("tk1".into(), None)]),
                ),
                30,
            ),
            (
                (
                    "metric1".into(),
                    Tags::from_iter(vec![("tk1".into(), Some("tv1".into()))]),
                ),
                300,
            ),
        ]);
        assert_eq!(expected, record_batch_to_hashmap_i64(&batches[0]));
    }

    #[tokio::test]
    async fn test_behavior_within_interval() {
        let metrics_behavior_test = MetricsBehaviorTest::new(1).await;

        // COUNTERS
        // metric1: 1 => 1
        // metric2: 1, +1 => 2
        // metric3: 1, -1 => 0
        // metric4: +1, +1 => 2
        metrics_behavior_test.send_metrics("metric1:1|c\n").await;
        metrics_behavior_test
            .send_metrics("metric2:1|c\nmetric2:+1|c\n")
            .await;
        metrics_behavior_test
            .send_metrics("metric3:1|c\nmetric3:-1|c\n")
            .await;
        metrics_behavior_test
            .send_metrics("metric4:+1|c\nmetric4:+1|c\n")
            .await;

        // GAUGES
        // metric1: 1.0 => 1.0
        // metric2: 1.0, +1.0 => 2.0
        // metric3: 1.0, -1.0 => 0.0
        // metric4: +1.0, +1.0 => 2.0
        metrics_behavior_test.send_metrics("metric1:1|g\n").await;
        metrics_behavior_test
            .send_metrics("metric2:1|g\nmetric2:+1|g\n")
            .await;
        metrics_behavior_test
            .send_metrics("metric3:1|g\nmetric3:-1|g\n")
            .await;
        metrics_behavior_test
            .send_metrics("metric4:+1|g\nmetric4:+1|g\n")
            .await;

        metrics_behavior_test.wait_for_flush().await;
        let counter_batches = metrics_behavior_test.get_counter_batches();

        let expected_counters = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 1),
            (("metric2".into(), Tags::new()), 2),
            (("metric3".into(), Tags::new()), 0),
            (("metric4".into(), Tags::new()), 2),
        ]);
        assert_eq!(
            expected_counters,
            record_batch_to_hashmap_i64(&counter_batches[0])
        );
        assert_eq!(counter_batches.len(), 1);

        let gauge_batches = metrics_behavior_test.get_gauge_batches();
        let expected_gauges = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 1.0),
            (("metric2".into(), Tags::new()), 2.0),
            (("metric3".into(), Tags::new()), 0.0),
            (("metric4".into(), Tags::new()), 2.0),
        ]);
        assert_eq!(
            expected_gauges,
            record_batch_to_hashmap_f64(&gauge_batches[0])
        );
        assert_eq!(gauge_batches.len(), 1);
    }

    #[tokio::test]
    async fn test_behavior_across_intervals() {
        // NOTE This test spans 2 flush intervals
        // Note the `~` means that nothing was sent for the metric in this interval

        // COUNTERS
        // NAME,      FI1,  FI2,  EXPECTED
        // metric1,   10,    7,    7
        // metric2,   10,    ~,    ~
        // metric3,   10,   -7,    0
        // metric4,   ~,     7,    7

        // GAUGES
        // NAME,      FI1,     FI2,    EXPECTED
        // metric1,   10.0,    +7.0,     17.0
        // metric2,   10.0,    -7.0,      3.0
        // metric3,   10.0,     7.0,      7.0
        // metric4,   10.0,       ~,     10.0
        // metric5,   ~ ,      +7.0,      7.0
        // metric6,   ~ ,      -7.0,     -7.0
        // metric7,   ~ ,       7.0,      7.0

        let metrics_behavior_test = MetricsBehaviorTest::new(1).await;

        metrics_behavior_test
            .send_metrics("metric1:10|c\nmetric2:10|c\nmetric3:10|c")
            .await;
        metrics_behavior_test
            .send_metrics("metric1:10.0|g\nmetric2:10.0|g\nmetric3:10.0|g\nmetric4:10.0|g")
            .await;

        // Wait for the duration of FI1
        metrics_behavior_test.wait_for_flush().await;

        // Clear the record batches from F1
        metrics_behavior_test.clear_batches();

        metrics_behavior_test
            .send_metrics("metric1:7|c\nmetric3:-7|c\nmetric4:7|c")
            .await;
        metrics_behavior_test
            .send_metrics("metric1:+7.0|g\nmetric2:-7.0|g\nmetric3:7.0|g\nmetric5:+7.0|g\nmetric6:-7.0|g\nmetric7:7.0|g\n")
            .await;

        // Wait for the duration of FI2
        metrics_behavior_test.wait_for_flush().await;

        // Check the aggregated data
        let counter_batches = metrics_behavior_test.get_counter_batches();

        let expected_counters = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 7),
            (("metric3".into(), Tags::new()), 0),
            (("metric4".into(), Tags::new()), 7),
        ]);
        assert_eq!(
            expected_counters,
            record_batch_to_hashmap_i64(&counter_batches[0])
        );
        assert_eq!(counter_batches.len(), 1);

        let gauge_batches = metrics_behavior_test.get_gauge_batches();
        let expected_gauges = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 17.0),
            (("metric2".into(), Tags::new()), 3.0),
            (("metric3".into(), Tags::new()), 7.0),
            (("metric4".into(), Tags::new()), 10.0),
            (("metric5".into(), Tags::new()), 7.0),
            (("metric6".into(), Tags::new()), -7.0),
            (("metric7".into(), Tags::new()), 7.0),
        ]);
        assert_eq!(
            expected_gauges,
            record_batch_to_hashmap_f64(&gauge_batches[0])
        );
        assert_eq!(gauge_batches.len(), 1);
    }
}
