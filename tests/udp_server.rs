use anyhow::Result;
use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray};
use arrow::{array::RecordBatch, ipc::reader::StreamReader};
use statsdfusion::metrics_store::MetricsStoreService;
use statsdfusion::{Service, metrics_store::Message};
use statsdfusion::{
    metric::Tags,
    metrics_store,
    startup::{StartupConfig, start_services},
    udp_server,
};
use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;

struct TestMetricsStore {
    is_ready: Arc<AtomicBool>,
    record_batches: Arc<RwLock<Vec<RecordBatch>>>,
    rx: Option<UnboundedReceiver<Message>>,
}

impl TestMetricsStore {
    pub fn new(rx: UnboundedReceiver<Message>) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            record_batches: Arc::new(RwLock::new(vec![])),
            rx: Some(rx),
        }
    }
}

impl Service for TestMetricsStore {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let record_batches = Arc::clone(&self.record_batches);
        let mut rx = std::mem::take(&mut self.rx).unwrap();

        async move {
            is_ready.store(true, Ordering::SeqCst);
            loop {
                select! {
                        _ = cancellation_token.cancelled() => {
                            break
                        }

                        message = rx.recv()
                        => {
                            if let Some(message) = message {
                                match message {
                                    Message::Counters(bytes) | Message::Gauges(bytes) => {
                                        let cursor = std::io::Cursor::new(bytes);
                                        let reader = StreamReader::try_new(cursor, None)?;
                                        let mut batches = record_batches.write().unwrap();
                                        for batch in reader {
                                            batches.push(batch?);
                                        }
                                    }
                                }
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

impl MetricsStoreService for TestMetricsStore {}

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
    use tokio::{
        net::UdpSocket,
        spawn,
        sync::mpsc::{self, UnboundedSender},
        time::sleep,
    };

    struct MetricsBehaviorTest {
        client_socket: UdpSocket,
        received_record_batches: Arc<RwLock<Vec<RecordBatch>>>,
        flush_interval: Duration,
    }

    impl MetricsBehaviorTest {
        async fn new(flush_interval_sec: u64) -> Self {
            let server_addr = {
                // Create a localhost addr w/ a wildcard port ,
                //   bind to get an available port,
                //   then drop the bind and return the addr w/ available port
                let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
                UdpSocket::bind(addr).await.unwrap().local_addr().unwrap()
            };

            let (metrics_store_tx, metrics_store_rx): (
                UnboundedSender<metrics_store::Message>,
                UnboundedReceiver<metrics_store::Message>,
            ) = mpsc::unbounded_channel();

            let mut udp_server = udp_server::UdpServer::new(
                server_addr.port(),
                flush_interval_sec,
                metrics_store_tx,
            );
            let mut metrics_store = TestMetricsStore::new(metrics_store_rx);

            let received_record_batches = metrics_store.record_batches.clone();

            // Start services and wait for them to be ready
            let running_services = start_services(
                &mut metrics_store,
                &mut udp_server,
                StartupConfig::default(),
            )
            .await
            .unwrap();

            // Connect client to the server
            let client_socket = UdpSocket::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
                .await
                .unwrap();

            client_socket.connect(server_addr).await.unwrap();

            // Spawn a background tasks that waits for the services
            spawn(async move {
                running_services.wait_until_shutdown().await.unwrap();
            });

            Self {
                client_socket,
                received_record_batches,
                flush_interval: Duration::from_secs(flush_interval_sec),
            }
        }

        async fn send_metrics(&self, packet: &str) {
            self.client_socket.send(packet.as_bytes()).await.unwrap();
        }

        async fn wait_for_flush(&self) {
            sleep(self.flush_interval).await;
        }

        fn get_batches(&self) -> Vec<RecordBatch> {
            self.received_record_batches.read().unwrap().clone()
        }

        fn clear_batches(&self) {
            self.received_record_batches.write().unwrap().clear();
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
        let batches = metrics_behavior_test.get_batches();

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
        let batches = metrics_behavior_test.get_batches();

        let expected_counters = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 1),
            (("metric2".into(), Tags::new()), 2),
            (("metric3".into(), Tags::new()), 0),
            (("metric4".into(), Tags::new()), 2),
        ]);
        assert_eq!(expected_counters, record_batch_to_hashmap_i64(&batches[0]));

        let expected_gauges = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 1.0),
            (("metric2".into(), Tags::new()), 2.0),
            (("metric3".into(), Tags::new()), 0.0),
            (("metric4".into(), Tags::new()), 2.0),
        ]);
        assert_eq!(expected_gauges, record_batch_to_hashmap_f64(&batches[1]));
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
        let batches = metrics_behavior_test.get_batches();

        let expected_counters = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 7),
            (("metric3".into(), Tags::new()), 0),
            (("metric4".into(), Tags::new()), 7),
        ]);
        assert_eq!(expected_counters, record_batch_to_hashmap_i64(&batches[0]));

        let expected_gauges = BTreeMap::from_iter(vec![
            (("metric1".into(), Tags::new()), 17.0),
            (("metric2".into(), Tags::new()), 3.0),
            (("metric3".into(), Tags::new()), 7.0),
            (("metric4".into(), Tags::new()), 10.0),
            (("metric5".into(), Tags::new()), 7.0),
            (("metric6".into(), Tags::new()), -7.0),
            (("metric7".into(), Tags::new()), 7.0),
        ]);
        assert_eq!(expected_gauges, record_batch_to_hashmap_f64(&batches[1]));
    }
}
