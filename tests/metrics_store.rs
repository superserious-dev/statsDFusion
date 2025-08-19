use anyhow::Result;
use arrow_flight::{FlightClient, FlightDescriptor, PutResult, encode::FlightDataEncoderBuilder};
use chrono::{DateTime, Utc};
use datafusion::arrow::{
    array::{
        Float64Array, Int64Array, StringArray, TimestampSecondArray,
        builder::{MapBuilder, StringBuilder},
    },
    record_batch::RecordBatch,
};
use futures::{Future, TryStreamExt, stream};
use statsdfusion::{
    Service,
    metric::schema_fields::{counters_schema, gauges_schema},
    metrics_store::MetricsStore,
    startup::{RunningServices, StartupConfig, start_services},
    udp_server::UdpServerService,
};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;

struct TestUdpServer {
    flight_port: u16,
    flight_client: Option<FlightClient>,
}

impl TestUdpServer {
    pub fn new(flight_port: u16) -> Self {
        Self {
            flight_port,
            flight_client: None,
        }
    }

    pub async fn get_flight_client(&mut self) -> Result<&mut FlightClient> {
        if self.flight_client.is_none() {
            let channel = Channel::from_shared(format!("http://127.0.0.1:{}", self.flight_port))
                .unwrap()
                .connect()
                .await
                .unwrap();
            self.flight_client = Some(FlightClient::new(channel));
        }
        Ok(self.flight_client.as_mut().unwrap())
    }

    pub async fn do_put_counters(
        &mut self,
        flushed_at: DateTime<Utc>,
    ) -> Result<Vec<arrow_flight::PutResult>> {
        let batch = create_counter_batch(flushed_at)?;
        let flight_descriptor =
            FlightDescriptor::new_path(vec![flushed_at.to_rfc3339(), "counters".to_string()]);
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter([Ok(batch)]));

        let client = self.get_flight_client().await?;
        let response = client.do_put(flight_stream).await?;
        Ok(response.try_collect().await?)
    }

    pub async fn do_put_gauges(
        &mut self,
        flushed_at: DateTime<Utc>,
    ) -> Result<Vec<arrow_flight::PutResult>> {
        let batch = create_gauge_batch(flushed_at)?;
        let flight_descriptor =
            FlightDescriptor::new_path(vec![flushed_at.to_rfc3339(), "gauges".to_string()]);
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter([Ok(batch)]));

        let client = self.get_flight_client().await?;
        let response = client.do_put(flight_stream).await?;
        Ok(response.try_collect().await?)
    }

    pub async fn do_put_empty_stream(&mut self) -> Result<Vec<PutResult>> {
        let flight_stream = stream::empty();
        let client = self.get_flight_client().await?;
        let response = client.do_put(flight_stream).await?;
        Ok(response.try_collect().await?)
    }

    pub async fn do_put_invalid_descriptor(
        &mut self,
        flushed_at: DateTime<Utc>,
    ) -> Result<Vec<PutResult>> {
        let batch = create_counter_batch(flushed_at)?;
        let flight_descriptor = FlightDescriptor::new_path(vec!["invalid".to_string()]);

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter([Ok(batch)]));

        let client = self.get_flight_client().await?;
        let response = client.do_put(flight_stream).await?;
        Ok(response.try_collect().await?)
    }

    pub async fn do_put_invalid_timestamp(&mut self) -> Result<Vec<PutResult>> {
        let batch = create_counter_batch(Utc::now())?;
        let flight_descriptor =
            FlightDescriptor::new_path(vec!["not-a-timestamp".to_string(), "counters".to_string()]);

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter([Ok(batch)]));

        let client = self.get_flight_client().await?;
        let response = client.do_put(flight_stream).await?;
        Ok(response.try_collect().await?)
    }

    pub async fn do_put_unknown_table(
        &mut self,
        flushed_at: DateTime<Utc>,
    ) -> Result<Vec<arrow_flight::PutResult>> {
        let batch = create_counter_batch(flushed_at)?;
        let flight_descriptor =
            FlightDescriptor::new_path(vec![flushed_at.to_rfc3339(), "unknown_table".to_string()]);

        let flight_stream = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(flight_descriptor))
            .build(stream::iter([Ok(batch)]));

        let client = self.get_flight_client().await?;
        let response = client.do_put(flight_stream).await?;
        Ok(response.try_collect().await?)
    }
}

impl Service for TestUdpServer {
    #[allow(clippy::manual_async_fn)]
    fn service(
        &mut self,
        _cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        async move { Ok(()) }
    }

    fn is_ready(&self) -> bool {
        true
    }
}

impl UdpServerService for TestUdpServer {}

fn create_counter_batch(flushed_at: DateTime<Utc>) -> Result<RecordBatch> {
    let schema = counters_schema();

    let names = StringArray::from(vec!["test.counter1", "test.counter2"]);
    let values = Int64Array::from(vec![10, 20]);

    let mut map_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    map_builder.append(true)?;
    map_builder.append(true)?;
    let tags = map_builder.finish();

    let flushed_at_array =
        TimestampSecondArray::from(vec![flushed_at.timestamp(), flushed_at.timestamp()])
            .with_timezone("+00:00".to_string());

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(names),
            Arc::new(values),
            Arc::new(tags),
            Arc::new(flushed_at_array),
        ],
    )?)
}

fn create_gauge_batch(flushed_at: DateTime<Utc>) -> Result<RecordBatch> {
    let schema = gauges_schema();

    let names = StringArray::from(vec!["test.gauge1", "test.gauge2"]);
    let values = Float64Array::from(vec![1.5, 2.7]);

    let mut map_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    map_builder.append(true)?;
    map_builder.append(true)?;
    let tags = map_builder.finish();

    let flushed_at_array =
        TimestampSecondArray::from(vec![flushed_at.timestamp(), flushed_at.timestamp()])
            .with_timezone("+00:00".to_string());

    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(names),
            Arc::new(values),
            Arc::new(tags),
            Arc::new(flushed_at_array),
        ],
    )?)
}

async fn start_metrics_store_with_client(
    temp_dir: &TempDir,
) -> Result<(RunningServices, TestUdpServer)> {
    let port = { TcpListener::bind("127.0.0.1:0").await?.local_addr()?.port() };

    let mut metrics_store = MetricsStore::new(temp_dir.path().to_path_buf(), port);
    let mut test_udp_server = TestUdpServer::new(port);

    let running_services = start_services(
        &mut metrics_store,
        &mut test_udp_server,
        StartupConfig::default(),
    )
    .await?;

    Ok((running_services, test_udp_server))
}

#[cfg(test)]
mod tests {
    use arrow_flight::PutResult;
    use tokio_util::bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_flight_do_put_counters() {
        let temp_dir = TempDir::new().unwrap();
        let (running_services, mut test_server) =
            start_metrics_store_with_client(&temp_dir).await.unwrap();

        let flushed_at = Utc::now();
        let results = test_server.do_put_counters(flushed_at).await.unwrap();
        running_services.wait_until_shutdown().await.unwrap();

        assert_eq!(
            results,
            vec![PutResult {
                app_metadata: Bytes::new()
            }]
        );
    }

    #[tokio::test]
    async fn test_flight_do_put_gauges() {
        let temp_dir = TempDir::new().unwrap();
        let (running_services, mut test_server) =
            start_metrics_store_with_client(&temp_dir).await.unwrap();

        let flushed_at = Utc::now();
        let results = test_server.do_put_gauges(flushed_at).await.unwrap();
        let _ = running_services.wait_until_shutdown().await;

        assert_eq!(
            results,
            vec![PutResult {
                app_metadata: Bytes::new()
            }]
        );
    }

    #[tokio::test]
    async fn test_flight_do_put_invalid_descriptor() {
        let temp_dir = TempDir::new().unwrap();
        let (running_services, mut test_server) =
            start_metrics_store_with_client(&temp_dir).await.unwrap();

        let flushed_at = Utc::now();

        let results = test_server.do_put_invalid_descriptor(flushed_at).await;
        let _ = running_services.wait_until_shutdown().await;

        assert!(
            results
                .unwrap_err()
                .to_string()
                .contains("Path must have exactly 2 elements, got 1")
        );
    }

    #[tokio::test]
    async fn test_flight_do_put_invalid_timestamp() {
        let temp_dir = TempDir::new().unwrap();
        let (running_services, mut test_server) =
            start_metrics_store_with_client(&temp_dir).await.unwrap();

        let results = test_server.do_put_invalid_timestamp().await;
        let _ = running_services.wait_until_shutdown().await;

        assert!(
            results
                .unwrap_err()
                .to_string()
                .contains("Unable to parse flushed_at time")
        );
    }

    #[tokio::test]
    async fn test_flight_do_put_unknown_table() {
        let temp_dir = TempDir::new().unwrap();
        let (running_services, mut test_server) =
            start_metrics_store_with_client(&temp_dir).await.unwrap();

        let flushed_at = Utc::now();
        let results = test_server.do_put_unknown_table(flushed_at).await;
        let _ = running_services.wait_until_shutdown().await;

        assert!(
            results
                .unwrap_err()
                .to_string()
                .contains("Table `unknown_table` does not exist")
        );
    }

    #[tokio::test]
    async fn test_flight_do_put_empty_stream() {
        let temp_dir = TempDir::new().unwrap();
        let (running_services, mut test_server) =
            start_metrics_store_with_client(&temp_dir).await.unwrap();

        let results = test_server.do_put_empty_stream().await;
        let _ = running_services.wait_until_shutdown().await;

        assert!(
            results
                .unwrap_err()
                .to_string()
                .contains("No flight data received")
        );
    }
}
