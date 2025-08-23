use crate::Service;
use anyhow::{Result, anyhow};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    decode::FlightRecordBatchStream,
    flight_descriptor::DescriptorType,
    flight_service_server::{FlightService, FlightServiceServer},
};
use chrono::DateTime;
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, BoxStream},
};
use log::info;
use std::{
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status, Streaming, transport::Server};

#[derive(Clone, Debug)]
struct InnerFlightServer {}

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
                        })?;
                    let table = flight_descriptor.path[1].clone();

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
                    "counters" => {
                        info!("Storing counter batch at {}", flushed_at);
                        dbg!(&batch);
                    }
                    "gauges" => {
                        info!("Storing gauge batch at {}", flushed_at);
                        dbg!(&batch);
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
        let _data_dir = self.data_dir.to_string_lossy().to_string();
        let flight_port = self.flight_port;

        async move {
            let socket_addr = SocketAddr::from(([127, 0, 0, 1], flight_port));
            let service = InnerFlightServer {};
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
