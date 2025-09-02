use crate::Service;
use anyhow::{Result, anyhow};
use arrow_flight::FlightClient;
use axum::{Router, routing::get};
use log::info;
use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{net::TcpListener, select, sync::Mutex};
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tower_http::services::ServeDir;

#[derive(Debug)]
struct HttpServerState {
    flight_client: Mutex<FlightClient>,
}

pub struct HttpServer {
    is_ready: Arc<AtomicBool>,
    http_port: u16,
    flight_port: u16,
}

impl HttpServer {
    pub fn new(http_port: u16, flight_port: u16) -> Self {
        Self {
            is_ready: Arc::new(AtomicBool::new(false)),
            http_port,
            flight_port,
        }
    }
}

impl Service for HttpServer {
    fn service(
        &mut self,
        cancellation_token: CancellationToken,
    ) -> impl Future<Output = Result<()>> + Send + 'static {
        let is_ready = Arc::clone(&self.is_ready);
        let http_port = self.http_port;
        let flight_port = self.flight_port;

        async move {
            let channel = Channel::from_shared(format!("http://127.0.0.1:{flight_port}"))
                .map_err(|e| anyhow!("Failed to create channel: {e}"))?
                .connect()
                .await
                .map_err(|e| anyhow!("Failed to connect to flight server: {e}"))?;
            let flight_client = FlightClient::new(channel);
            let http_server_state = HttpServerState {
                flight_client: Mutex::new(flight_client),
            };

            let http_socket_addr = SocketAddr::from(([127, 0, 0, 1], http_port));
            let listener = TcpListener::bind(http_socket_addr).await?;
            let router = Router::new()
                .route("/", get(routes::history))
                .route("/query", get(routes::query))
                .nest_service("/static", ServeDir::new("static/"))
                .with_state(Arc::new(http_server_state));

            info!("Starting Http server @ {http_socket_addr}");

            let server_future = axum::serve(listener, router);
            is_ready.store(true, Ordering::SeqCst);
            select! {
                _ = cancellation_token.cancelled() => {
                    info!("Shutting down Http server...");
                }
                result = server_future => {
                    match result {
                        Ok(()) => info!("Http server ran successfully"),
                        Err(e) => return Err(anyhow!("Http server failure: {}", e)),
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
pub trait HttpServerService: Service {}
impl HttpServerService for HttpServer {}

mod routes {
    use crate::{http_server::HttpServerState, metrics_store::DoGetQuery};
    use anyhow::{anyhow, bail};
    use arrow_flight::Ticket;
    use axum::{
        Json,
        extract::{Query, State},
        http::StatusCode,
        response::IntoResponse,
    };
    use chrono::{DateTime, Utc};
    use datafusion::{
        arrow::array::RecordBatch,
        common::arrow::json::{WriterBuilder, writer::JsonArray},
    };
    use futures::TryStreamExt as _;
    use maud::{DOCTYPE, html};
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::sync::Arc;

    #[derive(Debug, Serialize)]
    pub(crate) struct HistoryResponse {
        metrics: Value,
    }

    #[derive(Debug, Deserialize)]
    pub(crate) struct QueryParams {
        #[serde(rename = "name-regex")]
        name_regex: String,
        #[serde(rename = "flushed-at-start")]
        flushed_at_start: DateTime<Utc>,
        #[serde(rename = "flushed-at-end")]
        flushed_at_end: DateTime<Utc>,
    }

    pub(crate) async fn history() -> impl IntoResponse {
        let body = html! {
            (DOCTYPE)
            link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.indigo.min.css";
            link rel="stylesheet" href="/static/query.css";
            script type="text/javascript" src="https://cdn.jsdelivr.net/npm/echarts@6.0.0/dist/echarts.min.js" {}
            script defer type="text/javascript" src="/static/query.js" {}

            body {
                main."container-fluid" {
                    form id="metricsQueryForm" {
                        fieldset role="group" {
                            label {
                                "Start"
                                input type="datetime-local" name="flushed-at-start" required;
                            }
                            label {
                                "End"
                                input type="datetime-local" name="flushed-at-end" required;
                            }
                        }

                        fieldset role="group" {
                            input name="name-regex" placeholder="Name Regex" aria-label="Name Regex" required;
                            input type="submit" value="Query";
                        }
                    }

                    div id="chart" {}
                }
            }
        };
        (
            [
                ("Cache-Control", "no-store, must-revalidate"),
                ("Content-Type", "text/html"),
            ],
            body.into_string(),
        )
    }

    fn recordbatches_to_json(batches: &[RecordBatch]) -> anyhow::Result<serde_json::Value> {
        if !batches.is_empty() {
            let buf = Vec::new();
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(buf);
            let batch_refs: Vec<&_> = batches.iter().collect();

            if let Err(e) = writer.write_batches(&batch_refs) {
                bail!("Failed to write batches: {e}");
            }

            if let Err(e) = writer.finish() {
                bail!("Failed to finish writer: {e}");
            }

            serde_json::from_slice(&writer.into_inner()).map_err(|e| anyhow!(e.to_string()))
        } else {
            Ok(serde_json::Value::Array(vec![]))
        }
    }

    pub(crate) async fn query(
        State(state): State<Arc<HttpServerState>>,
        Query(params): Query<QueryParams>,
    ) -> impl IntoResponse {
        let query = DoGetQuery::NameRegexWithFlushedAtInterval(
            params.name_regex,
            (params.flushed_at_start, params.flushed_at_end),
        );
        let serialized: Vec<u8> = serde_json::to_vec(&query).unwrap();
        let ticket = Ticket::new(serialized);

        match state.flight_client.lock().await.do_get(ticket).await {
            Ok(record_batch_stream) => match record_batch_stream.try_collect::<Vec<_>>().await {
                Ok(batches) => match recordbatches_to_json(&batches) {
                    Ok(metrics) => {
                        let response = HistoryResponse { metrics };
                        Json(response).into_response()
                    }
                    Err(e) => {
                        log::error!("Error parsing batches: {e}");
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
                },
                Err(e) => {
                    log::error!("Error collecting batches: {e}");
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            },
            Err(e) => {
                log::error!("Flight client error: {e}");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            }
        }
    }
}
