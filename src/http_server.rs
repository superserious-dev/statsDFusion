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
                .route("/metrics", get(routes::metrics))
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
    use arrow_flight::{FlightClient, Ticket};
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
    use serde::{Deserialize, Serialize, Serializer};
    use std::fmt::Debug;
    use std::{collections::BTreeMap, sync::Arc};
    use tokio::sync::Mutex;

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

    pub(crate) async fn metrics(
        State(state): State<Arc<HttpServerState>>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        #[derive(Debug, Deserialize)]
        struct ListMetrics {
            name: String,
            tags: BTreeMap<String, Option<String>>,
            min_flushed_at: DateTime<Utc>,
            max_flushed_at: DateTime<Utc>,
            measurement_count: u128,
        }

        let metrics =
            fetch_do_get_as_type::<Vec<ListMetrics>>(&state.flight_client, DoGetQuery::ListMetrics)
                .await?;

        let tags2string = |tags: BTreeMap<String, Option<String>>| -> String {
            tags.iter()
                .map(|(k, v)| match v {
                    Some(val) => format!("{}={}", k, val),
                    None => k.clone(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        let body = html! {
            (DOCTYPE)
            link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.indigo.min.css";

            body {
                main."container-fluid" {

                    table {
                        thead {
                            tr {
                                th scope = "col" {"Name"}
                                th scope = "col" {"Tags"}
                                th scope = "col" {"First Seen"}
                                th scope = "col" {"Last Seen"}
                                th scope = "col" {"# of Measurements"}
                            }
                        }
                        tbody {
                            @for metric in metrics {
                                tr {
                                    th scope = "row" {(metric.name)}
                                    td {(tags2string(metric.tags))}
                                    // FIXME improve timezone handling
                                    td {(metric.min_flushed_at.format("%m-%d-%y %H:%M:%S %Z").to_string())}
                                    td {(metric.max_flushed_at.format("%m-%d-%y %H:%M:%S %Z").to_string())}
                                    td {(metric.measurement_count)}
                                }
                            }
                        }
                    }
                }
            }
        };
        let response = (
            [
                ("Cache-Control", "no-store, must-revalidate"),
                ("Content-Type", "text/html"),
            ],
            body.into_string(),
        );
        Ok(response)
    }

    pub(crate) async fn query(
        State(state): State<Arc<HttpServerState>>,
        Query(params): Query<QueryParams>,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let query = DoGetQuery::NameRegexWithFlushedAtInterval(
            params.name_regex,
            (params.flushed_at_start, params.flushed_at_end),
        );

        fn serialize_optional_vec<S>(
            vec: &Vec<Option<HistoryMetric>>,
            serializer: S,
        ) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let output = if vec.len() == 1 && vec[0].is_none() {
                &vec![]
            } else {
                vec
            };
            output.serialize(serializer)
        }

        #[derive(Debug, Deserialize, Serialize)]
        struct HistoryMetric {
            name: String,
            tags: BTreeMap<String, Option<String>>,
            value: f64,
        }

        #[derive(Debug, Deserialize, Serialize)]
        struct Flush {
            flushed_at: DateTime<Utc>,
            // custom serializer deals with a quirk in the upstream query
            // TODO investigate ways to improve this
            #[serde(serialize_with = "serialize_optional_vec")]
            metrics: Vec<Option<HistoryMetric>>,
        }

        let flushes = fetch_do_get_as_type::<Vec<Flush>>(&state.flight_client, query).await?;

        Ok(Json(flushes))
    }

    fn recordbatches_as_type<T: serde::de::DeserializeOwned + Debug + Default>(
        batches: &[RecordBatch],
    ) -> anyhow::Result<T> {
        if !batches.is_empty() {
            let buf = Vec::new();
            let mut writer = WriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(buf);
            let batch_refs: Vec<&_> = batches.iter().collect();

            if let Err(e) = writer.write_batches(&batch_refs) {
                bail!(e);
            }

            if let Err(e) = writer.finish() {
                bail!(e);
            }

            serde_json::from_slice(&writer.into_inner()).map_err(|e| anyhow!(e.to_string()))
        } else {
            Ok(T::default())
        }
    }

    async fn fetch_do_get_as_type<T: serde::de::DeserializeOwned + Debug + Default>(
        flight_client: &Mutex<FlightClient>,
        query: DoGetQuery,
    ) -> Result<T, (StatusCode, String)> {
        let serialized: Vec<u8> = serde_json::to_vec(&query).unwrap();
        let ticket = Ticket::new(serialized);

        let ticket_result = flight_client.lock().await.do_get(ticket).await;
        let Ok(batch_stream) = ticket_result else {
            let error = ticket_result.unwrap_err().to_string();
            log::error!("{error}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to fetch ticket results".to_string(),
            ));
        };

        let batches_result = batch_stream.try_collect::<Vec<_>>().await;
        let Ok(batches) = batches_result else {
            let error = batches_result.unwrap_err().to_string();
            log::error!("{error}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to extract record batches from stream".to_string(),
            ));
        };

        let typed_value_result = recordbatches_as_type::<T>(&batches);
        let Ok(typed_value) = typed_value_result else {
            let error = typed_value_result.unwrap_err().to_string();
            log::error!("{error}");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to convert record batches to JSON value".to_string(),
            ));
        };

        Ok(typed_value)
    }
}
