mod config;
mod models;
mod retry;
mod storage;
mod worker;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use config::Config;
use models::ExportTraceServiceRequest;
use retry::RetryQueue;
use storage::{extract_jobs, UploadJob};
use worker::{flush_retry_queue, run_retry_task};

struct AppState {
    tx: mpsc::Sender<UploadJob>,
    webhook_secret: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .json()
        .init();

    let config = Config::from_env();
    tracing::info!(
        bucket = ?config.gcs_bucket,
        listen = %config.listen_addr,
        workers = config.worker_count,
        channel = config.channel_capacity,
        retry_mb = config.retry_buffer_bytes / (1024 * 1024),
        "starting trace-ingestor"
    );

    let (tx, rx) = mpsc::channel::<UploadJob>(config.channel_capacity);

    let mut worker_handles = Vec::new();
    let mut retry_handle = None;

    // Only spin up GCS workers if a bucket is configured.
    if let Some(ref bucket) = config.gcs_bucket {
        let gcs_client = storage::build_gcs_client(bucket);
        let retry_queue = Arc::new(Mutex::new(RetryQueue::new(config.retry_buffer_bytes)));

        let shared_rx = Arc::new(Mutex::new(rx));

        for i in 0..config.worker_count {
            let client = gcs_client.clone();
            let rq = retry_queue.clone();
            let shared = shared_rx.clone();
            let handle = tokio::spawn(async move {
                worker_recv_loop(i, shared, client, rq).await;
            });
            worker_handles.push(handle);
        }

        let retry_client = gcs_client.clone();
        let retry_rq = retry_queue.clone();
        let retry_interval = config.retry_interval_secs;
        retry_handle = Some((
            tokio::spawn(async move {
                run_retry_task(retry_client, retry_rq, retry_interval).await;
            }),
            gcs_client,
            retry_queue,
        ));
    } else {
        tracing::warn!("GCS_BUCKET not set; ingest endpoint will accept but discard traces");
        tokio::spawn(async move {
            let mut rx = rx;
            while rx.recv().await.is_some() {}
        });
    }

    let state = Arc::new(AppState {
        tx,
        webhook_secret: config.webhook_secret,
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/v1/traces", post(ingest_traces))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .expect("failed to bind listener");

    tracing::info!("listening on {}", config.listen_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    tracing::info!("shutting down: draining channel");

    for handle in worker_handles {
        let _ = handle.await;
    }
    tracing::info!("workers drained");

    if let Some((handle, client, retry_queue)) = retry_handle {
        handle.abort();
        flush_retry_queue(&client, &retry_queue).await;
    }
    tracing::info!("shutdown complete");
}

async fn worker_recv_loop(
    id: usize,
    rx: Arc<Mutex<mpsc::Receiver<UploadJob>>>,
    client: storage::GcsClient,
    retry_queue: Arc<Mutex<RetryQueue>>,
) {
    tracing::info!(worker = id, "upload worker started");
    loop {
        let job = {
            let mut guard = rx.lock().await;
            guard.recv().await
        };
        match job {
            Some(job) => {
                if let Err(e) = storage::upload(&client, &job).await {
                    tracing::error!(
                        worker = id,
                        path = %job.path,
                        error = %e,
                        "GCS upload failed, queuing for retry"
                    );
                    let entry = retry::RetryEntry {
                        path: job.path,
                        data: job.data,
                        session_id: job.session_id,
                        attempts: 1,
                        retry_after: tokio::time::Instant::now() + retry::retry_delay(0),
                    };
                    retry_queue.lock().await.push(entry);
                } else {
                    tracing::debug!(worker = id, path = %job.path, "uploaded");
                }
            }
            None => {
                tracing::info!(worker = id, "upload worker stopped (channel closed)");
                break;
            }
        }
    }
}

async fn health() -> StatusCode {
    StatusCode::OK
}

async fn ingest_traces(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    // Test connection check
    if headers.contains_key("x-test-connection") {
        return StatusCode::OK;
    }

    // Auth check
    if let Some(ref secret) = state.webhook_secret {
        let provided = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        match provided {
            Some(token) if token == secret => {}
            _ => return StatusCode::UNAUTHORIZED,
        }
    }

    // Parse OTLP JSON
    let req: ExportTraceServiceRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(error = %e, "invalid JSON payload");
            return StatusCode::BAD_REQUEST;
        }
    };

    // Extract upload jobs
    let jobs = extract_jobs(&req);

    // Enqueue
    for job in jobs {
        match state.tx.try_send(job) {
            Ok(_) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!("channel full, applying backpressure");
                return StatusCode::SERVICE_UNAVAILABLE;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("channel closed unexpectedly");
                return StatusCode::INTERNAL_SERVER_ERROR;
            }
        }
    }

    StatusCode::ACCEPTED
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl+c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => tracing::info!("received ctrl+c"),
        _ = terminate => tracing::info!("received SIGTERM"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_app() -> (Router, mpsc::Receiver<UploadJob>) {
        let (tx, rx) = mpsc::channel::<UploadJob>(100);
        let state = Arc::new(AppState {
            tx,
            webhook_secret: None,
        });
        let router = Router::new()
            .route("/health", get(health))
            .route("/v1/traces", post(ingest_traces))
            .with_state(state);
        (router, rx)
    }

    fn test_app_with_secret(secret: &str) -> (Router, mpsc::Receiver<UploadJob>) {
        let (tx, rx) = mpsc::channel::<UploadJob>(100);
        let state = Arc::new(AppState {
            tx,
            webhook_secret: Some(secret.to_string()),
        });
        let router = Router::new()
            .route("/health", get(health))
            .route("/v1/traces", post(ingest_traces))
            .with_state(state);
        (router, rx)
    }

    #[tokio::test]
    async fn health_returns_200() {
        let (app, _rx) = test_app();
        let req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_connection_header_returns_200() {
        let (app, _rx) = test_app();
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .header("x-test-connection", "true")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn valid_trace_returns_202() {
        let (app, _rx) = test_app();
        let body = r#"{"resourceSpans":[]}"#;
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn invalid_json_returns_400() {
        let (app, _rx) = test_app();
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .body(Body::from("not json"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let (app, _rx) = test_app_with_secret("my-secret");
        let body = r#"{"resourceSpans":[]}"#;
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn valid_auth_returns_202() {
        let (app, _rx) = test_app_with_secret("my-secret");
        let body = r#"{"resourceSpans":[]}"#;
        let req = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .header("authorization", "Bearer my-secret")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn full_channel_returns_503() {
        let (tx, _rx) = mpsc::channel::<UploadJob>(1);
        let state = Arc::new(AppState {
            tx,
            webhook_secret: None,
        });
        let app = Router::new()
            .route("/v1/traces", post(ingest_traces))
            .with_state(state);

        let body = r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"traceId":"a","attributes":[]}]}]}]}"#;
        let req1 = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp1 = app.clone().oneshot(req1).await.unwrap();
        assert_eq!(resp1.status(), StatusCode::ACCEPTED);

        let req2 = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp2 = app.oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
}
