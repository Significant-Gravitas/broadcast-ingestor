use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::retry::{retry_delay, RetryEntry, RetryQueue, MAX_ATTEMPTS};
use crate::storage::{upload, GcsClient, UploadJob};

pub async fn run_retry_task(
    client: GcsClient,
    retry_queue: Arc<Mutex<RetryQueue>>,
    interval_secs: u64,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        let ready = retry_queue.lock().await.pop_ready();
        if ready.is_empty() {
            continue;
        }
        tracing::info!(count = ready.len(), "retrying failed uploads");
        for entry in ready {
            let job = UploadJob {
                path: entry.path.clone(),
                data: entry.data.clone(),
                session_id: entry.session_id.clone(),
            };
            if let Err(e) = upload(&client, &job).await {
                if entry.attempts >= MAX_ATTEMPTS {
                    tracing::warn!(
                        path = %entry.path,
                        attempts = entry.attempts,
                        error = %e,
                        "dropping retry entry after max attempts"
                    );
                } else {
                    let next = RetryEntry {
                        path: entry.path,
                        data: entry.data,
                        session_id: entry.session_id,
                        attempts: entry.attempts + 1,
                        retry_after: Instant::now() + retry_delay(entry.attempts),
                    };
                    retry_queue.lock().await.push(next);
                }
            } else {
                tracing::info!(path = %entry.path, "retry succeeded");
            }
        }
    }
}

pub async fn flush_retry_queue(client: &GcsClient, retry_queue: &Arc<Mutex<RetryQueue>>) {
    let entries = retry_queue.lock().await.drain_all();
    if entries.is_empty() {
        return;
    }
    tracing::info!(count = entries.len(), "flushing retry queue on shutdown");
    for entry in entries {
        let job = UploadJob {
            path: entry.path.clone(),
            data: entry.data.clone(),
            session_id: entry.session_id.clone(),
        };
        if let Err(e) = upload(client, &job).await {
            tracing::error!(path = %entry.path, error = %e, "failed to flush retry entry on shutdown");
        }
    }
}
