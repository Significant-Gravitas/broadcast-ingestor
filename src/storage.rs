use bytes::Bytes;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::sync::Arc;

use crate::models::{ExportTraceServiceRequest, ResourceSpans, Span};

pub type GcsClient = Arc<dyn ObjectStore>;

pub fn build_gcs_client(bucket: &str) -> GcsClient {
    Arc::new(
        GoogleCloudStorageBuilder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("failed to build GCS client"),
    )
}

/// A single upload job: serialized JSON + the computed GCS path.
pub struct UploadJob {
    pub path: String,
    pub data: Bytes,
    pub session_id: String,
}

/// Extract all upload jobs from an OTLP request.
pub fn extract_jobs(req: &ExportTraceServiceRequest) -> Vec<UploadJob> {
    let raw = serde_json::to_vec(req).unwrap_or_default();
    let data = Bytes::from(raw);

    // Find the first span to derive path components.
    let (first_span, first_resource) = first_span(req);

    let trace_id = first_span
        .map(|s| s.trace_id.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown");

    let user_id = first_resource
        .and_then(|r| r.resource_attribute("user.id"))
        .or_else(|| first_span.and_then(|s| s.attribute("user.id")))
        .unwrap_or("unknown_user");

    let session_id = first_span
        .and_then(|s| s.attribute("session.id"))
        .unwrap_or("no_session");

    let start_secs = first_span.and_then(|s| s.start_time_secs()).unwrap_or(0);

    let date_str = if start_secs > 0 {
        unix_to_date(start_secs)
    } else {
        unix_to_date(now_secs())
    };

    let unix_str = if start_secs > 0 {
        start_secs.to_string()
    } else {
        now_secs().to_string()
    };

    let trace_label = if trace_id == "unknown" {
        uuid::Uuid::new_v4().to_string()
    } else {
        trace_id.to_string()
    };

    let path = format!(
        "{}/users/{}/sessions/{}/{}-{}.json",
        date_str,
        sanitize(user_id),
        sanitize(session_id),
        unix_str,
        sanitize(&trace_label),
    );

    vec![UploadJob {
        path,
        data,
        session_id: session_id.to_string(),
    }]
}

fn first_span(req: &ExportTraceServiceRequest) -> (Option<&Span>, Option<&ResourceSpans>) {
    for rs in &req.resource_spans {
        for ss in &rs.scope_spans {
            if let Some(span) = ss.spans.first() {
                return (Some(span), Some(rs));
            }
        }
    }
    (None, None)
}

pub async fn upload(client: &GcsClient, job: &UploadJob) -> Result<(), object_store::Error> {
    let path = ObjectPath::from(job.path.as_str());
    client
        .put(&path, PutPayload::from_bytes(job.data.clone()))
        .await?;
    Ok(())
}

/// Convert unix seconds to "YYYY-MM-DD" without chrono.
/// Uses the civil date algorithm from Howard Hinnant.
fn unix_to_date(secs: u64) -> String {
    let days = (secs / 86400) as i64;
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Replace path-unsafe characters.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unix_to_date() {
        assert_eq!(unix_to_date(0), "1970-01-01");
        assert_eq!(unix_to_date(1700000000), "2023-11-14");
        assert_eq!(unix_to_date(1609459200), "2021-01-01");
    }

    #[test]
    fn test_sanitize() {
        assert_eq!(sanitize("hello-world_123"), "hello-world_123");
        assert_eq!(sanitize("user@foo/bar"), "user_foo_bar");
    }

    #[test]
    fn test_extract_jobs_full_span() {
        let json = r#"{
            "resourceSpans": [{
                "resource": {
                    "attributes": [
                        {"key": "user.id", "value": {"stringValue": "user-42"}}
                    ]
                },
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "abc123",
                        "startTimeUnixNano": "1700000000000000000",
                        "attributes": [
                            {"key": "session.id", "value": {"stringValue": "sess-1"}}
                        ]
                    }]
                }]
            }]
        }"#;
        let req: ExportTraceServiceRequest = serde_json::from_str(json).unwrap();
        let jobs = extract_jobs(&req);
        assert_eq!(jobs.len(), 1);
        let path = &jobs[0].path;
        assert!(path.starts_with("2023-11-14/users/user-42/sessions/sess-1/1700000000-abc123.json"));
    }

    #[test]
    fn test_extract_jobs_missing_attributes() {
        let json = r#"{"resourceSpans": [{"scopeSpans": [{"spans": [{"traceId": "", "attributes": []}]}]}]}"#;
        let req: ExportTraceServiceRequest = serde_json::from_str(json).unwrap();
        let jobs = extract_jobs(&req);
        assert_eq!(jobs.len(), 1);
        let path = &jobs[0].path;
        // Should use fallbacks
        assert!(path.contains("unknown_user"));
        assert!(path.contains("no_session"));
    }

    #[test]
    fn test_extract_jobs_empty_request() {
        let json = r#"{"resourceSpans": []}"#;
        let req: ExportTraceServiceRequest = serde_json::from_str(json).unwrap();
        let jobs = extract_jobs(&req);
        assert_eq!(jobs.len(), 1);
        assert!(jobs[0].path.contains("unknown_user"));
    }
}
