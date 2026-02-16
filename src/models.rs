use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExportTraceServiceRequest {
    #[serde(default)]
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceSpans {
    pub resource: Option<Resource>,
    #[serde(default)]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeSpans {
    #[serde(default)]
    pub spans: Vec<Span>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Span {
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub start_time_unix_nano: Option<StringOrNumber>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValue {
    pub key: String,
    pub value: Option<AnyValue>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AnyValue {
    pub string_value: Option<String>,
    pub int_value: Option<StringOrNumber>,
}

/// OTLP JSON encodes numbers as strings, but we handle both forms.
#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum StringOrNumber {
    String(String),
    Number(u64),
}

impl StringOrNumber {
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            StringOrNumber::Number(n) => Some(*n),
            StringOrNumber::String(s) => s.parse().ok(),
        }
    }
}

impl Span {
    pub fn start_time_secs(&self) -> Option<u64> {
        self.start_time_unix_nano
            .as_ref()
            .and_then(|v| v.as_u64())
            .map(|nanos| nanos / 1_000_000_000)
    }

    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.iter().find_map(|kv| {
            if kv.key == key {
                kv.value
                    .as_ref()
                    .and_then(|v| v.string_value.as_deref())
            } else {
                None
            }
        })
    }
}

impl ResourceSpans {
    pub fn resource_attribute(&self, key: &str) -> Option<&str> {
        self.resource.as_ref().and_then(|r| {
            r.attributes.iter().find_map(|kv| {
                if kv.key == key {
                    kv.value
                        .as_ref()
                        .and_then(|v| v.string_value.as_deref())
                } else {
                    None
                }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_minimal_request() {
        let json = r#"{"resourceSpans":[]}"#;
        let req: ExportTraceServiceRequest = serde_json::from_str(json).unwrap();
        assert!(req.resource_spans.is_empty());
    }

    #[test]
    fn deserialize_span_with_string_nano() {
        let json = r#"{
            "traceId": "abc123",
            "startTimeUnixNano": "1700000000000000000",
            "attributes": [
                {"key": "user.id", "value": {"stringValue": "user-42"}},
                {"key": "session.id", "value": {"stringValue": "sess-1"}}
            ]
        }"#;
        let span: Span = serde_json::from_str(json).unwrap();
        assert_eq!(span.trace_id, "abc123");
        assert_eq!(span.start_time_secs(), Some(1700000000));
        assert_eq!(span.attribute("user.id"), Some("user-42"));
        assert_eq!(span.attribute("session.id"), Some("sess-1"));
    }

    #[test]
    fn deserialize_span_with_numeric_nano() {
        let json = r#"{
            "traceId": "def456",
            "startTimeUnixNano": 1700000000000000000,
            "attributes": []
        }"#;
        let span: Span = serde_json::from_str(json).unwrap();
        assert_eq!(span.start_time_secs(), Some(1700000000));
    }

    #[test]
    fn missing_attributes_return_none() {
        let json = r#"{"traceId": "x", "attributes": []}"#;
        let span: Span = serde_json::from_str(json).unwrap();
        assert_eq!(span.attribute("user.id"), None);
        assert_eq!(span.start_time_secs(), None);
    }
}
