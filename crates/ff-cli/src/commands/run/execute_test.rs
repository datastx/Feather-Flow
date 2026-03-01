use super::*;

#[test]
fn test_telemetry_event_json_format() {
    let result = ModelRunResult {
        model: "stg_customers".to_string(),
        status: RunStatus::Success,
        materialization: "table".to_string(),
        duration_secs: 0.142,
        error: None,
    };

    let status_str = match result.status {
        RunStatus::Success => "success",
        RunStatus::Error => "error",
        RunStatus::Skipped => "skipped",
    };

    let event = TelemetryEvent {
        event: "model_executed",
        model: &result.model,
        status: status_str,
        duration_ms: (result.duration_secs * 1000.0) as u64,
        row_count: Some(1000),
        timestamp: "2024-01-15T10:30:00Z".to_string(),
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"event\":\"model_executed\""));
    assert!(json.contains("\"model\":\"stg_customers\""));
    assert!(json.contains("\"status\":\"success\""));
    assert!(json.contains("\"duration_ms\":142"));
    assert!(json.contains("\"row_count\":1000"));
    assert!(json.contains("\"timestamp\":\"2024-01-15T10:30:00Z\""));
}

#[test]
fn test_telemetry_event_without_row_count() {
    let event = TelemetryEvent {
        event: "model_executed",
        model: "failed_model",
        status: "error",
        duration_ms: 50,
        row_count: None,
        timestamp: "2024-01-15T10:30:00Z".to_string(),
    };

    let json = serde_json::to_string(&event).unwrap();
    assert!(json.contains("\"status\":\"error\""));
    assert!(!json.contains("row_count"));
}
