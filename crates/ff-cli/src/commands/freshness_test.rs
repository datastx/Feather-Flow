use super::*;

#[test]
fn test_parse_timestamp_various_formats() {
    assert!(common::parse_timestamp("2024-01-15 10:30:00").is_some());
    assert!(common::parse_timestamp("2024-01-15 10:30:00.123").is_some());
    assert!(common::parse_timestamp("2024-01-15T10:30:00Z").is_some());
    assert!(common::parse_timestamp("2024-01-15T10:30:00.123Z").is_some());
}

#[test]
fn test_period_to_seconds() {
    assert_eq!(period_to_seconds(30, &FreshnessPeriod::Minute), 1800);
    assert_eq!(period_to_seconds(2, &FreshnessPeriod::Hour), 7200);
    assert_eq!(period_to_seconds(1, &FreshnessPeriod::Day), 86400);
}

#[test]
fn test_determine_status_seconds() {
    // No thresholds - always pass
    assert_eq!(
        determine_status_seconds(Some(100), None, None),
        FreshnessStatus::Pass
    );

    // Under both thresholds
    assert_eq!(
        determine_status_seconds(Some(60), Some(120), Some(240)),
        FreshnessStatus::Pass
    );

    // Over warn but under error
    assert_eq!(
        determine_status_seconds(Some(180), Some(120), Some(240)),
        FreshnessStatus::Warn
    );

    // Over error
    assert_eq!(
        determine_status_seconds(Some(300), Some(120), Some(240)),
        FreshnessStatus::Error
    );

    // No age data
    assert_eq!(
        determine_status_seconds(None, Some(120), Some(240)),
        FreshnessStatus::RuntimeError
    );
}

#[test]
fn test_format_duration() {
    assert_eq!(format_duration(30), "30s");
    assert_eq!(format_duration(90), "1m 30s");
    assert_eq!(format_duration(7200), "2h 0m");
    assert_eq!(format_duration(172800), "2d 0h");
}
