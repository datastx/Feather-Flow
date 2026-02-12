use super::*;

#[test]
fn test_freshness_period_display() {
    assert_eq!(FreshnessPeriod::Minute.to_string(), "minute");
    assert_eq!(FreshnessPeriod::Hour.to_string(), "hour");
    assert_eq!(FreshnessPeriod::Day.to_string(), "day");
}

#[test]
fn test_freshness_threshold_conversions() {
    // Test various threshold conversions
    assert_eq!(
        FreshnessThreshold::new(1, FreshnessPeriod::Minute).to_seconds(),
        60
    );
    assert_eq!(
        FreshnessThreshold::new(1, FreshnessPeriod::Hour).to_seconds(),
        3600
    );
    assert_eq!(
        FreshnessThreshold::new(1, FreshnessPeriod::Day).to_seconds(),
        86400
    );

    // Test larger counts
    assert_eq!(
        FreshnessThreshold::new(24, FreshnessPeriod::Hour).to_seconds(),
        24 * 3600
    );
    assert_eq!(
        FreshnessThreshold::new(7, FreshnessPeriod::Day).to_seconds(),
        7 * 86400
    );
}
