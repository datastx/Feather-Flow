//! Freshness configuration for SLA monitoring

use serde::{Deserialize, Serialize};

/// Freshness configuration for SLA monitoring
///
/// Defines when a model should be considered stale based on
/// the maximum value of a timestamp column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessConfig {
    /// Column containing row timestamps (e.g., "updated_at", "loaded_at")
    pub loaded_at_field: String,

    /// Threshold after which to show a warning
    #[serde(default)]
    pub warn_after: Option<FreshnessThreshold>,

    /// Threshold after which to show an error
    #[serde(default)]
    pub error_after: Option<FreshnessThreshold>,
}

/// A freshness threshold (count + period)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessThreshold {
    /// Number of periods
    pub count: u32,

    /// Time period unit
    pub period: FreshnessPeriod,
}

impl FreshnessThreshold {
    /// Create a new threshold
    pub fn new(count: u32, period: FreshnessPeriod) -> Self {
        Self { count, period }
    }

    /// Convert the threshold to seconds
    pub fn to_seconds(&self) -> u64 {
        const SECS_PER_MINUTE: u64 = 60;
        const SECS_PER_HOUR: u64 = 3600;
        const SECS_PER_DAY: u64 = 86_400;

        let period_seconds = match self.period {
            FreshnessPeriod::Minute => SECS_PER_MINUTE,
            FreshnessPeriod::Hour => SECS_PER_HOUR,
            FreshnessPeriod::Day => SECS_PER_DAY,
        };
        self.count as u64 * period_seconds
    }
}

/// Time period unit for freshness thresholds
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FreshnessPeriod {
    /// Minutes
    Minute,
    /// Hours
    Hour,
    /// Days
    Day,
}

impl std::fmt::Display for FreshnessPeriod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FreshnessPeriod::Minute => write!(f, "minute"),
            FreshnessPeriod::Hour => write!(f, "hour"),
            FreshnessPeriod::Day => write!(f, "day"),
        }
    }
}

#[cfg(test)]
#[path = "freshness_test.rs"]
mod tests;
