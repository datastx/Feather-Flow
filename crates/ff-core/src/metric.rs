//! Metric definition and discovery
//!
//! This module provides types and functions for defining semantic layer metrics
//! on top of models, enabling consistent calculations across consumers.

use crate::error::{CoreError, CoreResult};
use crate::sql_utils::{quote_ident, quote_qualified};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Calculation type for metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricCalculation {
    /// Sum aggregation
    Sum,
    /// Count aggregation
    Count,
    /// Average aggregation
    Avg,
    /// Minimum value
    Min,
    /// Maximum value
    Max,
    /// Count distinct values
    CountDistinct,
}

impl std::fmt::Display for MetricCalculation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MetricCalculation::Sum => "SUM",
            MetricCalculation::Count => "COUNT",
            MetricCalculation::Avg => "AVG",
            MetricCalculation::Min => "MIN",
            MetricCalculation::Max => "MAX",
            MetricCalculation::CountDistinct => "COUNT_DISTINCT",
        };
        write!(f, "{}", s)
    }
}

/// A metric definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    /// Unique name of the metric
    pub name: String,
    /// Human-readable label
    #[serde(default)]
    pub label: Option<String>,
    /// Description of what the metric measures
    #[serde(default)]
    pub description: Option<String>,
    /// Base model this metric is built on
    pub model: String,
    /// Aggregation function to apply
    pub calculation: MetricCalculation,
    /// Column/expression to aggregate
    pub expression: String,
    /// Timestamp column for time-based analysis
    #[serde(default)]
    pub timestamp: Option<String>,
    /// Dimensions for grouping (GROUP BY columns)
    #[serde(default)]
    pub dimensions: Vec<String>,
    /// Filters to apply (WHERE conditions)
    #[serde(default)]
    pub filters: Vec<String>,
    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,
    /// Owner/maintainer of this metric
    #[serde(default)]
    pub owner: Option<String>,
    /// File path where this metric was defined
    #[serde(skip)]
    pub path: PathBuf,
}

/// Kind discriminator for metric files (must be "metric")
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MetricKind {
    /// The only valid kind value
    Metric,
}

/// Raw YAML structure for metric files
#[derive(Debug, Deserialize)]
struct MetricFile {
    /// Validated by serde during deserialization to ensure `kind: metric`
    #[allow(dead_code)]
    kind: MetricKind,
    name: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    description: Option<String>,
    model: String,
    calculation: MetricCalculation,
    expression: String,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    dimensions: Vec<String>,
    #[serde(default)]
    filters: Vec<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    owner: Option<String>,
}

impl Metric {
    /// Parse a metric from YAML content
    pub fn from_yaml(content: &str, path: &Path) -> CoreResult<Self> {
        let raw: MetricFile =
            serde_yaml::from_str(content).map_err(|e| CoreError::ConfigParseError {
                message: format!("{}: {}", path.display(), e),
            })?;

        // Validate name
        if raw.name.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: format!("{}: Metric name cannot be empty", path.display()),
            });
        }

        // Validate model
        if raw.model.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: format!("{}: Metric model cannot be empty", path.display()),
            });
        }

        // Validate expression
        if raw.expression.is_empty() {
            return Err(CoreError::ConfigInvalid {
                message: format!("{}: Metric expression cannot be empty", path.display()),
            });
        }

        Ok(Metric {
            name: raw.name,
            label: raw.label,
            description: raw.description,
            model: raw.model,
            calculation: raw.calculation,
            expression: raw.expression,
            timestamp: raw.timestamp,
            dimensions: raw.dimensions,
            filters: raw.filters,
            tags: raw.tags,
            owner: raw.owner,
            path: path.to_path_buf(),
        })
    }

    /// Load a metric from a file
    pub fn from_file(path: &Path) -> CoreResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| CoreError::IoWithPath {
            path: path.display().to_string(),
            source: e,
        })?;
        Self::from_yaml(&content, path)
    }

    /// Generate SQL for this metric
    ///
    /// Generates a SELECT query with the appropriate aggregation,
    /// dimensions, and filters.
    pub fn generate_sql(&self) -> String {
        let mut sql = String::from("SELECT\n");
        let quoted_alias = quote_ident(&self.name);

        // Add dimensions first (for GROUP BY)
        if !self.dimensions.is_empty() {
            for dim in &self.dimensions {
                sql.push_str(&format!("  {},\n", quote_ident(dim)));
            }
        }

        // Add the aggregation
        // Note: `self.expression` is a user-provided SQL expression (e.g. "amount",
        // "price * quantity") — it is NOT quoted because it may contain operators.
        let agg_sql = match self.calculation {
            MetricCalculation::CountDistinct => {
                format!("  COUNT(DISTINCT {}) AS {}", self.expression, quoted_alias)
            }
            _ => {
                format!(
                    "  {}({}) AS {}",
                    self.calculation, self.expression, quoted_alias
                )
            }
        };
        sql.push_str(&agg_sql);
        sql.push('\n');

        // FROM clause
        sql.push_str(&format!("FROM {}\n", quote_qualified(&self.model)));

        // WHERE clause (filters are user-provided SQL conditions, not quoted)
        if !self.filters.is_empty() {
            sql.push_str("WHERE ");
            sql.push_str(&self.filters.join("\n  AND "));
            sql.push('\n');
        }

        // GROUP BY clause
        if !self.dimensions.is_empty() {
            let quoted_dims: Vec<String> = self.dimensions.iter().map(|d| quote_ident(d)).collect();
            sql.push_str("GROUP BY ");
            sql.push_str(&quoted_dims.join(", "));
            sql.push('\n');
        }

        sql
    }

    /// Get the human-readable label, falling back to name
    pub fn display_label(&self) -> &str {
        self.label.as_deref().unwrap_or(&self.name)
    }
}

/// Minimal YAML probe to check the `kind` field without full deserialization
#[derive(Deserialize)]
struct MetricKindProbe {
    #[serde(default)]
    kind: Option<MetricKind>,
}

/// Discover metrics from a list of paths
///
/// Returns an error if duplicate metric names are found across files.
pub fn discover_metrics(paths: &[PathBuf]) -> CoreResult<Vec<Metric>> {
    let mut metrics = Vec::new();

    for path in paths {
        if !path.exists() || !path.is_dir() {
            continue;
        }

        // Find all .yml files
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let file_path = entry.path();
                if file_path
                    .extension()
                    .is_some_and(|ext| ext == "yml" || ext == "yaml")
                {
                    let content = match std::fs::read_to_string(&file_path) {
                        Ok(c) => c,
                        Err(_) => continue,
                    };

                    // Probe the kind field before full parse
                    let probe: MetricKindProbe = match serde_yaml::from_str(&content) {
                        Ok(p) => p,
                        Err(_) => continue,
                    };

                    if !matches!(probe.kind, Some(MetricKind::Metric)) {
                        continue;
                    }

                    match Metric::from_yaml(&content, &file_path) {
                        Ok(metric) => metrics.push(metric),
                        Err(_) => continue,
                    }
                }
            }
        }
    }

    // Detect duplicate metric names
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (idx, metric) in metrics.iter().enumerate() {
        if let Some(&prev_idx) = seen.get(&metric.name) {
            return Err(CoreError::MetricDuplicateName {
                name: metric.name.clone(),
                path1: metrics[prev_idx].path.display().to_string(),
                path2: metric.path.display().to_string(),
            });
        }
        seen.insert(metric.name.clone(), idx);
    }

    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_metric() {
        let yaml = r#"
version: "1"
kind: metric
name: total_revenue
model: fct_orders
calculation: sum
expression: order_amount
"#;
        let metric = Metric::from_yaml(yaml, Path::new("metrics/test.yml")).unwrap();
        assert_eq!(metric.name, "total_revenue");
        assert_eq!(metric.model, "fct_orders");
        assert_eq!(metric.calculation, MetricCalculation::Sum);
        assert_eq!(metric.expression, "order_amount");
        assert!(metric.dimensions.is_empty());
        assert!(metric.filters.is_empty());
    }

    #[test]
    fn test_parse_full_metric() {
        let yaml = r#"
version: "1"
kind: metric
name: total_revenue
label: Total Revenue
description: Sum of all order amounts
model: fct_orders
calculation: sum
expression: order_amount
timestamp: order_date
dimensions:
  - customer_segment
  - product_category
filters:
  - is_valid = true
tags:
  - finance
  - daily
owner: data-team@company.com
"#;
        let metric = Metric::from_yaml(yaml, Path::new("metrics/test.yml")).unwrap();
        assert_eq!(metric.name, "total_revenue");
        assert_eq!(metric.label, Some("Total Revenue".to_string()));
        assert_eq!(
            metric.description,
            Some("Sum of all order amounts".to_string())
        );
        assert_eq!(metric.model, "fct_orders");
        assert_eq!(metric.calculation, MetricCalculation::Sum);
        assert_eq!(metric.expression, "order_amount");
        assert_eq!(metric.timestamp, Some("order_date".to_string()));
        assert_eq!(
            metric.dimensions,
            vec!["customer_segment", "product_category"]
        );
        assert_eq!(metric.filters, vec!["is_valid = true"]);
        assert_eq!(metric.tags, vec!["finance", "daily"]);
        assert_eq!(metric.owner, Some("data-team@company.com".to_string()));
    }

    #[test]
    fn test_all_calculation_types() {
        let calculations = [
            ("sum", MetricCalculation::Sum),
            ("count", MetricCalculation::Count),
            ("avg", MetricCalculation::Avg),
            ("min", MetricCalculation::Min),
            ("max", MetricCalculation::Max),
            ("count_distinct", MetricCalculation::CountDistinct),
        ];

        for (calc_str, expected) in calculations {
            let yaml = format!(
                r#"
kind: metric
name: test_metric
model: test_model
calculation: {}
expression: test_col
"#,
                calc_str
            );
            let metric = Metric::from_yaml(&yaml, Path::new("test.yml")).unwrap();
            assert_eq!(metric.calculation, expected);
        }
    }

    #[test]
    fn test_generate_sql_simple() {
        let yaml = r#"
kind: metric
name: total_revenue
model: fct_orders
calculation: sum
expression: order_amount
"#;
        let metric = Metric::from_yaml(yaml, Path::new("test.yml")).unwrap();
        let sql = metric.generate_sql();

        assert!(sql.contains(r#"SUM(order_amount) AS "total_revenue""#));
        assert!(sql.contains(r#"FROM "fct_orders""#));
        assert!(!sql.contains("WHERE"));
        assert!(!sql.contains("GROUP BY"));
    }

    #[test]
    fn test_generate_sql_with_dimensions() {
        let yaml = r#"
kind: metric
name: total_revenue
model: fct_orders
calculation: sum
expression: order_amount
dimensions:
  - customer_segment
  - product_category
"#;
        let metric = Metric::from_yaml(yaml, Path::new("test.yml")).unwrap();
        let sql = metric.generate_sql();

        assert!(sql.contains(r#""customer_segment","#));
        assert!(sql.contains(r#""product_category","#));
        assert!(sql.contains(r#"SUM(order_amount) AS "total_revenue""#));
        assert!(sql.contains(r#"GROUP BY "customer_segment", "product_category""#));
    }

    #[test]
    fn test_generate_sql_with_filters() {
        let yaml = r#"
kind: metric
name: total_revenue
model: fct_orders
calculation: sum
expression: order_amount
filters:
  - is_valid = true
  - order_date >= '2024-01-01'
"#;
        let metric = Metric::from_yaml(yaml, Path::new("test.yml")).unwrap();
        let sql = metric.generate_sql();

        assert!(sql.contains("WHERE is_valid = true"));
        assert!(sql.contains("AND order_date >= '2024-01-01'"));
    }

    #[test]
    fn test_generate_sql_count_distinct() {
        let yaml = r#"
kind: metric
name: unique_customers
model: fct_orders
calculation: count_distinct
expression: customer_id
"#;
        let metric = Metric::from_yaml(yaml, Path::new("test.yml")).unwrap();
        let sql = metric.generate_sql();

        assert!(sql.contains(r#"COUNT(DISTINCT customer_id) AS "unique_customers""#));
    }

    #[test]
    fn test_invalid_kind() {
        let yaml = r#"
kind: model
name: test
model: test_model
calculation: sum
expression: col
"#;
        let result = Metric::from_yaml(yaml, Path::new("test.yml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_name() {
        let yaml = r#"
kind: metric
name: ""
model: test_model
calculation: sum
expression: col
"#;
        let result = Metric::from_yaml(yaml, Path::new("test.yml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_missing_model() {
        let yaml = r#"
kind: metric
name: test_metric
model: ""
calculation: sum
expression: col
"#;
        let result = Metric::from_yaml(yaml, Path::new("test.yml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_display_label() {
        let yaml1 = r#"
kind: metric
name: total_revenue
label: Total Revenue (USD)
model: fct_orders
calculation: sum
expression: order_amount
"#;
        let metric1 = Metric::from_yaml(yaml1, Path::new("test.yml")).unwrap();
        assert_eq!(metric1.display_label(), "Total Revenue (USD)");

        let yaml2 = r#"
kind: metric
name: total_revenue
model: fct_orders
calculation: sum
expression: order_amount
"#;
        let metric2 = Metric::from_yaml(yaml2, Path::new("test.yml")).unwrap();
        assert_eq!(metric2.display_label(), "total_revenue");
    }

    #[test]
    fn test_discover_metrics() {
        let temp = tempfile::TempDir::new().unwrap();
        let temp_dir = temp.path().to_path_buf();

        // Create a valid metric file
        std::fs::write(
            temp_dir.join("total_revenue.yml"),
            r#"
kind: metric
name: total_revenue
model: fct_orders
calculation: sum
expression: order_amount
"#,
        )
        .unwrap();

        // Create another valid metric file
        std::fs::write(
            temp_dir.join("unique_customers.yml"),
            r#"
kind: metric
name: unique_customers
model: fct_orders
calculation: count_distinct
expression: customer_id
"#,
        )
        .unwrap();

        // Create a non-metric file (should be ignored)
        std::fs::write(
            temp_dir.join("not_a_metric.yml"),
            r#"
kind: source
name: raw_data
"#,
        )
        .unwrap();

        let metrics = discover_metrics(std::slice::from_ref(&temp_dir)).unwrap();
        assert_eq!(metrics.len(), 2);
    }
}
