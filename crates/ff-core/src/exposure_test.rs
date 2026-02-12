use super::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_parse_minimal_exposure() {
    let yaml = r#"
version: 1
kind: exposure
name: revenue_dashboard
owner:
  name: Analytics Team
"#;
    let exposure = Exposure::from_yaml(yaml).unwrap();
    assert_eq!(exposure.name, "revenue_dashboard");
    assert_eq!(exposure.owner.name, "Analytics Team");
    assert_eq!(exposure.exposure_type, ExposureType::Dashboard);
    assert_eq!(exposure.maturity, ExposureMaturity::Medium);
}

#[test]
fn test_parse_full_exposure() {
    let yaml = r#"
version: 1
kind: exposure
name: revenue_dashboard
type: dashboard
owner:
  name: Analytics Team
  email: analytics@company.com
depends_on:
  - fct_orders
  - dim_customers
url: https://bi.company.com/dashboard/123
description: Executive revenue dashboard
maturity: high
tags:
  - executive
  - revenue
"#;
    let exposure = Exposure::from_yaml(yaml).unwrap();
    assert_eq!(exposure.name, "revenue_dashboard");
    assert_eq!(exposure.exposure_type, ExposureType::Dashboard);
    assert_eq!(exposure.owner.name, "Analytics Team");
    assert_eq!(
        exposure.owner.email,
        Some("analytics@company.com".to_string())
    );
    assert_eq!(exposure.depends_on.len(), 2);
    assert!(exposure.depends_on.contains(&"fct_orders".to_string()));
    assert!(exposure.depends_on.contains(&"dim_customers".to_string()));
    assert_eq!(
        exposure.url,
        Some("https://bi.company.com/dashboard/123".to_string())
    );
    assert_eq!(
        exposure.description,
        Some("Executive revenue dashboard".to_string())
    );
    assert_eq!(exposure.maturity, ExposureMaturity::High);
    assert_eq!(exposure.tags.len(), 2);
}

#[test]
fn test_exposure_types() {
    let cases = vec![
        ("dashboard", ExposureType::Dashboard),
        ("notebook", ExposureType::Notebook),
        ("ml_model", ExposureType::MlModel),
        ("application", ExposureType::Application),
        ("analysis", ExposureType::Analysis),
        ("other", ExposureType::Other),
    ];

    for (type_str, expected) in cases {
        let yaml = format!(
            r#"
version: 1
kind: exposure
name: test_exposure
type: {}
owner:
  name: Test
"#,
            type_str
        );
        let exposure = Exposure::from_yaml(&yaml).unwrap();
        assert_eq!(exposure.exposure_type, expected);
    }
}

#[test]
fn test_maturity_levels() {
    let cases = vec![
        ("high", ExposureMaturity::High),
        ("medium", ExposureMaturity::Medium),
        ("low", ExposureMaturity::Low),
    ];

    for (maturity_str, expected) in cases {
        let yaml = format!(
            r#"
version: 1
kind: exposure
name: test_exposure
maturity: {}
owner:
  name: Test
"#,
            maturity_str
        );
        let exposure = Exposure::from_yaml(&yaml).unwrap();
        assert_eq!(exposure.maturity, expected);
    }
}

#[test]
fn test_invalid_kind() {
    let yaml = r#"
version: 1
kind: model
name: not_an_exposure
owner:
  name: Test
"#;
    let result = Exposure::from_yaml(yaml);
    assert!(result.is_err());
}

#[test]
fn test_missing_name() {
    let yaml = r#"
version: 1
kind: exposure
name: ""
owner:
  name: Test
"#;
    let result = Exposure::from_yaml(yaml);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("name cannot be empty"));
}

#[test]
fn test_missing_owner_name() {
    let yaml = r#"
version: 1
kind: exposure
name: test_exposure
owner:
  name: ""
"#;
    let result = Exposure::from_yaml(yaml);
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("owner name"));
}

#[test]
fn test_depends_on_model() {
    let yaml = r#"
version: 1
kind: exposure
name: test_exposure
depends_on:
  - fct_orders
  - dim_customers
owner:
  name: Test
"#;
    let exposure = Exposure::from_yaml(yaml).unwrap();
    assert!(exposure.depends_on_model("fct_orders"));
    assert!(exposure.depends_on_model("dim_customers"));
    assert!(!exposure.depends_on_model("stg_orders"));
}

#[test]
fn test_from_file() {
    let temp = TempDir::new().unwrap();
    let exposure_path = temp.path().join("revenue_dashboard.yml");

    fs::write(
        &exposure_path,
        r#"
version: 1
kind: exposure
name: revenue_dashboard
type: dashboard
owner:
  name: Analytics Team
depends_on:
  - fct_orders
"#,
    )
    .unwrap();

    let exposure = Exposure::from_file(&exposure_path).unwrap();
    assert_eq!(exposure.name, "revenue_dashboard");
    assert_eq!(exposure.exposure_type, ExposureType::Dashboard);
    assert!(exposure.source_path.is_some());
}

#[test]
fn test_discover_exposures() {
    let temp = TempDir::new().unwrap();
    let exposures_dir = temp.path().join("exposures");
    fs::create_dir(&exposures_dir).unwrap();

    // Create two exposure files
    fs::write(
        exposures_dir.join("dashboard1.yml"),
        r#"
version: 1
kind: exposure
name: dashboard_one
owner:
  name: Team A
depends_on:
  - model_a
"#,
    )
    .unwrap();

    fs::write(
        exposures_dir.join("dashboard2.yaml"),
        r#"
version: 1
kind: exposure
name: dashboard_two
owner:
  name: Team B
depends_on:
  - model_b
"#,
    )
    .unwrap();

    // Create a non-exposure YAML file that should be skipped
    fs::write(
        exposures_dir.join("not_exposure.yml"),
        r#"
name: some_model
"#,
    )
    .unwrap();

    let exposures = discover_exposures(&[&exposures_dir]).unwrap();
    assert_eq!(exposures.len(), 2);

    let names: Vec<&str> = exposures.iter().map(|e| e.name.as_str()).collect();
    assert!(names.contains(&"dashboard_one"));
    assert!(names.contains(&"dashboard_two"));
}

#[test]
fn test_exposure_display_types() {
    assert_eq!(format!("{}", ExposureType::Dashboard), "dashboard");
    assert_eq!(format!("{}", ExposureType::MlModel), "ml_model");
    assert_eq!(format!("{}", ExposureMaturity::High), "high");
}
