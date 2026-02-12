use super::*;

#[test]
fn test_compute_checksum() {
    let checksum1 = compute_checksum("SELECT * FROM users");
    let checksum2 = compute_checksum("SELECT * FROM users");
    let checksum3 = compute_checksum("SELECT * FROM customers");

    assert_eq!(checksum1, checksum2);
    assert_ne!(checksum1, checksum3);
    assert_eq!(checksum1.len(), 64); // SHA256 produces 64 hex chars
}

#[test]
fn test_state_file_new() {
    let state = StateFile::new();
    assert!(state.models.is_empty());
}

#[test]
fn test_model_state_new() {
    let config = ModelStateConfig::new(
        Materialization::Incremental,
        Some("staging".to_string()),
        Some(vec!["id".to_string()]),
        Some(IncrementalStrategy::Merge),
        Some(OnSchemaChange::Ignore),
    );

    let state = ModelState::new(
        ModelName::new("my_model"),
        "SELECT * FROM users",
        Some(100),
        config,
    );

    assert_eq!(state.name, "my_model");
    assert_eq!(state.row_count, Some(100));
    assert!(!state.checksum.is_empty());
}

#[test]
fn test_is_model_modified() {
    let mut state_file = StateFile::new();

    let config = ModelStateConfig::new(Materialization::Table, None, None, None, None);
    let model_state = ModelState::new(
        ModelName::new("my_model"),
        "SELECT * FROM users",
        None,
        config,
    );

    state_file.upsert_model(model_state);

    // Same checksum should not be modified
    let same_checksum = compute_checksum("SELECT * FROM users");
    assert!(!state_file.is_model_modified("my_model", &same_checksum));

    // Different checksum should be modified
    let diff_checksum = compute_checksum("SELECT * FROM customers");
    assert!(state_file.is_model_modified("my_model", &diff_checksum));

    // Unknown model should be modified
    assert!(state_file.is_model_modified("unknown_model", &same_checksum));
}

#[test]
fn test_is_model_or_inputs_modified_new_model() {
    let state_file = StateFile::new();
    let inputs = HashMap::new();
    assert!(state_file.is_model_or_inputs_modified("new_model", "abc", None, &inputs));
}

#[test]
fn test_is_model_or_inputs_modified_unchanged() {
    let mut state_file = StateFile::new();
    let config = ModelStateConfig::new(Materialization::Table, None, None, None, None);
    let sql = "SELECT * FROM users";
    let schema = "version: 1\ncolumns: []";
    let mut inputs = HashMap::new();
    inputs.insert("upstream".to_string(), compute_checksum("SELECT 1"));

    let model_state = ModelState::new_with_checksums(
        ModelName::new("my_model"),
        sql,
        None,
        config,
        Some(compute_checksum(schema)),
        inputs.clone(),
    );
    state_file.upsert_model(model_state);

    assert!(!state_file.is_model_or_inputs_modified(
        "my_model",
        &compute_checksum(sql),
        Some(&compute_checksum(schema)),
        &inputs,
    ));
}

#[test]
fn test_is_model_or_inputs_modified_sql_changed() {
    let mut state_file = StateFile::new();
    let config = ModelStateConfig::new(Materialization::Table, None, None, None, None);
    let model_state = ModelState::new_with_checksums(
        ModelName::new("my_model"),
        "SELECT * FROM users",
        None,
        config,
        None,
        HashMap::new(),
    );
    state_file.upsert_model(model_state);

    assert!(state_file.is_model_or_inputs_modified(
        "my_model",
        &compute_checksum("SELECT * FROM customers"),
        None,
        &HashMap::new(),
    ));
}

#[test]
fn test_is_model_or_inputs_modified_schema_changed() {
    let mut state_file = StateFile::new();
    let config = ModelStateConfig::new(Materialization::Table, None, None, None, None);
    let sql = "SELECT * FROM users";
    let model_state = ModelState::new_with_checksums(
        ModelName::new("my_model"),
        sql,
        None,
        config,
        Some(compute_checksum("old schema")),
        HashMap::new(),
    );
    state_file.upsert_model(model_state);

    assert!(state_file.is_model_or_inputs_modified(
        "my_model",
        &compute_checksum(sql),
        Some(&compute_checksum("new schema")),
        &HashMap::new(),
    ));
}

#[test]
fn test_is_model_or_inputs_modified_input_changed() {
    let mut state_file = StateFile::new();
    let config = ModelStateConfig::new(Materialization::Table, None, None, None, None);
    let sql = "SELECT * FROM users";
    let mut old_inputs = HashMap::new();
    old_inputs.insert("upstream".to_string(), compute_checksum("SELECT 1"));

    let model_state = ModelState::new_with_checksums(
        ModelName::new("my_model"),
        sql,
        None,
        config,
        None,
        old_inputs,
    );
    state_file.upsert_model(model_state);

    let mut new_inputs = HashMap::new();
    new_inputs.insert("upstream".to_string(), compute_checksum("SELECT 2"));

    assert!(state_file.is_model_or_inputs_modified(
        "my_model",
        &compute_checksum(sql),
        None,
        &new_inputs,
    ));
}
