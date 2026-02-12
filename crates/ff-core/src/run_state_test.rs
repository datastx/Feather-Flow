use super::*;
use tempfile::tempdir;

#[test]
fn test_run_state_new() {
    let state = RunState::new(
        vec![ModelName::new("model_a"), ModelName::new("model_b")],
        Some("--select model_a model_b".to_string()),
        "abc123".to_string(),
    );

    assert_eq!(state.pending_models.len(), 2);
    assert_eq!(state.completed_models.len(), 0);
    assert_eq!(state.failed_models.len(), 0);
    assert_eq!(state.status, RunStatus::Running);
}

#[test]
fn test_mark_completed() {
    let mut state = RunState::new(
        vec![ModelName::new("model_a"), ModelName::new("model_b")],
        None,
        "abc123".to_string(),
    );

    state.mark_completed("model_a", 1500);

    assert_eq!(state.pending_models.len(), 1);
    assert_eq!(state.completed_models.len(), 1);
    assert!(state.is_completed("model_a"));
    assert!(!state.is_completed("model_b"));
}

#[test]
fn test_mark_failed() {
    let mut state = RunState::new(
        vec![ModelName::new("model_a"), ModelName::new("model_b")],
        None,
        "abc123".to_string(),
    );

    state.mark_failed("model_a", "SQL error: invalid syntax");

    assert_eq!(state.pending_models.len(), 1);
    assert_eq!(state.failed_models.len(), 1);
    assert!(state.is_failed("model_a"));
    assert!(!state.is_failed("model_b"));
}

#[test]
fn test_models_to_run() {
    let mut state = RunState::new(
        vec![
            ModelName::new("model_a"),
            ModelName::new("model_b"),
            ModelName::new("model_c"),
        ],
        None,
        "abc123".to_string(),
    );

    state.mark_completed("model_a", 1000);
    state.mark_failed("model_b", "error");

    let to_run = state.models_to_run();
    assert_eq!(to_run.len(), 2);
    assert!(to_run.contains(&ModelName::new("model_b"))); // Failed, should retry
    assert!(to_run.contains(&ModelName::new("model_c"))); // Still pending
}

#[test]
fn test_save_and_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("run_state.json");

    let mut state = RunState::new(
        vec![ModelName::new("model_a"), ModelName::new("model_b")],
        Some("--select +model_b".to_string()),
        "abc123".to_string(),
    );
    state.mark_completed("model_a", 1500);

    state.save(&path).unwrap();

    let loaded = RunState::load(&path).unwrap().unwrap();
    assert_eq!(loaded.run_id, state.run_id);
    assert_eq!(loaded.completed_models.len(), 1);
    assert_eq!(loaded.pending_models.len(), 1);
}

#[test]
fn test_summary() {
    let mut state = RunState::new(
        vec![
            ModelName::new("model_a"),
            ModelName::new("model_b"),
            ModelName::new("model_c"),
        ],
        None,
        "abc123".to_string(),
    );

    state.mark_completed("model_a", 1000);
    state.mark_completed("model_b", 2000);
    state.mark_failed("model_c", "error");

    let summary = state.summary();
    assert_eq!(summary.completed, 2);
    assert_eq!(summary.failed, 1);
    assert_eq!(summary.pending, 0);
    assert_eq!(summary.total_duration_ms, 3000);
}
