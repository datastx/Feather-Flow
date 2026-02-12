use super::*;

#[test]
fn test_parse_model_selector() {
    let s = Selector::parse("my_model").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            include_ancestors,
            include_descendants,
        } => {
            assert_eq!(name, "my_model");
            assert!(!include_ancestors);
            assert!(!include_descendants);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_ancestor_selector() {
    let s = Selector::parse("+my_model").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            include_ancestors,
            include_descendants,
        } => {
            assert_eq!(name, "my_model");
            assert!(include_ancestors);
            assert!(!include_descendants);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_descendant_selector() {
    let s = Selector::parse("my_model+").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            include_ancestors,
            include_descendants,
        } => {
            assert_eq!(name, "my_model");
            assert!(!include_ancestors);
            assert!(include_descendants);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_both_selector() {
    let s = Selector::parse("+my_model+").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            include_ancestors,
            include_descendants,
        } => {
            assert_eq!(name, "my_model");
            assert!(include_ancestors);
            assert!(include_descendants);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_path_selector() {
    let s = Selector::parse("path:models/staging/*").unwrap();
    match s.selector_type {
        SelectorType::Path { pattern } => {
            assert_eq!(pattern, "models/staging/*");
        }
        _ => panic!("Expected Path selector"),
    }
}

#[test]
fn test_parse_tag_selector() {
    let s = Selector::parse("tag:daily").unwrap();
    match s.selector_type {
        SelectorType::Tag { tag } => {
            assert_eq!(tag, "daily");
        }
        _ => panic!("Expected Tag selector"),
    }
}

#[test]
fn test_parse_empty_path() {
    let result = Selector::parse("path:");
    assert!(result.is_err());
}

#[test]
fn test_parse_empty_tag() {
    let result = Selector::parse("tag:");
    assert!(result.is_err());
}

#[test]
fn test_matches_path_pattern_exact() {
    assert!(Selector::matches_path_pattern(
        Path::new("models/staging/stg_orders.sql"),
        "staging"
    ));
}

#[test]
fn test_matches_path_pattern_wildcard() {
    assert!(Selector::matches_path_pattern(
        Path::new("models/staging/stg_orders.sql"),
        "models/staging/*"
    ));
}

#[test]
fn test_matches_path_pattern_double_wildcard() {
    assert!(Selector::matches_path_pattern(
        Path::new("models/staging/subdir/stg_orders.sql"),
        "models/**/*.sql"
    ));
}

#[test]
fn test_parse_state_modified() {
    let s = Selector::parse("state:modified").unwrap();
    match s.selector_type {
        SelectorType::State {
            state_type,
            include_descendants,
        } => {
            assert_eq!(state_type, StateType::Modified);
            assert!(!include_descendants);
        }
        _ => panic!("Expected State selector"),
    }
}

#[test]
fn test_parse_state_modified_with_descendants() {
    let s = Selector::parse("state:modified+").unwrap();
    match s.selector_type {
        SelectorType::State {
            state_type,
            include_descendants,
        } => {
            assert_eq!(state_type, StateType::Modified);
            assert!(include_descendants);
        }
        _ => panic!("Expected State selector"),
    }
}

#[test]
fn test_parse_state_new() {
    let s = Selector::parse("state:new").unwrap();
    match s.selector_type {
        SelectorType::State {
            state_type,
            include_descendants,
        } => {
            assert_eq!(state_type, StateType::New);
            assert!(!include_descendants);
        }
        _ => panic!("Expected State selector"),
    }
}

#[test]
fn test_parse_state_invalid() {
    let result = Selector::parse("state:invalid");
    assert!(result.is_err());
}

#[test]
fn test_requires_state() {
    let model_selector = Selector::parse("my_model").unwrap();
    assert!(!model_selector.requires_state());

    let state_selector = Selector::parse("state:modified").unwrap();
    assert!(state_selector.requires_state());
}

#[test]
fn test_parse_owner_selector() {
    let s = Selector::parse("owner:data-team").unwrap();
    match s.selector_type {
        SelectorType::Owner { owner } => {
            assert_eq!(owner, "data-team");
        }
        _ => panic!("Expected Owner selector"),
    }
}

#[test]
fn test_parse_owner_selector_with_email() {
    let s = Selector::parse("owner:data-team@company.com").unwrap();
    match s.selector_type {
        SelectorType::Owner { owner } => {
            assert_eq!(owner, "data-team@company.com");
        }
        _ => panic!("Expected Owner selector"),
    }
}

#[test]
fn test_parse_empty_owner() {
    let result = Selector::parse("owner:");
    assert!(result.is_err());
}
