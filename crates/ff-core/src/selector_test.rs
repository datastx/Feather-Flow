use super::*;

#[test]
fn test_parse_model_selector() {
    let s = Selector::parse("my_model").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::None);
            assert_eq!(descendant_depth, TraversalDepth::None);
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
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Unlimited);
            assert_eq!(descendant_depth, TraversalDepth::None);
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
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::None);
            assert_eq!(descendant_depth, TraversalDepth::Unlimited);
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
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Unlimited);
            assert_eq!(descendant_depth, TraversalDepth::Unlimited);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_bounded_ancestor() {
    let s = Selector::parse("1+my_model").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Bounded(1));
            assert_eq!(descendant_depth, TraversalDepth::None);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_bounded_descendant() {
    let s = Selector::parse("my_model+2").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::None);
            assert_eq!(descendant_depth, TraversalDepth::Bounded(2));
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_bounded_both() {
    let s = Selector::parse("1+my_model+3").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Bounded(1));
            assert_eq!(descendant_depth, TraversalDepth::Bounded(3));
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_unbounded_ancestor_bounded_descendant() {
    let s = Selector::parse("+my_model+2").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Unlimited);
            assert_eq!(descendant_depth, TraversalDepth::Bounded(2));
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_bounded_ancestor_unbounded_descendant() {
    let s = Selector::parse("2+my_model+").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Bounded(2));
            assert_eq!(descendant_depth, TraversalDepth::Unlimited);
        }
        _ => panic!("Expected Model selector"),
    }
}

#[test]
fn test_parse_zero_bounded() {
    let s = Selector::parse("0+my_model").unwrap();
    match s.selector_type {
        SelectorType::Model {
            name,
            ancestor_depth,
            descendant_depth,
        } => {
            assert_eq!(name, "my_model");
            assert_eq!(ancestor_depth, TraversalDepth::Bounded(0));
            assert_eq!(descendant_depth, TraversalDepth::None);
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

// -----------------------------------------------------------------------
// apply_selectors integration tests
// -----------------------------------------------------------------------

use crate::model::{ModelConfig, ModelKind};
use crate::table_name::TableName;
use std::path::PathBuf;

/// Construct a minimal `Model` for testing purposes.
fn make_test_model(name: &str) -> Model {
    Model {
        name: ModelName::new(name),
        path: PathBuf::from(format!("models/{}/{}.sql", name, name)),
        raw_sql: format!("SELECT 1 AS {}", name),
        compiled_sql: None,
        config: ModelConfig::default(),
        depends_on: HashSet::new(),
        external_deps: HashSet::<TableName>::new(),
        schema: None,
        base_name: None,
        version: None,
        kind: ModelKind::default(),
    }
}

/// Build a 3-node DAG (raw → stg → fct) and the corresponding models map.
fn build_test_dag_and_models() -> (HashMap<ModelName, Model>, crate::dag::ModelDag) {
    let models: HashMap<ModelName, Model> = ["raw", "stg", "fct"]
        .iter()
        .map(|n| (ModelName::new(*n), make_test_model(n)))
        .collect();

    let mut deps = HashMap::new();
    deps.insert("raw".to_string(), vec![]);
    deps.insert("stg".to_string(), vec!["raw".to_string()]);
    deps.insert("fct".to_string(), vec!["stg".to_string()]);

    let dag = crate::dag::ModelDag::build(&deps).unwrap();
    (models, dag)
}

#[test]
fn test_apply_selectors_none_returns_all() {
    let (models, dag) = build_test_dag_and_models();
    let result = apply_selectors(&None, &models, &dag).unwrap();
    assert_eq!(result.len(), 3);
    // Must be in topo order: raw before stg before fct
    let raw_pos = result.iter().position(|m| m == "raw").unwrap();
    let stg_pos = result.iter().position(|m| m == "stg").unwrap();
    let fct_pos = result.iter().position(|m| m == "fct").unwrap();
    assert!(raw_pos < stg_pos);
    assert!(stg_pos < fct_pos);
}

#[test]
fn test_apply_selectors_empty_string() {
    let (models, dag) = build_test_dag_and_models();
    let result = apply_selectors(&Some("".to_string()), &models, &dag).unwrap();
    assert!(result.is_empty());
}

#[test]
fn test_apply_selectors_comma_separated() {
    let (models, dag) = build_test_dag_and_models();
    let result = apply_selectors(&Some("raw,fct".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains(&"raw".to_string()));
    assert!(result.contains(&"fct".to_string()));
}

#[test]
fn test_apply_selectors_with_spaces() {
    let (models, dag) = build_test_dag_and_models();
    let result = apply_selectors(&Some(" raw , fct ".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains(&"raw".to_string()));
    assert!(result.contains(&"fct".to_string()));
}

#[test]
fn test_apply_selectors_bounded_traversal() {
    let (models, dag) = build_test_dag_and_models();
    // 1+fct = fct + 1 ancestor (stg)
    let result = apply_selectors(&Some("1+fct".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 2);
    assert!(result.contains(&"stg".to_string()));
    assert!(result.contains(&"fct".to_string()));
}

#[test]
fn test_apply_selectors_unbounded_traversal() {
    let (models, dag) = build_test_dag_and_models();
    // +fct = fct + all ancestors
    let result = apply_selectors(&Some("+fct".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.contains(&"raw".to_string()));
    assert!(result.contains(&"stg".to_string()));
    assert!(result.contains(&"fct".to_string()));
}

#[test]
fn test_apply_selectors_topo_order() {
    let (models, dag) = build_test_dag_and_models();
    // Even if "fct" is listed first, result must be in topo order
    let result = apply_selectors(&Some("fct,raw".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result[0], "raw");
    assert_eq!(result[1], "fct");
}

#[test]
fn test_apply_selectors_deduplication() {
    let (models, dag) = build_test_dag_and_models();
    let result = apply_selectors(&Some("stg,stg".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "stg");
}

#[test]
fn test_apply_selectors_trailing_comma() {
    let (models, dag) = build_test_dag_and_models();
    let result = apply_selectors(&Some("stg,".to_string()), &models, &dag).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0], "stg");
}
