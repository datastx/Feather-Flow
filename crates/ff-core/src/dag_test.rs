use super::*;

#[test]
fn test_build_dag() {
    let mut deps = HashMap::new();
    deps.insert("stg_orders".to_string(), vec![]);
    deps.insert(
        "fct_orders".to_string(),
        vec!["stg_orders".to_string(), "stg_customers".to_string()],
    );
    deps.insert("stg_customers".to_string(), vec![]);

    let dag = ModelDag::build(&deps).unwrap();
    let order = dag.topological_order().unwrap();

    // fct_orders should come after stg_orders and stg_customers
    let fct_pos = order.iter().position(|m| m == "fct_orders").unwrap();
    let stg_orders_pos = order.iter().position(|m| m == "stg_orders").unwrap();
    let stg_customers_pos = order.iter().position(|m| m == "stg_customers").unwrap();

    assert!(fct_pos > stg_orders_pos);
    assert!(fct_pos > stg_customers_pos);
}

#[test]
fn test_circular_dependency() {
    let mut deps = HashMap::new();
    deps.insert("a".to_string(), vec!["b".to_string()]);
    deps.insert("b".to_string(), vec!["c".to_string()]);
    deps.insert("c".to_string(), vec!["a".to_string()]);

    let result = ModelDag::build(&deps);
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        CoreError::CircularDependency { .. }
    ));
}

#[test]
fn test_selector_ancestors() {
    let mut deps = HashMap::new();
    deps.insert("raw".to_string(), vec![]);
    deps.insert("stg".to_string(), vec!["raw".to_string()]);
    deps.insert("fct".to_string(), vec!["stg".to_string()]);

    let dag = ModelDag::build(&deps).unwrap();
    let selected = dag.select("+fct").unwrap();

    assert_eq!(selected.len(), 3);
    assert!(selected.contains(&"raw".to_string()));
    assert!(selected.contains(&"stg".to_string()));
    assert!(selected.contains(&"fct".to_string()));
}

#[test]
fn test_selector_descendants() {
    let mut deps = HashMap::new();
    deps.insert("raw".to_string(), vec![]);
    deps.insert("stg".to_string(), vec!["raw".to_string()]);
    deps.insert("fct".to_string(), vec!["stg".to_string()]);

    let dag = ModelDag::build(&deps).unwrap();
    let selected = dag.select("raw+").unwrap();

    assert_eq!(selected.len(), 3);
    assert!(selected.contains(&"raw".to_string()));
    assert!(selected.contains(&"stg".to_string()));
    assert!(selected.contains(&"fct".to_string()));
}

/// Build a 4-node linear chain: raw → stg → int → fct
fn build_linear_dag() -> ModelDag {
    let mut deps = HashMap::new();
    deps.insert("raw".to_string(), vec![]);
    deps.insert("stg".to_string(), vec!["raw".to_string()]);
    deps.insert("int".to_string(), vec!["stg".to_string()]);
    deps.insert("fct".to_string(), vec!["int".to_string()]);
    ModelDag::build(&deps).unwrap()
}

#[test]
fn test_ancestors_bounded_1() {
    let dag = build_linear_dag();
    let result = dag.ancestors_bounded("fct", 1);
    assert_eq!(result, vec!["int".to_string()]);
}

#[test]
fn test_ancestors_bounded_2() {
    let dag = build_linear_dag();
    let mut result = dag.ancestors_bounded("fct", 2);
    result.sort();
    assert_eq!(result, vec!["int".to_string(), "stg".to_string()]);
}

#[test]
fn test_ancestors_bounded_all() {
    let dag = build_linear_dag();
    let mut result = dag.ancestors_bounded("fct", 10);
    result.sort();
    assert_eq!(
        result,
        vec!["int".to_string(), "raw".to_string(), "stg".to_string()]
    );
}

#[test]
fn test_ancestors_bounded_0() {
    let dag = build_linear_dag();
    let result = dag.ancestors_bounded("fct", 0);
    assert!(result.is_empty());
}

#[test]
fn test_descendants_bounded_1() {
    let dag = build_linear_dag();
    let result = dag.descendants_bounded("raw", 1);
    assert_eq!(result, vec!["stg".to_string()]);
}

#[test]
fn test_descendants_bounded_2() {
    let dag = build_linear_dag();
    let mut result = dag.descendants_bounded("raw", 2);
    result.sort();
    assert_eq!(result, vec!["int".to_string(), "stg".to_string()]);
}

#[test]
fn test_descendants_bounded_all() {
    let dag = build_linear_dag();
    let mut result = dag.descendants_bounded("raw", 10);
    result.sort();
    assert_eq!(
        result,
        vec!["fct".to_string(), "int".to_string(), "stg".to_string()]
    );
}

#[test]
fn test_descendants_bounded_0() {
    let dag = build_linear_dag();
    let result = dag.descendants_bounded("raw", 0);
    assert!(result.is_empty());
}

#[test]
fn test_self_dependency_filtered_by_build() {
    // Self-references (e.g. incremental models) are silently filtered out
    // during DAG construction so they don't create cycles.
    let mut deps = HashMap::new();
    deps.insert("a".to_string(), vec!["a".to_string()]);

    let dag = ModelDag::build(&deps).expect("self-dep should be filtered, not cause a cycle");
    assert!(dag.dependencies("a").is_empty());
}

#[test]
fn test_self_dependency_with_other_deps() {
    // A model that depends on itself AND another model: the self-ref is
    // filtered but the real dependency is preserved.
    let mut deps = HashMap::new();
    deps.insert("a".to_string(), vec![]);
    deps.insert("b".to_string(), vec!["a".to_string(), "b".to_string()]);

    let dag = ModelDag::build(&deps).expect("self-dep on b should be filtered");
    assert_eq!(dag.dependencies("b"), vec!["a".to_string()]);
    assert!(dag.dependencies("a").is_empty());
}

#[test]
fn test_bounded_nonexistent_model() {
    let dag = build_linear_dag();
    let result = dag.ancestors_bounded("nonexistent", 1);
    assert!(result.is_empty());
    let result = dag.descendants_bounded("nonexistent", 1);
    assert!(result.is_empty());
}
