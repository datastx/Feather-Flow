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
