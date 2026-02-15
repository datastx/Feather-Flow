use super::*;
use crate::types::{IntBitWidth, SqlType, TypedColumn};
use std::collections::HashMap;
use std::sync::Arc;

#[test]
fn test_provider_resolves_table() {
    let mut catalog: SchemaCatalog = HashMap::new();
    catalog.insert(
        "orders".to_string(),
        Arc::new(RelSchema::new(vec![TypedColumn {
            name: "id".to_string(),
            source_table: None,
            sql_type: SqlType::Integer {
                bits: IntBitWidth::I32,
            },
            nullability: Nullability::NotNull,
            provenance: vec![],
        }])),
    );

    let registry = FunctionRegistry::new();
    let provider = FeatherFlowProvider::new(&catalog, &registry);
    let result = provider.get_table_source(TableReference::bare("orders"));
    assert!(result.is_ok());
    let source = result.unwrap();
    assert_eq!(source.schema().fields().len(), 1);
    assert_eq!(source.schema().field(0).name(), "id");
}

#[test]
fn test_provider_unknown_table_errors() {
    let catalog: SchemaCatalog = HashMap::new();
    let registry = FunctionRegistry::new();
    let provider = FeatherFlowProvider::new(&catalog, &registry);
    let result = provider.get_table_source(TableReference::bare("nonexistent"));
    assert!(result.is_err());
}
