//! AC1: MemStore::put returns a HandleEnvelope whose row_count == rows.len()
//! and whose schema echoes the input; rows are NOT in the envelope.

use mqo_duckdb_handle_store::{ColumnSchema, MemStore, ResultStore};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

#[test]
fn ac1_envelope_row_count_and_schema_no_rows() {
    let mut store = MemStore::new(MemStoreConfig::default());
    let rows = vec![
        json!({"city": "NYC", "sales": 100}),
        json!({"city": "LA",  "sales": 200}),
        json!({"city": "SF",  "sales": 150}),
    ];
    let schema = vec![
        ColumnSchema { name: "city".to_string(),  ty: "STRING".to_string() },
        ColumnSchema { name: "sales".to_string(), ty: "INTEGER".to_string() },
    ];

    let env = store.put(&rows, &schema, 1_000_000).unwrap();

    // row_count must match input length
    assert_eq!(env.row_count, 3, "row_count must equal rows.len()");

    // schema must echo input exactly
    assert_eq!(env.schema.len(), 2);
    assert_eq!(env.schema[0].name, "city");
    assert_eq!(env.schema[0].ty, "STRING");
    assert_eq!(env.schema[1].name, "sales");
    assert_eq!(env.schema[1].ty, "INTEGER");

    // HandleEnvelope has no rows field — confirmed by struct definition
    // (the struct only has handle, row_count, schema)
}

#[test]
fn ac1_empty_rows_accepted() {
    let mut store = MemStore::with_defaults();
    let env = store.put(&[], &[], 0).unwrap();
    assert_eq!(env.row_count, 0);
    assert_eq!(env.schema.len(), 0);
    assert!(!env.handle.0.is_empty());
}
