//! AC3: metadata() returns the envelope without materialising rows.
//!
//! Verified by using InstrumentedMemStore which panics if get_rows is called —
//! a pure metadata() call must never touch the row data.

use mqo_duckdb_handle_store::{ColumnSchema, ResultStore};
use mqo_duckdb_handle_store::mem_store::{InstrumentedMemStore, MemStoreConfig};
use serde_json::json;

#[test]
fn ac3_metadata_does_not_read_rows() {
    let mut store = InstrumentedMemStore::new(MemStoreConfig::default());
    let rows = vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})];
    let schema = vec![ColumnSchema { name: "x".to_string(), ty: "INTEGER".to_string() }];

    let env = store.put(&rows, &schema, 0).unwrap();

    // metadata() must not internally call get_rows / touch rows
    let meta = store.metadata(&env.handle).unwrap();

    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.schema.len(), 1);
    assert_eq!(meta.schema[0].name, "x");
    // handle is echoed back
    assert_eq!(meta.handle, env.handle);
}

#[test]
fn ac3_metadata_on_missing_handle_returns_not_found() {
    use mqo_duckdb_handle_store::{DatasetHandle, StoreError};
    let store = InstrumentedMemStore::new(MemStoreConfig::default());
    let fake = DatasetHandle("00000000-0000-0000-0000-000000000000".to_string());
    let err = store.metadata(&fake).unwrap_err();
    assert!(
        matches!(err, StoreError::HandleNotFound(_)),
        "expected HandleNotFound, got {err:?}"
    );
}
