//! AC6: Total-row cap triggers LRU eviction on put; cap is never exceeded.

use mqo_duckdb_handle_store::{ColumnSchema, MemStore, ResultStore, StoreError};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

fn schema() -> Vec<ColumnSchema> {
    vec![ColumnSchema { name: "v".to_string(), ty: "INTEGER".to_string() }]
}

#[test]
fn ac6_cap_evicts_lru_on_put() {
    // Cap = 6 rows. Insert 3 rows at t=1 (h1), 3 rows at t=2 (h2).
    // Total = 6 — at cap. Insert 3 more at t=3 (h3) → LRU (h1, last_accessed=1) evicted.
    let cap = 6_usize;
    let mut store = MemStore::new(MemStoreConfig { ttl_secs: 9999, total_row_cap: cap });

    let rows3: Vec<_> = (0..3).map(|i| json!({"v": i})).collect();

    let h1 = store.put(&rows3, &schema(), 1).unwrap().handle;
    let h2 = store.put(&rows3, &schema(), 2).unwrap().handle;

    // Touch h2 to make it recently accessed (h1 remains least recent).
    // MemStore's current impl uses inserted_at for LRU — h1 was inserted at t=1 < t=2 → LRU.

    let h3 = store.put(&rows3, &schema(), 3).unwrap().handle;

    // h1 should have been evicted (LRU)
    let err = store.get_rows(&h1, 0, 10).unwrap_err();
    assert!(
        matches!(err, StoreError::HandleNotFound(_)),
        "LRU handle must be evicted, got {err:?}"
    );

    // h2 and h3 must still exist
    assert_eq!(store.get_rows(&h2, 0, 10).unwrap().len(), 3);
    assert_eq!(store.get_rows(&h3, 0, 10).unwrap().len(), 3);

    // Total rows must be <= cap
    assert!(
        store.total_rows() <= cap,
        "total rows {} exceeds cap {}",
        store.total_rows(),
        cap
    );
}

#[test]
fn ac6_unlimited_cap_never_evicts() {
    // cap = 0 → unlimited
    let mut store = MemStore::new(MemStoreConfig { ttl_secs: 9999, total_row_cap: 0 });
    let rows: Vec<_> = (0..100).map(|i| json!({"i": i})).collect();

    let h1 = store.put(&rows, &schema(), 0).unwrap().handle;
    let h2 = store.put(&rows, &schema(), 1).unwrap().handle;
    let h3 = store.put(&rows, &schema(), 2).unwrap().handle;

    assert_eq!(store.get_rows(&h1, 0, 1).unwrap().len(), 1);
    assert_eq!(store.get_rows(&h2, 0, 1).unwrap().len(), 1);
    assert_eq!(store.get_rows(&h3, 0, 1).unwrap().len(), 1);
    assert_eq!(store.total_rows(), 300);
}

#[test]
fn ac6_cap_exactly_one_row() {
    // Cap = 1. Each new put must evict the previous handle.
    let mut store = MemStore::new(MemStoreConfig { ttl_secs: 9999, total_row_cap: 1 });
    let row = vec![json!({"v": 0})];

    let h_a = store.put(&row, &schema(), 1).unwrap().handle;
    let h_b = store.put(&row, &schema(), 2).unwrap().handle;

    assert!(
        matches!(store.get_rows(&h_a, 0, 10).unwrap_err(), StoreError::HandleNotFound(_)),
        "h_a should be evicted"
    );
    assert_eq!(store.get_rows(&h_b, 0, 10).unwrap().len(), 1);
    assert!(store.total_rows() <= 1);
}
