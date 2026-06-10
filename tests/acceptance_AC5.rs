//! AC5: evict_expired drops a stale handle; get_rows on it returns HandleNotFound.
//! A fresher handle is kept. Time is injected (no wall-clock).

use mqo_duckdb_handle_store::{ColumnSchema, MemStore, ResultStore, StoreError};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

#[test]
fn ac5_evict_stale_keep_fresh() {
    let ttl = 3600_u64; // 1 hour
    let mut store = MemStore::new(MemStoreConfig { ttl_secs: ttl, total_row_cap: 0 });

    let schema: Vec<ColumnSchema> = vec![];
    let rows = vec![json!({"v": 1})];

    // Insert "old" handle at t=0
    let old_env = store.put(&rows, &schema, 0).unwrap();

    // Insert "fresh" handle at t=3600 (right at TTL boundary — still valid since we use <)
    let fresh_env = store.put(&rows, &schema, ttl).unwrap();

    // Evict at now=3601: old (age=3601) >= ttl=3600 → evicted; fresh (age=1) < ttl → kept
    store.evict_expired(ttl + 1);

    // Old handle must be gone
    let err = store.get_rows(&old_env.handle, 0, 10).unwrap_err();
    assert!(
        matches!(err, StoreError::HandleNotFound(_)),
        "evicted handle must return HandleNotFound, got {err:?}"
    );

    // Fresh handle must still be accessible
    let fresh_rows = store.get_rows(&fresh_env.handle, 0, 10).unwrap();
    assert_eq!(fresh_rows.len(), 1, "fresh handle must survive eviction");
}

#[test]
fn ac5_no_eviction_before_ttl() {
    let ttl = 1000_u64;
    let mut store = MemStore::new(MemStoreConfig { ttl_secs: ttl, total_row_cap: 0 });
    let schema: Vec<ColumnSchema> = vec![];

    let env = store.put(&[], &schema, 500).unwrap();

    // Evict at now=1000: age = 500 < ttl=1000 → kept
    store.evict_expired(1000);

    let result = store.get_rows(&env.handle, 0, 10);
    assert!(result.is_ok(), "handle should not be evicted before TTL");
}

#[test]
fn ac5_multiple_handles_selectively_evicted() {
    let ttl = 100_u64;
    let mut store = MemStore::new(MemStoreConfig { ttl_secs: ttl, total_row_cap: 0 });
    let schema: Vec<ColumnSchema> = vec![];

    // Insert 3 handles at different times
    let h0 = store.put(&[json!(0)], &schema, 0).unwrap().handle;
    let h50 = store.put(&[json!(50)], &schema, 50).unwrap().handle;
    let h99 = store.put(&[json!(99)], &schema, 99).unwrap().handle;

    // Evict at now=110: age(h0)=110 >= 100 → gone; age(h50)=60 < 100 → kept; age(h99)=11 < 100 → kept
    store.evict_expired(110);

    assert!(matches!(
        store.get_rows(&h0, 0, 10).unwrap_err(),
        StoreError::HandleNotFound(_)
    ));
    assert!(store.get_rows(&h50, 0, 10).is_ok());
    assert!(store.get_rows(&h99, 0, 10).is_ok());
}
