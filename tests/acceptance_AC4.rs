//! AC4: Two put calls with identical rows return two distinct handles (immutable derive).

use mqo_duckdb_handle_store::{ColumnSchema, MemStore, ResultStore};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

#[test]
fn ac4_two_puts_distinct_handles() {
    let mut store = MemStore::new(MemStoreConfig::default());
    let rows = vec![json!({"v": 42})];
    let schema = vec![ColumnSchema { name: "v".to_string(), ty: "INTEGER".to_string() }];

    let env1 = store.put(&rows, &schema, 0).unwrap();
    let env2 = store.put(&rows, &schema, 0).unwrap();

    assert_ne!(
        env1.handle, env2.handle,
        "identical puts must produce distinct handles"
    );

    // Both handles must be independently readable
    let r1 = store.get_rows(&env1.handle, 0, 10).unwrap();
    let r2 = store.get_rows(&env2.handle, 0, 10).unwrap();
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    assert_eq!(r1[0]["v"], json!(42));
    assert_eq!(r2[0]["v"], json!(42));
}

#[test]
fn ac4_many_puts_all_distinct() {
    let mut store = MemStore::with_defaults();
    let rows = vec![json!({"n": 1})];
    let schema: Vec<ColumnSchema> = vec![];

    let n = 20;
    let handles: Vec<_> = (0..n)
        .map(|_| store.put(&rows, &schema, 0).unwrap().handle)
        .collect();

    // All handles must be unique
    let mut seen = std::collections::HashSet::new();
    for h in &handles {
        assert!(seen.insert(h.0.clone()), "duplicate handle detected: {}", h.0);
    }
}
