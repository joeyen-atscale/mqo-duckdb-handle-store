//! AC2: get_rows returns the exact requested slice; out-of-range offset returns empty Vec.

use mqo_duckdb_handle_store::{ColumnSchema, MemStore, ResultStore};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

fn make_rows(n: usize) -> Vec<serde_json::Value> {
    (0..n).map(|i| json!({"i": i})).collect()
}

fn schema() -> Vec<ColumnSchema> {
    vec![ColumnSchema { name: "i".to_string(), ty: "INTEGER".to_string() }]
}

#[test]
fn ac2_full_slice() {
    let mut store = MemStore::new(MemStoreConfig::default());
    let rows = make_rows(10);
    let env = store.put(&rows, &schema(), 0).unwrap();

    let got = store.get_rows(&env.handle, 0, 10).unwrap();
    assert_eq!(got.len(), 10);
    for (i, row) in got.iter().enumerate() {
        assert_eq!(row["i"], json!(i));
    }
}

#[test]
fn ac2_offset_and_limit() {
    let mut store = MemStore::with_defaults();
    let rows = make_rows(10);
    let env = store.put(&rows, &schema(), 0).unwrap();

    // offset=3, limit=4 → rows[3..7]
    let got = store.get_rows(&env.handle, 3, 4).unwrap();
    assert_eq!(got.len(), 4);
    assert_eq!(got[0]["i"], json!(3));
    assert_eq!(got[3]["i"], json!(6));
}

#[test]
fn ac2_limit_clamped_at_end() {
    let mut store = MemStore::with_defaults();
    let rows = make_rows(5);
    let env = store.put(&rows, &schema(), 0).unwrap();

    // offset=3, limit=100 → rows[3..5] (only 2 rows left)
    let got = store.get_rows(&env.handle, 3, 100).unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["i"], json!(3));
    assert_eq!(got[1]["i"], json!(4));
}

#[test]
fn ac2_out_of_range_offset_returns_empty() {
    let mut store = MemStore::with_defaults();
    let rows = make_rows(5);
    let env = store.put(&rows, &schema(), 0).unwrap();

    // offset == row_count → empty, not an error
    let got = store.get_rows(&env.handle, 5, 10).unwrap();
    assert!(got.is_empty(), "out-of-range offset must return empty Vec");

    // offset >> row_count also empty
    let got2 = store.get_rows(&env.handle, 999, 10).unwrap();
    assert!(got2.is_empty());
}

#[test]
fn ac2_limit_zero_returns_empty() {
    let mut store = MemStore::with_defaults();
    let rows = make_rows(5);
    let env = store.put(&rows, &schema(), 0).unwrap();

    let got = store.get_rows(&env.handle, 0, 0).unwrap();
    assert!(got.is_empty());
}
