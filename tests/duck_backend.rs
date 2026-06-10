//! AC7: DuckStore satisfies the same ResultStore contract (AC1/AC2/AC3 against DuckStore).
//!
//! These tests compile and run only when `--features duckdb` is specified.
//! The default build (no feature) excludes this file entirely.

#[cfg(feature = "duckdb")]
mod duck_tests {
    use mqo_duckdb_handle_store::{ColumnSchema, DuckStore, ResultStore, StoreError};
    use mqo_duckdb_handle_store::duck_store::DuckStoreConfig;
    use serde_json::json;

    fn schema() -> Vec<ColumnSchema> {
        vec![
            ColumnSchema { name: "city".to_string(), ty: "STRING".to_string() },
            ColumnSchema { name: "sales".to_string(), ty: "INTEGER".to_string() },
        ]
    }

    // AC1 via DuckStore
    #[test]
    fn duck_ac1_envelope_row_count_and_schema() {
        let mut store = DuckStore::with_defaults().unwrap();
        let rows = vec![
            json!({"city": "NYC", "sales": 100}),
            json!({"city": "LA",  "sales": 200}),
        ];
        let env = store.put(&rows, &schema(), 0).unwrap();
        assert_eq!(env.row_count, 2);
        assert_eq!(env.schema.len(), 2);
        assert_eq!(env.schema[0].name, "city");
        assert!(!env.handle.0.is_empty());
    }

    // AC2 via DuckStore
    #[test]
    fn duck_ac2_get_rows_slice() {
        let mut store = DuckStore::with_defaults().unwrap();
        let rows: Vec<_> = (0..10).map(|i| json!({"i": i})).collect();
        let schema = vec![ColumnSchema { name: "i".to_string(), ty: "INTEGER".to_string() }];
        let env = store.put(&rows, &schema, 0).unwrap();

        let got = store.get_rows(&env.handle, 2, 3).unwrap();
        assert_eq!(got.len(), 3);

        // out-of-range returns empty
        let empty = store.get_rows(&env.handle, 999, 10).unwrap();
        assert!(empty.is_empty());
    }

    // AC3 via DuckStore — metadata does not touch data table
    #[test]
    fn duck_ac3_metadata_no_rows() {
        let mut store = DuckStore::with_defaults().unwrap();
        let rows = vec![json!({"x": 1}), json!({"x": 2})];
        let schema = vec![ColumnSchema { name: "x".to_string(), ty: "INTEGER".to_string() }];
        let env = store.put(&rows, &schema, 0).unwrap();

        let meta = store.metadata(&env.handle).unwrap();
        assert_eq!(meta.row_count, 2);
        assert_eq!(meta.handle, env.handle);
    }

    // AC4 via DuckStore — two puts → two distinct handles
    #[test]
    fn duck_ac4_distinct_handles() {
        let mut store = DuckStore::with_defaults().unwrap();
        let rows = vec![json!({"v": 1})];
        let schema: Vec<ColumnSchema> = vec![];
        let e1 = store.put(&rows, &schema, 0).unwrap();
        let e2 = store.put(&rows, &schema, 0).unwrap();
        assert_ne!(e1.handle, e2.handle);
    }

    // AC5 via DuckStore — evict_expired
    #[test]
    fn duck_ac5_evict_expired() {
        let mut store = DuckStore::new(DuckStoreConfig { ttl_secs: 100, total_row_cap: 0 }).unwrap();
        let rows = vec![json!({"v": 1})];
        let schema: Vec<ColumnSchema> = vec![];

        let old = store.put(&rows, &schema, 0).unwrap().handle;
        let fresh = store.put(&rows, &schema, 99).unwrap().handle;

        store.evict_expired(110);

        assert!(matches!(
            store.get_rows(&old, 0, 10).unwrap_err(),
            StoreError::HandleNotFound(_)
        ));
        assert!(store.get_rows(&fresh, 0, 10).is_ok());
    }
}
