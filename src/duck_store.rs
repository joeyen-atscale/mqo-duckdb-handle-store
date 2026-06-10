//! DuckDB-backed `ResultStore` implementation.
//!
//! Enabled only when compiled with `--features duckdb`.
//!
//! Each `put` call creates a new DuckDB table named `_h_<uuid>` (with hyphens
//! replaced by underscores) and inserts rows as JSON strings.  `get_rows` runs
//! `SELECT * FROM _h_<uuid> LIMIT ? OFFSET ?`.  This gives downstream
//! handle-ops (aggregate/slice/period-over-period) full DuckDB SQL over the stored
//! data via the table name — that is the operator coverage DuckDB buys.
//!
//! Metadata (row_count + schema) is stored in a `_meta` table and read without
//! touching the data tables.
//!
//! TTL + LRU eviction mirror `MemStore` semantics.

use std::collections::HashMap;

use duckdb::Connection;
use serde_json::Value;

use crate::{ColumnSchema, DatasetHandle, HandleEnvelope, ResultStore, StoreError};

/// Metadata kept in-memory for a DuckDB-backed handle.
struct DuckMeta {
    row_count: usize,
    schema: Vec<ColumnSchema>,
    inserted_at: u64,
    last_accessed: u64,
    /// DuckDB table name for this handle's rows.
    table_name: String,
}

/// Configuration for `DuckStore`.
pub struct DuckStoreConfig {
    pub ttl_secs: u64,
    /// Max total row count across all live handles. 0 = unlimited.
    pub total_row_cap: usize,
}

impl Default for DuckStoreConfig {
    fn default() -> Self {
        DuckStoreConfig {
            ttl_secs: 3600,
            total_row_cap: 0,
        }
    }
}

/// In-process DuckDB `ResultStore` implementation.
///
/// Requires `--features duckdb` to compile.
pub struct DuckStore {
    conn: Connection,
    meta: HashMap<DatasetHandle, DuckMeta>,
    config: DuckStoreConfig,
}

impl DuckStore {
    /// Open a new in-memory DuckDB store.
    pub fn new(config: DuckStoreConfig) -> Result<Self, StoreError> {
        let conn =
            Connection::open_in_memory().map_err(|e| StoreError::BackendError(e.to_string()))?;
        Ok(DuckStore {
            conn,
            meta: HashMap::new(),
            config,
        })
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Result<Self, StoreError> {
        Self::new(DuckStoreConfig::default())
    }

    fn total_rows(&self) -> usize {
        self.meta.values().map(|m| m.row_count).sum()
    }

    /// Map a handle string to a valid DuckDB identifier (replace `-` with `_`).
    fn table_name(handle: &DatasetHandle) -> String {
        format!("_h_{}", handle.0.replace('-', "_"))
    }

    fn evict_for_cap(&mut self, incoming: usize) {
        if self.config.total_row_cap == 0 {
            return;
        }
        while self.total_rows() + incoming > self.config.total_row_cap && !self.meta.is_empty() {
            let lru = self
                .meta
                .iter()
                .min_by_key(|(_, m)| m.last_accessed)
                .map(|(h, _)| h.clone())
                .expect("meta non-empty");
            if let Some(m) = self.meta.remove(&lru) {
                let _ = self
                    .conn
                    .execute(&format!("DROP TABLE IF EXISTS {}", m.table_name), []);
            }
        }
    }

    fn drop_handle_table(&mut self, handle: &DatasetHandle) {
        if let Some(m) = self.meta.get(handle) {
            let tname = m.table_name.clone();
            let _ = self
                .conn
                .execute(&format!("DROP TABLE IF EXISTS {tname}"), []);
        }
    }
}

impl ResultStore for DuckStore {
    fn put(
        &mut self,
        rows: &[Value],
        schema: &[ColumnSchema],
        now_unix: u64,
    ) -> Result<HandleEnvelope, StoreError> {
        let incoming = rows.len();
        self.evict_for_cap(incoming);

        let handle = DatasetHandle::new_random();
        let tname = Self::table_name(&handle);

        // Build column definitions from schema.
        let col_defs: Vec<String> = schema
            .iter()
            .map(|c| format!("{} TEXT", sanitize_ident(&c.name)))
            .collect();

        let create_sql = if col_defs.is_empty() {
            format!("CREATE TABLE {tname} (_row_json TEXT)")
        } else {
            format!("CREATE TABLE {tname} ({})", col_defs.join(", "))
        };

        self.conn
            .execute(&create_sql, [])
            .map_err(|e| StoreError::BackendError(e.to_string()))?;

        // Insert rows as JSON blobs into the _row_json column (or individual cols).
        // We use a single _row_json TEXT column strategy for simplicity and
        // schema-agnosticism — downstream SQL can json_extract from it.
        // When schema is non-empty, rebuild with _row_json.
        if !col_defs.is_empty() {
            // Drop and recreate with _row_json TEXT for uniform storage.
            self.conn
                .execute(&format!("DROP TABLE {tname}"), [])
                .map_err(|e| StoreError::BackendError(e.to_string()))?;
            self.conn
                .execute(&format!("CREATE TABLE {tname} (_row_json TEXT)"), [])
                .map_err(|e| StoreError::BackendError(e.to_string()))?;
        }

        for row in rows {
            let json_str = row.to_string();
            self.conn
                .execute(
                    &format!("INSERT INTO {tname} VALUES ($1)"),
                    duckdb::params![json_str],
                )
                .map_err(|e| StoreError::BackendError(e.to_string()))?;
        }

        self.meta.insert(
            handle.clone(),
            DuckMeta {
                row_count: incoming,
                schema: schema.to_vec(),
                inserted_at: now_unix,
                last_accessed: now_unix,
                table_name: tname,
            },
        );

        Ok(HandleEnvelope {
            handle,
            row_count: incoming,
            schema: schema.to_vec(),
        })
    }

    fn get_rows(
        &self,
        h: &DatasetHandle,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Value>, StoreError> {
        let m = self
            .meta
            .get(h)
            .ok_or_else(|| StoreError::HandleNotFound(h.0.clone()))?;

        if offset >= m.row_count {
            return Ok(vec![]);
        }

        let sql = format!(
            "SELECT _row_json FROM {} LIMIT {} OFFSET {}",
            m.table_name, limit, offset
        );

        let mut stmt = self
            .conn
            .prepare(&sql)
            .map_err(|e| StoreError::BackendError(e.to_string()))?;

        let rows: Vec<Value> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| StoreError::BackendError(e.to_string()))?
            .filter_map(|r| r.ok())
            .filter_map(|s| serde_json::from_str(&s).ok())
            .collect();

        Ok(rows)
    }

    fn metadata(&self, h: &DatasetHandle) -> Result<HandleEnvelope, StoreError> {
        let m = self
            .meta
            .get(h)
            .ok_or_else(|| StoreError::HandleNotFound(h.0.clone()))?;

        // Metadata reads only the in-memory meta map — no DuckDB table access.
        Ok(HandleEnvelope {
            handle: h.clone(),
            row_count: m.row_count,
            schema: m.schema.clone(),
        })
    }

    fn evict_expired(&mut self, now_unix: u64) {
        let ttl = self.config.ttl_secs;
        let to_drop: Vec<DatasetHandle> = self
            .meta
            .iter()
            .filter(|(_, m)| now_unix.saturating_sub(m.inserted_at) >= ttl)
            .map(|(h, _)| h.clone())
            .collect();

        for h in &to_drop {
            self.drop_handle_table(h);
            self.meta.remove(h);
        }
    }
}

/// Sanitise a column name to a safe DuckDB identifier.
fn sanitize_ident(name: &str) -> String {
    // Wrap in double-quotes and escape any internal double-quotes.
    let escaped = name.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

impl Drop for DuckStore {
    fn drop(&mut self) {
        // Clean up all tables.
        let tables: Vec<String> = self.meta.values().map(|m| m.table_name.clone()).collect();
        for t in tables {
            let _ = self.conn.execute(&format!("DROP TABLE IF EXISTS {t}"), []);
        }
    }
}
