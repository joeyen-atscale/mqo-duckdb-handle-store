//! Result-set handle store for mqo-mcp-server.
//!
//! Provides a `ResultStore` trait with two implementations:
//! - `MemStore`: in-memory HashMap with TTL + total-row-count cap + LRU eviction (default)
//! - `DuckStore`: in-process DuckDB backend (behind `--features duckdb`)
//!
//! Time is always injected as `now_unix: u64`; no `std::time::SystemTime` calls.
//! Zero `unsafe`.

pub mod mem_store;
pub use mem_store::MemStore;

#[cfg(feature = "duckdb")]
pub mod duck_store;
#[cfg(feature = "duckdb")]
pub use duck_store::DuckStore;

use serde::{Deserialize, Serialize};

/// Opaque handle identifying a stored result set (UUID string).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatasetHandle(pub String);

impl DatasetHandle {
    /// Create a new random UUID handle.
    pub fn new_random() -> Self {
        DatasetHandle(uuid::Uuid::new_v4().to_string())
    }
}

impl std::fmt::Display for DatasetHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Column name + type descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub ty: String,
}

/// Envelope returned to the LLM — contains the handle + metadata, NOT the rows.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleEnvelope {
    pub handle: DatasetHandle,
    pub row_count: usize,
    pub schema: Vec<ColumnSchema>,
}

/// Errors returned by `ResultStore` operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreError {
    /// The supplied handle was not found (may have been evicted or never stored).
    HandleNotFound(String),
    /// A backing-store operation failed; message describes the cause.
    BackendError(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::HandleNotFound(h) => write!(f, "handle not found: {h}"),
            StoreError::BackendError(m) => write!(f, "backend error: {m}"),
        }
    }
}

impl std::error::Error for StoreError {}

/// Core trait implemented by both `MemStore` and `DuckStore`.
pub trait ResultStore {
    /// Store `rows` under a freshly allocated handle and return its envelope.
    ///
    /// Every call allocates a new handle (immutable-derive semantics).
    /// If a total-row cap is configured and would be exceeded, LRU eviction runs
    /// before inserting.
    fn put(
        &mut self,
        rows: &[serde_json::Value],
        schema: &[ColumnSchema],
        now_unix: u64,
    ) -> Result<HandleEnvelope, StoreError>;

    /// Return a bounded slice of rows `[offset, offset+limit)`.
    ///
    /// An out-of-range `offset` (>= row_count) returns an empty `Vec`, not an error.
    fn get_rows(
        &self,
        h: &DatasetHandle,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>, StoreError>;

    /// Return the envelope (row_count + schema) without materialising any rows.
    fn metadata(&self, h: &DatasetHandle) -> Result<HandleEnvelope, StoreError>;

    /// Evict all handles whose insertion time is older than `now_unix - ttl_secs`.
    ///
    /// `now_unix` is caller-supplied; the store never reads a wall clock.
    fn evict_expired(&mut self, now_unix: u64);
}
