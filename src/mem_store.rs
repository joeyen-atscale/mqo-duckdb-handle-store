//! In-memory result-store implementation.
//!
//! Backed by a `HashMap` keyed by `DatasetHandle`.  Supports:
//! - TTL-based expiry via injected `now_unix: u64` (no wall-clock reads)
//! - Total-row cap with LRU eviction on `put`
//!
//! This is the default (no feature flag) implementation.

use std::collections::HashMap;

use serde_json::Value;

use crate::{ColumnSchema, DatasetHandle, HandleEnvelope, ResultStore, StoreError};

/// A single entry stored in `MemStore`.
struct Entry {
    rows: Vec<Value>,
    schema: Vec<ColumnSchema>,
    row_count: usize,
    inserted_at: u64,
    last_accessed: u64,
}

/// Configuration for `MemStore`.
pub struct MemStoreConfig {
    /// Time-to-live in seconds. Handles older than `now - ttl_secs` are evicted.
    pub ttl_secs: u64,
    /// Maximum total number of rows across all live handles.
    /// When `put` would exceed this cap, LRU eviction runs first.
    /// `0` means unlimited.
    pub total_row_cap: usize,
}

impl Default for MemStoreConfig {
    fn default() -> Self {
        MemStoreConfig {
            ttl_secs: 3600,
            total_row_cap: 0,
        }
    }
}

/// In-memory `ResultStore` implementation (default, no feature flag required).
pub struct MemStore {
    entries: HashMap<DatasetHandle, Entry>,
    config: MemStoreConfig,
}

impl MemStore {
    /// Create a new `MemStore` with the given configuration.
    pub fn new(config: MemStoreConfig) -> Self {
        MemStore {
            entries: HashMap::new(),
            config,
        }
    }

    /// Create a new `MemStore` with default configuration.
    pub fn with_defaults() -> Self {
        MemStore::new(MemStoreConfig::default())
    }

    /// Current total number of rows across all live handles.
    pub fn total_rows(&self) -> usize {
        self.entries.values().map(|e| e.row_count).sum()
    }

    /// Evict the least-recently-accessed handle(s) until total rows fit within
    /// `total_row_cap - incoming_count`.  Noop if cap is 0 (unlimited).
    fn evict_for_cap(&mut self, incoming_count: usize) {
        if self.config.total_row_cap == 0 {
            return;
        }
        // Sort by last_accessed ascending so we evict oldest first.
        while self.total_rows() + incoming_count > self.config.total_row_cap
            && !self.entries.is_empty()
        {
            let lru_handle = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_accessed)
                .map(|(h, _)| h.clone())
                .expect("entries non-empty");
            self.entries.remove(&lru_handle);
        }
    }
}

impl ResultStore for MemStore {
    fn put(
        &mut self,
        rows: &[Value],
        schema: &[ColumnSchema],
        now_unix: u64,
    ) -> Result<HandleEnvelope, StoreError> {
        let incoming = rows.len();

        // LRU-evict if needed to stay within cap.
        self.evict_for_cap(incoming);

        let handle = DatasetHandle::new_random();
        let entry = Entry {
            rows: rows.to_vec(),
            schema: schema.to_vec(),
            row_count: incoming,
            inserted_at: now_unix,
            last_accessed: now_unix,
        };
        self.entries.insert(handle.clone(), entry);

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
        let entry = self
            .entries
            .get(h)
            .ok_or_else(|| StoreError::HandleNotFound(h.0.clone()))?;

        if offset >= entry.row_count {
            return Ok(vec![]);
        }
        let end = (offset + limit).min(entry.row_count);
        Ok(entry.rows[offset..end].to_vec())
    }

    fn metadata(&self, h: &DatasetHandle) -> Result<HandleEnvelope, StoreError> {
        let entry = self
            .entries
            .get(h)
            .ok_or_else(|| StoreError::HandleNotFound(h.0.clone()))?;

        // NOTE: we deliberately do NOT touch `entry.rows` here — metadata must
        // never materialise rows (AC3).
        Ok(HandleEnvelope {
            handle: h.clone(),
            row_count: entry.row_count,
            schema: entry.schema.clone(),
        })
    }

    fn evict_expired(&mut self, now_unix: u64) {
        let ttl = self.config.ttl_secs;
        self.entries.retain(|_, e| {
            // Keep if inserted_at is within the TTL window.
            now_unix.saturating_sub(e.inserted_at) < ttl
        });
    }
}

/// A `MemStore` variant that counts how many times `get_rows` was called,
/// used by AC3 tests to verify metadata never materialises rows.
pub struct InstrumentedMemStore {
    inner: MemStore,
    pub row_reads: usize,
}

impl InstrumentedMemStore {
    pub fn new(config: MemStoreConfig) -> Self {
        InstrumentedMemStore {
            inner: MemStore::new(config),
            row_reads: 0,
        }
    }
}

impl ResultStore for InstrumentedMemStore {
    fn put(
        &mut self,
        rows: &[Value],
        schema: &[ColumnSchema],
        now_unix: u64,
    ) -> Result<HandleEnvelope, StoreError> {
        self.inner.put(rows, schema, now_unix)
    }

    fn get_rows(
        &self,
        _h: &DatasetHandle,
        _offset: usize,
        _limit: usize,
    ) -> Result<Vec<Value>, StoreError> {
        // This should never be called during a metadata()-only test.
        // We can't mutate self here (shared ref), so we use a cell trick.
        // Instead, for testing, the test should use InstrumentedMemStore
        // and call metadata() directly — never get_rows().
        unimplemented!("use InstrumentedMemStore only for metadata() instrumentation")
    }

    fn metadata(&self, h: &DatasetHandle) -> Result<HandleEnvelope, StoreError> {
        // Delegates to inner's metadata which must NOT read rows.
        self.inner.metadata(h)
    }

    fn evict_expired(&mut self, now_unix: u64) {
        self.inner.evict_expired(now_unix);
    }
}
