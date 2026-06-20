# mqo-duckdb-handle-store

A result-set store that keeps query rows out of an LLM's context window and hands back an opaque `{handle, row_count, schema}` envelope instead of the rows themselves.

## Why it exists

A `run_query` against a semantic layer can return thousands of rows. Streaming those rows into a model's context is the wrong default: it burns tokens, and the model rarely needs every row at once — it needs to know the result exists, how big it is, and what its columns are, then pull bounded slices on demand.

This library does that split. `put` stores the rows and returns only the envelope. The model reasons over the handle and the shape; rows come back through `get_rows(handle, offset, limit)`, never all at once. The store is the backing piece behind `mqo-mcp-server`'s handle-based result flow.

There are two implementations behind one trait. `MemStore` is pure Rust — a `HashMap` with TTL and an optional row cap — and is the default, so the common build pulls no heavy dependencies and CI stays fast. `DuckStore` puts the rows in an in-process DuckDB table per handle, which is the path that lets downstream operations run SQL over stored results; it is opt-in behind a Cargo feature so the bundled DuckDB C++ build is never compiled unless you ask for it.

## Install

```toml
# default: MemStore only, no DuckDB
mqo-duckdb-handle-store = "0.1"

# opt in to the DuckDB backend (compiles a bundled DuckDB; slow first build)
mqo-duckdb-handle-store = { version = "0.1", features = ["duckdb"] }
```

## Quickstart

Both backends implement the same `ResultStore` trait: `put` / `get_rows` / `metadata` / `evict_expired`.

```rust
use mqo_duckdb_handle_store::{MemStore, ResultStore, ColumnSchema};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

let mut store = MemStore::new(MemStoreConfig {
    ttl_secs: 3600,        // evict handles older than one hour
    total_row_cap: 50_000, // 0 = unlimited; otherwise LRU-evict to stay under
});

let rows = vec![json!({"city": "NYC", "sales": 100})];
let schema = vec![ColumnSchema { name: "city".into(), ty: "STRING".into() }];

// Store the rows; get back the envelope — the rows are NOT in it.
// `now_unix` is supplied by the caller; the store never reads a clock.
let env = store.put(&rows, &schema, 1_718_000_000).unwrap();
// env.handle, env.row_count == 1, env.schema

// Pull a bounded slice on demand.
let slice = store.get_rows(&env.handle, 0, 10).unwrap();

// Read shape (row_count + schema) without materialising any rows.
let meta = store.metadata(&env.handle).unwrap();

// Drop handles past their TTL — again, the time is injected.
store.evict_expired(1_718_003_601);
```

The DuckDB backend is a drop-in for the same trait:

```rust
use mqo_duckdb_handle_store::{DuckStore, ResultStore};

let mut store = DuckStore::with_defaults().unwrap(); // --features duckdb
```

## How it works

- **The envelope carries shape, not data.** `put` returns `{handle, row_count, schema}`. Rows only ever leave through `get_rows`, bounded by `offset`/`limit`. An out-of-range offset returns an empty `Vec`, not an error.
- **Handles are immutable.** Every `put` allocates a fresh UUID; nothing overwrites an existing handle.
- **Time is injected.** Every method that cares about time takes `now_unix: u64` from the caller. There is no `SystemTime::now()` in the crate, which makes TTL and eviction deterministic to test.
- **Eviction has two triggers.** `evict_expired(now)` drops handles past their TTL. A non-zero `total_row_cap` makes `put` LRU-evict — oldest-accessed first — until the incoming rows fit.
- **`metadata` never reads rows.** It returns the envelope from in-memory bookkeeping; in `DuckStore` it touches the meta map only, not the data tables.

`DuckStore` stores each handle's rows in its own table (`_h_<uuid>`), one row per record in a single `_row_json TEXT` column. That keeps storage schema-agnostic; SQL over a stored result reads the JSON via DuckDB's `json_extract` rather than typed columns. Tables are dropped on eviction and on `Drop`.

## Where it fits

Part of the **[mqo-mcp](https://github.com/joeyen-atscale/mqo-mcp)** fleet — the AtScale MQO/MCP engine for AI analytics. This crate is the storage layer for that server's handle-based result flow, the piece that keeps large `run_query` results addressable without putting them in the model's context.

## Status

Version 0.1. The default `MemStore` path is covered by the acceptance tests in `tests/` (run `cargo test`). The `DuckStore` backend is gated behind `--features duckdb` and tested separately (`cargo test --features duckdb`); the default build excludes DuckDB entirely. The crate contains no `unsafe`.
