# mqo-duckdb-handle-store

Result-set handle store for `mqo-mcp-server`. Stores `run_query` result sets out of
the LLM context window and returns an opaque `{handle, row_count, schema}` envelope
instead. Rows are retrieved on demand via bounded `get_rows(handle, offset, limit)`.

## TL;DR

ATSCALE-49212 asks to "store MCP `run_query` result sets in an external resource and
return an opaque handle to the LLM instead of the full row data." This library is the
DuckDB instantiation of that idea, plus a lightweight pure-Rust `MemStore` default that
keeps CI fast. The DuckDB backend is an opt-in Cargo feature (`--features duckdb`) so
the heavy bundled C++ build is never pulled in unless explicitly requested.

## Usage

### Default: MemStore (no extra deps)

```rust
use mqo_duckdb_handle_store::{MemStore, ResultStore, ColumnSchema};
use mqo_duckdb_handle_store::mem_store::MemStoreConfig;
use serde_json::json;

let mut store = MemStore::new(MemStoreConfig {
    ttl_secs: 3600,    // evict handles older than 1 hour
    total_row_cap: 50_000,  // LRU-evict if total rows would exceed this
});

let rows = vec![json!({"city": "NYC", "sales": 100})];
let schema = vec![ColumnSchema { name: "city".into(), ty: "STRING".into() }];

// put() injects now_unix — no wall-clock reads
let env = store.put(&rows, &schema, /* now_unix */ 1_718_000_000).unwrap();
// env = HandleEnvelope { handle: DatasetHandle("...uuid..."), row_count: 1, schema: [...] }
// rows are NOT in the envelope

// fetch a slice
let slice = store.get_rows(&env.handle, 0, 10).unwrap();

// metadata without row materialisation
let meta = store.metadata(&env.handle).unwrap();

// TTL eviction (inject current time; store never reads SystemTime)
store.evict_expired(1_718_003_601);
```

### Opt-in: DuckStore (`--features duckdb`)

Add to `Cargo.toml`:

```toml
mqo-duckdb-handle-store = { version = "0.1", features = ["duckdb"] }
```

Then:

```rust
use mqo_duckdb_handle_store::{DuckStore, ResultStore};
use mqo_duckdb_handle_store::duck_store::DuckStoreConfig;

let mut store = DuckStore::with_defaults().unwrap();
// Same ResultStore trait — put/get_rows/metadata/evict_expired
// Each handle maps to a DuckDB table `_h_<uuid>`, enabling SQL ops over stored rows.
```

The `--features duckdb` build pulls in the `duckdb` crate with the `bundled` feature
(compiles a large C++ amalgamation). This is intentionally opt-in.

## Acceptance criteria

| AC | Status | Description |
|----|--------|-------------|
| AC1 | MUST | `put` returns envelope with `row_count == rows.len()`, schema echoed, no rows |
| AC2 | MUST | `get_rows(h, offset, limit)` returns exact slice; out-of-range offset → empty Vec |
| AC3 | MUST | `metadata` returns envelope without materialising rows |
| AC4 | MUST | Two identical `put` calls return distinct handles (immutable-derive) |
| AC5 | MUST | `evict_expired(now)` drops stale handles; `get_rows` returns `HandleNotFound` |
| AC6 | MUST | Total-row cap triggers LRU eviction; cap never exceeded |
| AC7 | SHOULD | `DuckStore` satisfies AC1–AC5 (behind `--features duckdb`); default build excludes DuckDB |
| AC8 | MUST | `cargo test` (default) passes; `cargo clippy -D warnings` clean; zero `unsafe` |

## Design notes

- **Time is injected** — `now_unix: u64` is a caller arg; no `std::time::SystemTime::now()` anywhere.
- **Immutable derive** — every `put` allocates a fresh UUID handle; nothing overwrites an existing one.
- **Feature gate** — `[features] duckdb = ["dep:duckdb"]` in Cargo.toml; default build is pure Rust.
- **Zero `unsafe`** — confirmed by `grep -r 'unsafe' src/`.
