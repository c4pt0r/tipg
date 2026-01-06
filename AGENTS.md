# Agent Instructions for pg-tikv

## Project Overview
pg-tikv is a PostgreSQL-compatible SQL layer on TiKV. Written in Rust with async/await.

## Build Commands

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo check              # Check compilation (faster, no codegen)
cargo run                # Run server (debug)
```

## Test Commands

```bash
# Run ALL unit tests
cargo test

# Run a SINGLE test by name (partial match)
cargo test test_parse_select
cargo test test_json

# Run tests in a specific module
cargo test sql::expr::tests
cargo test storage::encoding::tests
cargo test protocol::handler::tests

# Run tests with output visible
cargo test -- --nocapture

# Run integration tests (requires TiKV running)
python3 scripts/integration_test.py

# Run a specific SQL test file manually
PGPASSWORD=admin psql -h 127.0.0.1 -p 5433 -U admin -d postgres -f tests/05_transaction.sql
```

## Lint Commands

```bash
cargo clippy -- -D warnings   # Check for warnings (treated as errors in CI)
cargo fmt                     # Format code
cargo fmt -- --check          # Check formatting without modifying
```

## Code Style Guidelines

### Imports
Order imports in groups separated by blank lines:
1. Standard library (`std::`)
2. External crates (`anyhow::`, `tokio::`, etc.)
3. Internal modules (`crate::`, `super::`)

```rust
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::Mutex;

use crate::storage::TikvStore;
use crate::types::{Row, Value};
```

### Error Handling
- Use `anyhow::Result` for functions that can fail
- Use `?` operator for error propagation
- Create descriptive error messages with context

```rust
let schema = self.store.get_schema(txn, &table_name).await?
    .ok_or_else(|| anyhow!("Table '{}' not found", table_name))?;
```

### Naming Conventions
- Types: `PascalCase` (`TableSchema`, `ExecuteResult`)
- Functions/methods: `snake_case` (`execute_query`, `get_schema`)
- Constants: `SCREAMING_SNAKE_CASE` (`DEFAULT_PG_PORT`)
- Module files: `snake_case.rs`

### Async Functions
All I/O operations are async. TiKV operations require `&mut Transaction`.

```rust
pub async fn execute(&self, session: &mut Session, sql: &str) -> Result<ExecuteResult> {
    let txn = session.get_mut_txn().expect("Transaction must be active");
    self.store.scan(txn, &table_name).await?
}
```

### Type Definitions
Use `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]` for data types.

### Module Structure
```
src/
├── main.rs           # Entry point, TCP server
├── auth/             # Authentication & RBAC
├── pool.rs           # TiKV connection pooling
├── protocol/         # PostgreSQL wire protocol (pgwire handlers)
├── sql/              # SQL parsing & execution
│   ├── executor.rs   # Query execution
│   ├── expr.rs       # Expression evaluation
│   ├── parser.rs     # SQL parsing wrapper
│   ├── session.rs    # Transaction state management
│   └── aggregate.rs  # Aggregation functions
├── storage/          # TiKV storage layer
│   ├── encoding.rs   # Key-value encoding
│   └── tikv_store.rs # TiKV client wrapper
├── tls.rs            # TLS/SSL setup
└── types/            # Data types (Value, Row, Schema)
```

### Testing Patterns
Unit tests go in the same file with `#[cfg(test)]` module. Use descriptive names: `test_<feature>_<scenario>`.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tenant_username_dot() {
        let (ks, user) = parse_tenant_username("tenant_a.admin");
        assert_eq!(ks, Some("tenant_a".to_string()));
        assert_eq!(user, "admin");
    }
}
```

### Transaction Patterns
```rust
let is_autocommit = !session.is_in_transaction();
if is_autocommit { session.begin().await?; }
// ... execute statement ...
if is_autocommit {
    if result.is_ok() { session.commit().await?; }
    else { session.rollback().await?; }
}
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV PD addresses |
| `PG_PORT` | `5433` | PostgreSQL listen port |
| `PG_NAMESPACE` | (none) | Multi-tenant namespace prefix |
| `PG_KEYSPACE` | `default` | TiKV keyspace name |
| `PG_PASSWORD` | (none) | Fallback password |
| `PG_TLS_CERT` | (none) | TLS certificate path |
| `PG_TLS_KEY` | (none) | TLS private key path |
| `RUST_LOG` | `info` | Log level |

## Common Patterns

### Adding a New SQL Function
1. Add evaluation logic in `src/sql/expr.rs` in `eval_function()`
2. Add unit tests in the same file
3. Add integration test in `tests/` directory

### Adding a New Statement Type
1. Handle parsing in `execute()` method of `src/sql/executor.rs`
2. Match on `Statement::YourType` enum variant from sqlparser
3. Implement execution logic, use existing helpers

### Key Encoding
Data is stored in TiKV with prefixed keys:
- Schema: `_sys_schema_{table_name}`
- Row data: `t_{table_id}_{pk_value}`
- Index: `i_{table_id}_{index_id}_{idx_val}`

## Known Issues / Tech Debt
- `executor.rs` is 2000+ lines - needs splitting into ddl.rs, dml.rs, query.rs
- Duplicate expression evaluation: `eval_expr` vs `eval_expr_join`
- No query planner layer (optimizer selects index scan directly)
