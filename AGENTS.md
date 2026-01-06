# pg-tikv Agent Instructions

PostgreSQL-compatible SQL layer on TiKV. Rust + async/await + pgwire + sqlparser.

## Quick Reference

```bash
cargo build                    # Debug build
cargo test                     # Unit tests (146 tests)
cargo clippy -- -D warnings    # Lint
python3 scripts/integration_test.py  # Integration tests (requires TiKV)
```

## Structure

```
src/
├── main.rs              # TCP server, startup, TLS setup
├── pool.rs              # TiKV client pool (per-keyspace)
├── tls.rs               # TLS/SSL config
├── auth/                # RBAC: users, roles, privileges
├── protocol/            # pgwire handlers, multi-tenant auth
├── sql/                 # SQL parsing & execution (see src/sql/AGENTS.md)
├── storage/             # TiKV client wrapper, key encoding
└── types/               # Value, Row, TableSchema, DataType
```

## Where to Look

| Task | Location |
|------|----------|
| Add SQL function | `src/sql/expr.rs` → `eval_function()` |
| Add statement type | `src/sql/executor.rs` → `execute_statement_on_txn()` |
| Change wire protocol | `src/protocol/handler.rs` |
| Modify key encoding | `src/storage/encoding.rs` |
| Add auth feature | `src/auth/rbac.rs` |

## Environment Variables

| Variable | Default | Notes |
|----------|---------|-------|
| `PD_ENDPOINTS` | `127.0.0.1:2379` | TiKV PD addresses |
| `PG_PORT` | `5433` | Listen port |
| `PG_KEYSPACE` | `default` | TiKV keyspace (API v2) |
| `PG_PASSWORD` | - | Fallback password |
| `PG_TLS_CERT`/`PG_TLS_KEY` | - | TLS cert/key paths |

## Conventions

### Imports (3 groups, blank line separated)
```rust
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::Mutex;

use crate::storage::TikvStore;
```

### Error Handling
- `anyhow::Result` everywhere
- Context with `.ok_or_else(|| anyhow!("..."))?`
- Never panic in production paths

### Transaction Pattern
```rust
let is_autocommit = !session.is_in_transaction();
if is_autocommit { session.begin().await?; }
// ... work ...
if is_autocommit {
    if result.is_ok() { session.commit().await?; }
    else { session.rollback().await?; }
}
```

### Testing
- Unit tests: `#[cfg(test)] mod tests` in same file
- Integration: `tests/*.sql` files
- Name: `test_<feature>_<scenario>`

## Anti-Patterns

- **Never** use `unwrap()` on user input
- **Never** suppress type errors (`as any` equivalent)
- **Never** commit without explicit request
- **Avoid** adding to executor.rs (already 2700+ lines)

## Key Encoding

| Entity | Key Pattern |
|--------|-------------|
| Schema | `_sys_schema_{table}` |
| Row | `t_{table_id}_{pk}` |
| Index | `i_{table_id}_{idx_id}_{val}` |
| User | `_sys_user_{name}` |

## Tech Debt

- `executor.rs` 2762 lines → split into ddl.rs, dml.rs, query.rs
- Duplicate: `eval_expr` vs `eval_expr_join` → unify
- No query planner → optimizer picks index directly

## Multi-Tenant

Username format: `tenant.user` or `tenant:user`
- `tenant_a.admin` → keyspace=tenant_a, user=admin
- `admin` → keyspace=default, user=admin

Default admin password: `admin` (bootstrapped per keyspace)
