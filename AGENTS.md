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

## Lessons Learned (Bug Patterns to Avoid)

### 1. Aggregates in HAVING vs SELECT
When implementing GROUP BY with HAVING, aggregate functions in HAVING clause may differ from those in SELECT. Must collect aggregates from BOTH:
```rust
// WRONG: Only collect from projection
for item in select.projection { collect_agg_funcs(...) }

// RIGHT: Collect from projection AND having
for item in select.projection { collect_agg_funcs(...) }
if let Some(having) = &select.having { collect_having_agg_funcs(...) }
```
The fallback `temp_agg` approach (creating aggregator on-the-fly) only sees ONE row, not the whole group.

### 2. NATURAL JOIN Column Handling
PostgreSQL NATURAL JOIN output format:
1. Common columns appear FIRST, shown ONCE (no table prefix)
2. Remaining columns from left table (no prefix)  
3. Remaining columns from right table (no prefix)

Must track `is_natural` flag and `common_cols` through the join processing, then use them when building SELECT * output.

### 3. pgwire Tag Format for INSERT
PostgreSQL returns `INSERT oid count` (e.g., `INSERT 0 3`), not `INSERT count`.
```rust
// WRONG: psql shows "could not interpret result from server"
Tag::new("INSERT").with_rows(n)

// RIGHT: Proper PostgreSQL format
Tag::new("INSERT").with_oid(0).with_rows(n)
```

### 4. SELECT INTO Table Structure
SELECT INTO creates tables WITHOUT constraints (no PK, no SERIAL, no DEFAULT). This is PostgreSQL-compatible - the new table only has column types. Don't "fix" this by adding auto-increment.

### 5. Window Functions: PARTITION BY with/without ORDER BY
PostgreSQL window functions behave differently based on ORDER BY presence:
- **Without ORDER BY**: Returns aggregate over ENTIRE partition (same value for all rows)
- **With ORDER BY**: Returns running/cumulative aggregate up to current row

```rust
// WRONG: Always compute running aggregate
for &row_idx in &row_indices {
    running_sum += val;
    results[row_idx] = running_sum;  // Bug: running sum even without ORDER BY
}

// RIGHT: Check if ORDER BY is present
if wf.order_by.is_empty() {
    // Compute partition total first
    let total = row_indices.iter().map(|&i| get_val(i)).sum();
    for &row_idx in &row_indices {
        results[row_idx] = total;  // Same value for all rows in partition
    }
} else {
    // Compute running aggregate
    for &row_idx in &row_indices {
        running_sum += get_val(row_idx);
        results[row_idx] = running_sum;
    }
}
```
This applies to SUM, COUNT, AVG, MIN, MAX window functions.

### 6. Test Coverage: Boundary Conditions Matter
The window function bug was missed because tests only covered `SUM OVER (PARTITION BY x ORDER BY y)` (running sum), never `SUM OVER (PARTITION BY x)` (partition total). PostgreSQL behavior changes based on ORDER BY presence - this is a **critical boundary condition**.

**Testing lessons:**
1. Always test with AND without optional clauses (ORDER BY, WHERE, HAVING, etc.)
2. Generate `.expected` files from real PostgreSQL, don't just verify "query runs"
3. When implementing SQL features, check PostgreSQL docs for semantic differences based on clause presence

```sql
-- WRONG: Only testing one variant
SELECT SUM(x) OVER (PARTITION BY y ORDER BY z) FROM t;  -- running sum

-- RIGHT: Test both variants
SELECT SUM(x) OVER (PARTITION BY y ORDER BY z) FROM t;  -- running sum
SELECT SUM(x) OVER (PARTITION BY y) FROM t;              -- partition total (different!)
```

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
