# Progress Summary

## Overview
We have successfully built `pg-tikv`, a functional PostgreSQL-compatible SQL layer running on top of TiKV. The system implements a complete vertical slice of a relational database:
1.  **Protocol Layer**: Handles PostgreSQL wire protocol (using `pgwire` 0.28).
2.  **SQL Layer**: Parses SQL (using `sqlparser`) and executes plans.
3.  **Storage Layer**: Maps relational data to TiKV's Key-Value store with transaction support.

## Achievements
- [x] **Project Skeleton**: Setup Rust project with `tikv-client`, `pgwire`, and `sqlparser`.
- [x] **KV Mapping**: Designed and implemented efficient encoding for Tables, Rows, Indexes, and Metadata.
- [x] **Transaction Support**: 
    - **Session-Level Transactions**: Support `BEGIN`, `COMMIT`, `ROLLBACK`.
    - **Atomic DML**: All DML operations are atomic.
    - **Safety**: Robust transaction lifecycle management.
    - **Pessimistic Transactions**: Uses TiKV pessimistic transactions for better isolation and early conflict detection.
    - **SELECT FOR UPDATE**: Explicit row locking for concurrent access control.
- [x] **DDL Support**:
    - `CREATE TABLE [IF NOT EXISTS]` with `SERIAL`, `DEFAULT`, `UNIQUE`, `PRIMARY KEY (comp, osite)`.
    - `CREATE INDEX`.
    - `DROP TABLE [IF EXISTS]`.
    - `TRUNCATE TABLE`.
    - `ALTER TABLE ADD COLUMN` with Online Schema Change.
    - `ALTER TABLE ADD CONSTRAINT PRIMARY KEY` for composite PKs.
    - `SHOW TABLES`.
- [x] **DML Support**:
    - `INSERT` with `RETURNING` clause support. Maintains indexes.
    - `SELECT` with `WHERE` filtering and column projection.
    - `SELECT DISTINCT` for deduplication.
    - `UPDATE` with `WHERE` filtering and expressions. Maintains indexes.
    - `DELETE` with `WHERE` filtering. Maintains indexes.
    - **Advanced Query**: `ORDER BY`, `LIMIT`, `OFFSET`.
    - **Aggregation**: `COUNT`, `SUM`, `MAX`, `MIN`, `AVG`.
    - **JOIN**: `INNER JOIN`, `LEFT JOIN` with `ON` clause.
- [x] **Optimization**:
    - **Index Scan**: Optimizer automatically selects Index Scan for Point Lookups (`col1=val1 AND col2=val2`).
- [x] **Advanced Features**:
    - **Secondary Indexes**: Create and maintain unique/non-unique secondary indexes.
    - **Composite Primary Keys**: Support tables with multi-column primary keys.
    - **Expression Evaluator**: Supports `+`, `-`, `*`, `/`, `%`, `||` (concat), `AND`, `OR`, `=`, `>`, `<`, `>=`, `<=`, `<>`.
    - **NULL Handling**: `IS NULL`, `IS NOT NULL`, `COALESCE()`, `NULLIF()`.
    - **Range Expressions**: `IN (...)`, `BETWEEN ... AND ...`.
    - **Multi-tenancy**: Support `PG_NAMESPACE` env var.
- [x] **PostgreSQL Functions**:
    - **String Functions**: `UPPER`, `LOWER`, `LENGTH`, `CONCAT`, `LEFT`, `RIGHT`, `LPAD`, `RPAD`, `REPLACE`, `REVERSE`, `REPEAT`, `SPLIT_PART`, `INITCAP`, `SUBSTRING`, `TRIM`, `POSITION`.
    - **Math Functions**: `ABS`, `CEIL`, `FLOOR`, `ROUND`, `TRUNC`, `SQRT`, `POWER`, `EXP`, `LN`, `LOG`, `SIGN`, `MOD`, `PI`, `RANDOM`.
    - **Date Functions**: `NOW`, `CURRENT_TIMESTAMP`, `CURRENT_DATE`, `DATE_TRUNC`, `EXTRACT`, `TO_CHAR`, `AGE`.
    - **Comparison**: `GREATEST`, `LEAST`.
    - **Conditional**: `CASE WHEN ... THEN ... ELSE ... END`.
    - **Type Conversion**: `CAST(expr AS type)`, PostgreSQL `::` syntax.
    - **Pattern Matching**: `LIKE`, `ILIKE`, `NOT LIKE`.
- [x] **GROUP BY Enhancements**:
    - **HAVING clause**: Filter aggregated groups with conditions.
- [x] **DateTime and Interval**:
    - **INTERVAL type**: `INTERVAL '1 day'`, `INTERVAL '2' HOUR`.
    - **DateTime Arithmetic**: `TIMESTAMP + INTERVAL`, `TIMESTAMP - INTERVAL`.
    - **TypedString**: `'1 day'::interval`, `'2024-01-01'::timestamp`.
- [x] **COPY Protocol** (pgwire 0.28):
    - `COPY table (cols) FROM stdin` with tab-separated data.
    - Automatic type parsing for all column types.
    - Full `pg_restore` compatibility for database dumps.
- [x] **pg_catalog Compatibility**:
    - `pg_is_in_recovery()`, `pg_backend_pid()`, `version()`.
    - `current_database()`, `current_schema()`, `current_user`.
    - `has_schema_privilege()`, `has_table_privilege()`.
    - `obj_description()`, `col_description()`.
- [x] **UUID Support**:
    - Native UUID data type storage and display.
    - `gen_random_uuid()` function.
    - Cast from TEXT: `'uuid-string'::uuid`.

## Current State
- The server listens on `0.0.0.0:5433` by default.
- It connects to a local TiKV cluster (`127.0.0.1:2379`).
- Ready for TPC-C benchmark testing (functional verification).

## TPC-C Ready Features
| Feature | Status | Notes |
|---------|--------|-------|
| `AVG()` aggregation | ✅ | Stock level query |
| `IS NULL / IS NOT NULL` | ✅ | Order carrier check |
| `IN (val1, val2, ...)` | ✅ | Item filtering |
| `BETWEEN low AND high` | ✅ | Range queries |
| `COALESCE(a, b, ...)` | ✅ | NULL handling |
| Modulo operator `%` | ✅ | District/warehouse calc |
| `INNER JOIN` | ✅ | Multi-table queries |
| `LEFT JOIN` | ✅ | Optional relationships |
| `SELECT DISTINCT` | ✅ | Unique results |
| `GROUP BY` with aggregation | ✅ | Summary queries |
| `HAVING` clause | ✅ | Filter aggregated groups |
| String concatenation `\|\|` | ✅ | String operations |
| `LIKE` / `ILIKE` | ✅ | Pattern matching |
| `CASE WHEN` | ✅ | Conditional logic |
| String functions | ✅ | UPPER, LOWER, LENGTH, etc. |
| Math functions | ✅ | ABS, ROUND, FLOOR, etc. |

## Technical Details

### Storage Schema
| Entity | Key Pattern | Value |
|--------|-------------|-------|
| **Next Table ID** | `n_{ns}_` + `_sys_next_table_id` | `u64` (Big Endian) |
| **Sequences** | `n_{ns}_` + `_sys_seq_{table_id}` | `u64` (Big Endian) |
| **Table Schema** | `n_{ns}_` + `_sys_schema_{table_name}` | `TableSchema` (Bincode) |
| **Row Data** | `n_{ns}_` + `t_{table_id}_{pk_value}` | `Row` (Bincode) |
| **Index Data** | `n_{ns}_` + `i_{table_id}_{index_id}_{idx_val}` | `PK` (Bincode) |

## Unit Test Coverage

| Module | Tests | Description |
|--------|-------|-------------|
| `sql/parser.rs` | 3 | SQL parsing |
| `sql/expr.rs` | 35 | Expression evaluation (arithmetic, comparison, logical, IS NULL, IN, BETWEEN, LIKE, CASE, CAST, string/math functions, interval/datetime, UUID) |
| `sql/aggregate.rs` | 14 | Aggregation functions (COUNT, SUM, MAX, MIN, AVG) |
| `storage/encoding.rs` | 15 | Key encoding, serialization, namespace handling |
| **Total** | **67** | All passing ✅ |

## Architecture Notes

**Key Issues Identified:**
1. Monolithic executor.rs (700+ lines) - needs splitting
2. Duplicate expression evaluation functions
3. No query planner layer

**Improvement Priorities:**
- Phase 1: Tests ✅ Complete
- Phase 2: Refactor executor into modules
- Phase 3: Add query planner
- Phase 4: Advanced features (subqueries, CTEs)

## Future Work
- [ ] **Refactor Executor**: Split into ddl.rs, dml.rs, query.rs, join.rs
- [ ] **Unify Eval Functions**: Merge eval_expr and eval_expr_join
- [ ] **Range Index Scan**: Support range queries on indexes (requires memcomparable encoding)
- [ ] **Subqueries**: `WHERE col IN (SELECT ...)`
- [ ] **RIGHT/FULL OUTER JOIN**: Extended join types
- [ ] **Coprocessor Pushdown**: Implement TiKV Coprocessor (DAG Request)

## Test Files
| Test | Description | Status |
|------|-------------|--------|
| `01_ddl_basic.sql` | CREATE/DROP TABLE, SHOW TABLES | ✅ |
| `02_dml_crud.sql` | INSERT/SELECT/UPDATE/DELETE | ✅ |
| `03_schema_evolution.sql` | ALTER TABLE ADD COLUMN | ✅ |
| `04_returning.sql` | INSERT ... RETURNING | ✅ |
| `05_transaction.sql` | BEGIN/COMMIT/ROLLBACK | ✅ |
| `06_composite_pk.sql` | Composite Primary Keys | ✅ |
| `07_order_limit.sql` | ORDER BY, LIMIT, OFFSET | ✅ |
| `08_aggregation.sql` | COUNT, SUM, MAX, MIN | ✅ |
| `09_index.sql` | CREATE INDEX, UNIQUE INDEX | ✅ |
| `10_index_scan.sql` | Index Scan Optimization | ✅ |
| `11_group_by.sql` | GROUP BY with aggregations | ✅ |
| `12_tpcc_basic.sql` | TPC-C like queries | ✅ |
| `13_pg_functions.sql` | PostgreSQL functions (string, math, LIKE, CASE, HAVING) | ✅ |
| `14_pessimistic_txn.sql` | Pessimistic transactions, SELECT FOR UPDATE | ✅ |
| `15_uuid.sql` | UUID type and gen_random_uuid() | ✅ |
| `16_dvdrental_compat.sql` | pg_restore compatibility (COPY, pg_catalog) | ✅ |

## How to Run
```bash
# 1. Start TiKV Playground
tiup playground --mode tikv-slim

# 2. Start Server
cargo run

# 3. Connect Client
psql -h 127.0.0.1 -p 5433 -d postgres

# 4. Run Tests
./run_tests.sh

# 5. Restore a PostgreSQL dump
pg_restore -h 127.0.0.1 -p 5433 -d postgres --no-owner --no-privileges /path/to/dump/
```
