# pg-tikv Development Progress

## Current Status: Phase 2 Complete ✅

### Completed Features

#### Phase 1 - SQL Extensions ✅

| Feature | Status | Test File |
|---------|--------|-----------|
| FETCH FIRST N ROWS | ✅ | `tests/25_phase1_features.sql` |
| NATURAL JOIN | ✅ | `tests/25_phase1_features.sql` |
| CROSS JOIN | ✅ | `tests/25_phase1_features.sql` |
| UPDATE ... FROM | ✅ | `tests/25_phase1_features.sql` |
| CREATE TABLE AS | ✅ | `tests/25_phase1_features.sql` |
| SELECT INTO | ✅ | `tests/25_phase1_features.sql` |

#### Phase 2 - Query Optimization ✅

| Feature | Status | Notes |
|---------|--------|-------|
| Query Planner | ✅ | `src/sql/planner.rs` - Cost-based optimization |
| Index Selection | ✅ | Chooses best index based on cost estimation |
| JOIN Order Optimization | ✅ | Sorts by estimated row count |
| Predicate Pushdown | ✅ | Assigns predicates to source tables |
| EXPLAIN Statement | ✅ | `tests/26_explain.sql` - PostgreSQL-compatible output |

#### Phase 3 - Advanced SQL (Partial) ✅

| Feature | Status | Test File |
|---------|--------|-----------|
| RIGHT OUTER JOIN | ✅ | `tests/22_new_features.sql` |
| FULL OUTER JOIN | ✅ | `tests/22_new_features.sql` |
| UNION / INTERSECT / EXCEPT | ✅ | `tests/22_new_features.sql` |

#### Code Refactoring ✅

| Change | Status | Notes |
|--------|--------|-------|
| Split executor.rs | ✅ | ddl.rs, dml.rs, rbac.rs, window.rs, helpers.rs, query.rs |
| Add explain.rs | ✅ | Query plan generation and formatting |
| Add planner.rs | ✅ | Cost-based index selection, JOIN optimization |

#### Bug Fixes

| Issue | Fix | File |
|-------|-----|------|
| Transaction panic on drop | Set `CheckLevel::Warn` | `src/storage/tikv_store.rs` |

---

## TODO: Remaining Features

### Phase 3 - Advanced SQL (Remaining)

| Feature | Priority | Notes |
|---------|----------|-------|
| Recursive CTEs | Low | `WITH RECURSIVE` not supported |
| Lateral Joins | Low | `LATERAL` subqueries |

### Phase 4 - Constraints & Integrity

| Feature | Priority | Notes |
|---------|----------|-------|
| Foreign Key Enforcement | Medium | Parsed but not enforced |
| CHECK Constraints | Medium | Parsed but not enforced |
| NOT NULL Enforcement | High | Parsed but not fully enforced |

### Phase 5 - Performance

| Feature | Priority | Notes |
|---------|----------|-------|
| Parallel Query Execution | Low | Single-threaded execution |
| Batch Insert Optimization | Medium | Currently row-by-row |
| Connection Pooling | Medium | One TiKV client per connection |

### Phase 6 - Compatibility

| Feature | Priority | Notes |
|---------|----------|-------|
| EXPLAIN ANALYZE | Medium | EXPLAIN done, ANALYZE not yet |
| pg_catalog Tables | Low | System catalog emulation |
| Information Schema | Low | Standard metadata views |

---

## Known Limitations

1. **Tables require PRIMARY KEY** - SELECT INTO creates tables without PK
2. **No recursive CTEs** - `WITH RECURSIVE` not parsed
3. **Foreign keys not enforced** - Parsed and stored but not checked
4. **EXPLAIN costs differ from PostgreSQL** - Different planner, no statistics

---

## Test Coverage

```
Unit tests: 181 tests
Integration tests: 13 tests

tests/
├── 01-21: Core functionality tests
├── 22_new_features.sql: UNION, RIGHT/FULL JOIN, JSON, ARRAY
├── 25_phase1_features.sql: Phase 1 SQL extensions
├── 26_explain.sql: EXPLAIN statement tests (27 cases)
└── dvdrental/: Sample database for pg_restore compatibility
```

Run tests:
```bash
# Unit tests
cargo test

# Integration tests (requires TiKV)
python3 scripts/integration_test.py
```
