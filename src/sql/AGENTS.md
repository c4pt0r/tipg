# SQL Module

Core SQL parsing and execution. 6200+ lines across 7 files.

## Files

| File | Lines | Purpose |
|------|-------|---------|
| `executor.rs` | 2762 | Statement execution (DDL, DML, query) |
| `expr.rs` | 2981 | Expression evaluation, functions, operators |
| `aggregate.rs` | 242 | COUNT, SUM, AVG, MIN, MAX |
| `session.rs` | 108 | Transaction state, current user |
| `result.rs` | 94 | ExecuteResult enum |
| `parser.rs` | 40 | sqlparser wrapper |
| `mod.rs` | 15 | Module exports |

## Where to Look

| Task | Location |
|------|----------|
| Add function (UPPER, NOW, etc.) | `expr.rs` → `eval_function()` |
| Add operator (+, -, LIKE, etc.) | `expr.rs` → `eval_binary_op()` |
| Add statement (CREATE, DROP) | `executor.rs` → `execute_statement_on_txn()` |
| Add aggregate | `aggregate.rs` → `Aggregator` enum |
| Change transaction behavior | `session.rs` |

## expr.rs Hotspots

```
eval_function()      # ~line 400  - String/math/date functions
eval_binary_op()     # ~line 200  - Operators (+, -, *, /, LIKE)
eval_json_op()       # ~line 600  - JSON ->, ->> operators
eval_cast()          # ~line 800  - Type casts
```

## executor.rs Hotspots

```
execute_select()     # ~line 500  - SELECT with JOIN, WHERE, GROUP BY
execute_insert()     # ~line 800  - INSERT with RETURNING, ON CONFLICT
execute_update()     # ~line 1000 - UPDATE with RETURNING
execute_delete()     # ~line 1100 - DELETE with RETURNING
execute_create_table() # ~line 200
execute_grant()      # ~line 2400 - RBAC
```

## Adding a SQL Function

1. Add match arm in `eval_function()`:
```rust
"my_func" => {
    let arg = eval_expr(args[0], row, schema)?;
    // transform arg
    Ok(Value::Text(result))
}
```

2. Add test in same file:
```rust
#[test]
fn test_my_func() {
    let result = eval_function_standalone("my_func", vec![Value::Text("x".into())]);
    assert_eq!(result, Ok(Value::Text("expected".into())));
}
```

3. Add integration test in `tests/` if needed.

## Known Issues

- `executor.rs` too large → needs splitting
- `eval_expr` vs `eval_expr_join` duplication
- Window functions impl is hacky (sorts entire result set)
