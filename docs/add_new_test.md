# Adding Integration Tests

This guide explains how to add new integration tests to pg-tikv.

## Two Testing Approaches

| Approach | File | When to Use |
|----------|------|-------------|
| Python integration test | `scripts/integration_test.py` | Complex scenarios, multi-step operations, tenant isolation |
| SQL file test | `tests/*.sql` | SQL syntax validation, PostgreSQL compatibility |

## Approach 1: Python Integration Test

### Step 1: Create Test Function

Add a new function in `scripts/integration_test.py`:

```python
def test_your_feature() -> bool:
    log_info("Testing your feature...")

    # Setup
    run_sql("tenant_a.secret", "DROP TABLE IF EXISTS test_table")
    run_sql("tenant_a.secret", "CREATE TABLE test_table (id INT PRIMARY KEY, value INT)")

    # Test case 1: Valid operation
    result = run_sql("tenant_a.secret", "INSERT INTO test_table VALUES (1, 100)")
    if "INSERT" not in result:
        log_error("Insert failed")
        return False
    log_info("Insert: PASSED")

    # Test case 2: Expected failure
    result = run_sql("tenant_a.secret", "INSERT INTO test_table VALUES (1, 200)")  # duplicate PK
    if "duplicate" not in result.lower() and "error" not in result.lower():
        log_error("Duplicate key not rejected")
        return False
    log_info("Duplicate rejection: PASSED")

    # Cleanup
    run_sql("tenant_a.secret", "DROP TABLE test_table")
    log_info("Your feature: PASSED")
    return True
```

### Step 2: Register Test

Add to `tests` list in `run_all_tests()`:

```python
def run_all_tests() -> bool:
    tests = [
        ("Basic Connection", test_basic_connection),
        ("Password Auth", test_password_auth),
        # ... existing tests ...
        ("Your Feature", test_your_feature),  # Add here
    ]
```

### Key Functions

| Function | Purpose |
|----------|---------|
| `run_sql(user, sql)` | Execute SQL, returns output string |
| `log_info(msg)` | Print info message (green) |
| `log_warn(msg)` | Print warning (yellow) |
| `log_error(msg)` | Print error (red) |

### User Format for Multi-Tenant

```python
run_sql("tenant_a.secret", "SELECT 1")   # tenant_a keyspace, password=secret
run_sql("tenant_b.secret", "SELECT 1")   # tenant_b keyspace, password=secret
run_sql("secret", "SELECT 1")            # default keyspace, password=secret
```

### Test Pattern

```python
def test_xxx() -> bool:
    log_info("Testing xxx...")

    # 1. Setup (DROP IF EXISTS, CREATE)
    # 2. Test valid cases (check result contains expected string)
    # 3. Test invalid cases (check result contains error message)
    # 4. Cleanup (DROP)
    # 5. Return True if all passed

    log_info("xxx: PASSED")
    return True
```

## Approach 2: SQL File Test

### Step 1: Create SQL File

Create `tests/XX_feature_name.sql`:

```sql
DROP TABLE IF EXISTS test_table;

CREATE TABLE test_table (
    id INT PRIMARY KEY,
    value INT CHECK (value > 0)
);

INSERT INTO test_table VALUES (1, 100);
INSERT INTO test_table VALUES (2, -1);

SELECT * FROM test_table ORDER BY id;

DROP TABLE test_table;
```

### Step 2: Create Expected Output (Optional)

Create `tests/XX_feature_name.sql.expected` with PostgreSQL output:

```
DROP TABLE
CREATE TABLE
INSERT 0 1
ERROR:  new row for relation "test_table" violates check constraint "test_table_value_check"
DETAIL:  Failing row contains (2, -1).
 id | value 
----+-------
  1 |   100
(1 row)

DROP TABLE
```

### Generating Expected Output

```bash
# Run against real PostgreSQL to generate expected output
psql -h localhost -U postgres -f tests/XX_feature_name.sql > tests/XX_feature_name.sql.expected 2>&1
```

### Naming Convention

```
tests/
├── 01_ddl_basic.sql
├── 02_dml_crud.sql
├── ...
├── 26_explain.sql
├── 27_constraints.sql        # Use next number
└── 28_your_feature.sql
```

## Running Tests

```bash
# Python integration (recommended, auto-starts TiKV + pg-tikv)
python3 scripts/integration_test.py

# SQL file tests (requires running server)
# 1. Start TiKV: tiup playground --mode tikv-slim
# 2. Start pg-tikv: cargo run
# 3. Run: ./run_tests.sh
```

## Examples

### Example: Testing CHECK Constraints (Python)

```python
def test_check_constraints() -> bool:
    log_info("Testing CHECK constraints...")

    run_sql("tenant_a.secret", "DROP TABLE IF EXISTS check_test")
    run_sql("tenant_a.secret", """CREATE TABLE check_test (
        id INT PRIMARY KEY,
        age INT CHECK (age >= 0),
        status TEXT CHECK (status IN ('active', 'inactive'))
    )""")

    result = run_sql("tenant_a.secret", "INSERT INTO check_test VALUES (1, 25, 'active')")
    if "INSERT" not in result:
        log_error("Valid insert failed")
        return False
    log_info("Valid insert: PASSED")

    result = run_sql("tenant_a.secret", "INSERT INTO check_test VALUES (2, -1, 'active')")
    if "violates check constraint" not in result.lower():
        log_error("Negative age not rejected")
        return False
    log_info("CHECK age >= 0: PASSED")

    result = run_sql("tenant_a.secret", "INSERT INTO check_test VALUES (3, 30, 'deleted')")
    if "violates check constraint" not in result.lower():
        log_error("Invalid status not rejected")
        return False
    log_info("CHECK status IN (...): PASSED")

    run_sql("tenant_a.secret", "DROP TABLE check_test")
    log_info("CHECK constraints: PASSED")
    return True
```

### Example: Testing Window Functions (SQL File)

`tests/28_window_agg.sql`:
```sql
DROP TABLE IF EXISTS window_test;
CREATE TABLE window_test (dept TEXT, salary INT);
INSERT INTO window_test VALUES ('A', 100), ('A', 200), ('B', 150);

SELECT dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM window_test ORDER BY dept, salary;

DROP TABLE window_test;
```

## Checklist

- [ ] Test function returns `bool`
- [ ] Test cleans up created tables
- [ ] Test logs progress with `log_info`
- [ ] Test logs failures with `log_error`
- [ ] Test registered in `run_all_tests()`
- [ ] SQL file uses sequential numbering
- [ ] Expected file generated from real PostgreSQL (if applicable)
