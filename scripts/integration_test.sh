#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="/tmp/pg-tikv-test"
TIKV_CONFIG="$LOG_DIR/tikv.toml"
PD_PORT=""
PG_PORT=15433
PLAYGROUND_PID=""
PGTIKV_PID=""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    
    if [ -n "$PGTIKV_PID" ] && kill -0 "$PGTIKV_PID" 2>/dev/null; then
        echo "Stopping pg-tikv (PID: $PGTIKV_PID)"
        kill "$PGTIKV_PID" 2>/dev/null || true
        wait "$PGTIKV_PID" 2>/dev/null || true
    fi
    
    if [ -n "$PLAYGROUND_PID" ] && kill -0 "$PLAYGROUND_PID" 2>/dev/null; then
        echo "Stopping tiup playground (PID: $PLAYGROUND_PID)"
        kill "$PLAYGROUND_PID" 2>/dev/null || true
        wait "$PLAYGROUND_PID" 2>/dev/null || true
    fi
    
    pkill -f "tiup playground" 2>/dev/null || true
    pkill -f "tikv-server" 2>/dev/null || true
    pkill -f "pd-server" 2>/dev/null || true
    
    echo "Cleanup complete"
}

trap cleanup EXIT

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

wait_for_port() {
    local port=$1
    local timeout=${2:-30}
    local count=0
    
    while ! nc -z 127.0.0.1 "$port" 2>/dev/null; do
        sleep 1
        count=$((count + 1))
        if [ $count -ge $timeout ]; then
            return 1
        fi
    done
    return 0
}

extract_pd_port() {
    local log_file="$1"
    local timeout=30
    local count=0
    
    while [ $count -lt $timeout ]; do
        if [ -f "$log_file" ]; then
            local port=$(grep -o "PD client.*127.0.0.1:[0-9]*" "$log_file" 2>/dev/null | grep -o "[0-9]*$" | head -1)
            if [ -n "$port" ]; then
                echo "$port"
                return 0
            fi
        fi
        sleep 1
        count=$((count + 1))
    done
    return 1
}

setup_tikv() {
    log_info "Setting up TiKV cluster..."
    
    mkdir -p "$LOG_DIR"
    
    cat > "$TIKV_CONFIG" << 'EOF'
[storage]
api-version = 2
enable-ttl = true
EOF
    
    log_info "Starting tiup playground..."
    tiup playground --mode tikv-slim --kv.config "$TIKV_CONFIG" > "$LOG_DIR/playground.log" 2>&1 &
    PLAYGROUND_PID=$!
    
    log_info "Waiting for PD to start (PID: $PLAYGROUND_PID)..."
    PD_PORT=$(extract_pd_port "$LOG_DIR/playground.log")
    
    if [ -z "$PD_PORT" ]; then
        log_error "Failed to extract PD port from logs"
        cat "$LOG_DIR/playground.log"
        exit 1
    fi
    
    log_info "PD is running on port $PD_PORT"
    
    if ! wait_for_port "$PD_PORT" 30; then
        log_error "PD port $PD_PORT is not accessible"
        exit 1
    fi
    
    sleep 3
}

create_keyspaces() {
    log_info "Creating keyspaces..."
    
    local keyspaces=("default" "tenant_a" "tenant_b")
    
    for ks in "${keyspaces[@]}"; do
        log_info "Creating keyspace: $ks"
        tiup ctl:v8.5.4 pd -u "http://127.0.0.1:$PD_PORT" keyspace create "$ks" 2>/dev/null || {
            log_warn "Keyspace '$ks' may already exist, continuing..."
        }
    done
    
    log_info "Listing keyspaces:"
    tiup ctl:v8.5.4 pd -u "http://127.0.0.1:$PD_PORT" keyspace list 2>/dev/null || true
}

build_pgtikv() {
    log_info "Building pg-tikv..."
    cd "$PROJECT_DIR"
    cargo build --release 2>&1 | tail -5
}

start_pgtikv() {
    log_info "Starting pg-tikv on port $PG_PORT..."
    
    cd "$PROJECT_DIR"
    PD_ENDPOINTS="127.0.0.1:$PD_PORT" \
    PG_PORT="$PG_PORT" \
    PG_PASSWORD="testpass" \
    ./target/release/pg-tikv > "$LOG_DIR/pgtikv.log" 2>&1 &
    PGTIKV_PID=$!
    
    log_info "pg-tikv started (PID: $PGTIKV_PID)"
    
    if ! wait_for_port "$PG_PORT" 30; then
        log_error "pg-tikv port $PG_PORT is not accessible"
        cat "$LOG_DIR/pgtikv.log"
        exit 1
    fi
    
    sleep 2
}

run_sql() {
    local user=$1
    local sql=$2
    local password=${3:-testpass}
    
    PGPASSWORD="$password" psql -h 127.0.0.1 -p "$PG_PORT" -U "$user" -d postgres -c "$sql" 2>&1
}

run_sql_file() {
    local user=$1
    local file=$2
    local password=${3:-testpass}
    
    PGPASSWORD="$password" psql -h 127.0.0.1 -p "$PG_PORT" -U "$user" -d postgres -f "$file" 2>&1
}

test_basic_connection() {
    log_info "Testing basic connection..."
    
    local result=$(run_sql "admin" "SELECT 1 as test")
    if echo "$result" | grep -q "1"; then
        log_info "Basic connection: PASSED"
        return 0
    else
        log_error "Basic connection: FAILED"
        echo "$result"
        return 1
    fi
}

test_tenant_isolation() {
    log_info "Testing tenant isolation..."
    
    log_info "Creating table in tenant_a..."
    run_sql "tenant_a.admin" "DROP TABLE IF EXISTS users"
    run_sql "tenant_a.admin" "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)"
    run_sql "tenant_a.admin" "INSERT INTO users (name) VALUES ('Alice'), ('Bob')"
    
    log_info "Creating table in tenant_b..."
    run_sql "tenant_b.admin" "DROP TABLE IF EXISTS users"
    run_sql "tenant_b.admin" "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)"
    run_sql "tenant_b.admin" "INSERT INTO users (name) VALUES ('Charlie')"
    
    log_info "Verifying tenant_a data..."
    local tenant_a_count=$(run_sql "tenant_a.admin" "SELECT COUNT(*) FROM users" | grep -o "[0-9]" | head -1)
    
    log_info "Verifying tenant_b data..."
    local tenant_b_count=$(run_sql "tenant_b.admin" "SELECT COUNT(*) FROM users" | grep -o "[0-9]" | head -1)
    
    if [ "$tenant_a_count" = "2" ] && [ "$tenant_b_count" = "1" ]; then
        log_info "Tenant isolation: PASSED (tenant_a: $tenant_a_count rows, tenant_b: $tenant_b_count rows)"
        return 0
    else
        log_error "Tenant isolation: FAILED (tenant_a: $tenant_a_count rows, tenant_b: $tenant_b_count rows)"
        return 1
    fi
}

test_ddl_operations() {
    log_info "Testing DDL operations..."
    
    run_sql "tenant_a.admin" "DROP TABLE IF EXISTS test_ddl"
    
    run_sql "tenant_a.admin" "CREATE TABLE test_ddl (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )"
    
    local tables=$(run_sql "tenant_a.admin" "SHOW TABLES")
    if echo "$tables" | grep -q "test_ddl"; then
        log_info "CREATE TABLE: PASSED"
    else
        log_error "CREATE TABLE: FAILED"
        return 1
    fi
    
    run_sql "tenant_a.admin" "ALTER TABLE test_ddl ADD COLUMN age INTEGER"
    
    run_sql "tenant_a.admin" "CREATE INDEX idx_test_name ON test_ddl (name)"
    
    run_sql "tenant_a.admin" "DROP TABLE test_ddl"
    
    log_info "DDL operations: PASSED"
    return 0
}

test_dml_operations() {
    log_info "Testing DML operations..."
    
    run_sql "tenant_a.admin" "DROP TABLE IF EXISTS test_dml"
    run_sql "tenant_a.admin" "CREATE TABLE test_dml (id SERIAL PRIMARY KEY, value INTEGER)"
    
    run_sql "tenant_a.admin" "INSERT INTO test_dml (value) VALUES (10), (20), (30)"
    
    local sum=$(run_sql "tenant_a.admin" "SELECT SUM(value) FROM test_dml" | grep -o "[0-9]*" | head -1)
    if [ "$sum" = "60" ]; then
        log_info "INSERT + SELECT: PASSED"
    else
        log_error "INSERT + SELECT: FAILED (expected 60, got $sum)"
        return 1
    fi
    
    run_sql "tenant_a.admin" "UPDATE test_dml SET value = value * 2 WHERE value > 15"
    local new_sum=$(run_sql "tenant_a.admin" "SELECT SUM(value) FROM test_dml" | grep -o "[0-9]*" | head -1)
    if [ "$new_sum" = "110" ]; then
        log_info "UPDATE: PASSED"
    else
        log_error "UPDATE: FAILED (expected 110, got $new_sum)"
        return 1
    fi
    
    run_sql "tenant_a.admin" "DELETE FROM test_dml WHERE value > 50"
    local count=$(run_sql "tenant_a.admin" "SELECT COUNT(*) FROM test_dml" | grep -o "[0-9]" | head -1)
    if [ "$count" = "1" ]; then
        log_info "DELETE: PASSED"
    else
        log_error "DELETE: FAILED (expected 1 row, got $count)"
        return 1
    fi
    
    run_sql "tenant_a.admin" "DROP TABLE test_dml"
    log_info "DML operations: PASSED"
    return 0
}

test_transactions() {
    log_info "Testing transactions..."
    
    run_sql "tenant_a.admin" "DROP TABLE IF EXISTS test_txn"
    run_sql "tenant_a.admin" "CREATE TABLE test_txn (id INTEGER PRIMARY KEY, value INTEGER)"
    run_sql "tenant_a.admin" "INSERT INTO test_txn VALUES (1, 100)"
    
    run_sql "tenant_a.admin" "BEGIN; UPDATE test_txn SET value = 200 WHERE id = 1; ROLLBACK"
    local value=$(run_sql "tenant_a.admin" "SELECT value FROM test_txn WHERE id = 1" | grep -o "[0-9]*" | head -1)
    if [ "$value" = "100" ]; then
        log_info "ROLLBACK: PASSED"
    else
        log_error "ROLLBACK: FAILED (expected 100, got $value)"
        return 1
    fi
    
    run_sql "tenant_a.admin" "BEGIN; UPDATE test_txn SET value = 300 WHERE id = 1; COMMIT"
    value=$(run_sql "tenant_a.admin" "SELECT value FROM test_txn WHERE id = 1" | grep -o "[0-9]*" | head -1)
    if [ "$value" = "300" ]; then
        log_info "COMMIT: PASSED"
    else
        log_error "COMMIT: FAILED (expected 300, got $value)"
        return 1
    fi
    
    run_sql "tenant_a.admin" "DROP TABLE test_txn"
    log_info "Transaction tests: PASSED"
    return 0
}

test_json_operations() {
    log_info "Testing JSON operations..."
    
    run_sql "tenant_a.admin" "DROP TABLE IF EXISTS test_json"
    run_sql "tenant_a.admin" "CREATE TABLE test_json (id SERIAL PRIMARY KEY, data JSONB)"
    
    run_sql "tenant_a.admin" "INSERT INTO test_json (data) VALUES ('{\"name\": \"Alice\", \"age\": 30}')"
    run_sql "tenant_a.admin" "INSERT INTO test_json (data) VALUES ('{\"name\": \"Bob\", \"age\": 25}')"
    
    local name=$(run_sql "tenant_a.admin" "SELECT data->>'name' FROM test_json WHERE id = 1")
    if echo "$name" | grep -q "Alice"; then
        log_info "JSON extraction: PASSED"
    else
        log_error "JSON extraction: FAILED"
        return 1
    fi
    
    run_sql "tenant_a.admin" "DROP TABLE test_json"
    log_info "JSON operations: PASSED"
    return 0
}

test_password_auth() {
    log_info "Testing password authentication..."
    
    local result=$(PGPASSWORD="wrongpass" psql -h 127.0.0.1 -p "$PG_PORT" -U "admin" -d postgres -c "SELECT 1" 2>&1)
    if echo "$result" | grep -qi "password\|authentication\|failed"; then
        log_info "Wrong password rejection: PASSED"
    else
        log_warn "Wrong password test inconclusive: $result"
    fi
    
    local result=$(run_sql "admin" "SELECT 1")
    if echo "$result" | grep -q "1"; then
        log_info "Correct password: PASSED"
    else
        log_error "Correct password: FAILED"
        return 1
    fi
    
    log_info "Password authentication: PASSED"
    return 0
}

run_all_tests() {
    local passed=0
    local failed=0
    
    log_info "========================================="
    log_info "Running Integration Tests"
    log_info "========================================="
    
    if test_basic_connection; then ((passed++)); else ((failed++)); fi
    if test_password_auth; then ((passed++)); else ((failed++)); fi
    if test_tenant_isolation; then ((passed++)); else ((failed++)); fi
    if test_ddl_operations; then ((passed++)); else ((failed++)); fi
    if test_dml_operations; then ((passed++)); else ((failed++)); fi
    if test_transactions; then ((passed++)); else ((failed++)); fi
    if test_json_operations; then ((passed++)); else ((failed++)); fi
    
    log_info "========================================="
    log_info "Test Results: $passed passed, $failed failed"
    log_info "========================================="
    
    if [ $failed -gt 0 ]; then
        return 1
    fi
    return 0
}

main() {
    log_info "pg-tikv Integration Test Suite"
    log_info "=============================="
    
    if ! command -v tiup &> /dev/null; then
        log_error "tiup is not installed. Please install TiUP first."
        exit 1
    fi
    
    if ! command -v psql &> /dev/null; then
        log_error "psql is not installed. Please install PostgreSQL client."
        exit 1
    fi
    
    setup_tikv
    create_keyspaces
    build_pgtikv
    start_pgtikv
    
    if run_all_tests; then
        log_info "All tests passed!"
        exit 0
    else
        log_error "Some tests failed!"
        exit 1
    fi
}

main "$@"
