#!/usr/bin/env python3
"""
pg-tikv Integration Test Runner

Usage:
    python3 scripts/integration_test.py                    # Run built-in tests
    python3 scripts/integration_test.py tests/basic.sql   # Run single SQL file
    python3 scripts/integration_test.py tests/            # Run all .sql files in directory
    python3 scripts/integration_test.py --help            # Show help

Options:
    --user USER         PostgreSQL user (default: postgres)
    --password PASS     PostgreSQL password (default: postgres)
    --port PORT         pg-tikv port (default: 5433)
    --host HOST         pg-tikv host (default: 127.0.0.1)
    --docker            Use Docker for TiKV/pg-tikv (default)
    --tiup              Use tiup playground instead of Docker
    --no-setup          Skip TiKV/pg-tikv setup (use existing)
    --no-cleanup        Don't cleanup after tests
    --verbose           Show SQL output
    --stop-on-error     Stop on first error
"""

import subprocess
import sys
import os
import time
import signal
import re
import socket
import atexit
import argparse
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Tuple
from enum import Enum

PROJECT_DIR = Path(__file__).parent.parent
DOCKER_DIR = PROJECT_DIR / "docker"
LOG_DIR = Path("/tmp/pg-tikv-test")
DEFAULT_PORT = 5433

GREEN = "\033[0;32m"
YELLOW = "\033[1;33m"
RED = "\033[0;31m"
BLUE = "\033[0;34m"
NC = "\033[0m"


class TestResult(Enum):
    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


class SetupMode(Enum):
    DOCKER = "docker"
    TIUP = "tiup"
    NONE = "none"


@dataclass
class ProcessManager:
    playground_proc: Optional[subprocess.Popen] = None
    pgtikv_proc: Optional[subprocess.Popen] = None
    pd_port: Optional[int] = None
    docker_compose_file: Optional[Path] = None


@dataclass
class TestConfig:
    user: str = "postgres"
    password: str = "postgres"
    port: int = DEFAULT_PORT
    host: str = "127.0.0.1"
    setup_mode: SetupMode = SetupMode.DOCKER
    no_cleanup: bool = False
    verbose: bool = False
    stop_on_error: bool = False
    test_files: List[Path] = field(default_factory=list)


@dataclass
class TestStats:
    passed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: int = 0

    @property
    def total(self) -> int:
        return self.passed + self.failed + self.skipped + self.errors

    def add(self, result: TestResult):
        if result == TestResult.PASSED:
            self.passed += 1
        elif result == TestResult.FAILED:
            self.failed += 1
        elif result == TestResult.SKIPPED:
            self.skipped += 1
        else:
            self.errors += 1


pm = ProcessManager()
config = TestConfig()


def log_info(msg: str):
    print(f"{GREEN}[INFO]{NC} {msg}")


def log_warn(msg: str):
    print(f"{YELLOW}[WARN]{NC} {msg}")


def log_error(msg: str):
    print(f"{RED}[ERROR]{NC} {msg}")


def log_test(name: str, result: TestResult, details: str = ""):
    color = {
        TestResult.PASSED: GREEN,
        TestResult.FAILED: RED,
        TestResult.SKIPPED: YELLOW,
        TestResult.ERROR: RED,
    }[result]
    suffix = f" - {details}" if details else ""
    print(f"{color}[{result.value}]{NC} {name}{suffix}")


def cleanup():
    if config.no_cleanup:
        log_info("Skipping cleanup (--no-cleanup)")
        return

    print(f"{YELLOW}Cleaning up...{NC}")

    if config.setup_mode == SetupMode.DOCKER:
        cleanup_docker()
    elif config.setup_mode == SetupMode.TIUP:
        cleanup_tiup()

    print("Cleanup complete")


def cleanup_docker():
    log_info("Stopping Docker containers...")
    compose_file = DOCKER_DIR / "docker-compose.test.yml"
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down", "-v"],
        capture_output=True,
        cwd=DOCKER_DIR,
    )


def cleanup_tiup():
    if pm.pgtikv_proc and pm.pgtikv_proc.poll() is None:
        print(f"Stopping pg-tikv (PID: {pm.pgtikv_proc.pid})")
        pm.pgtikv_proc.terminate()
        try:
            pm.pgtikv_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pm.pgtikv_proc.kill()

    if pm.playground_proc and pm.playground_proc.poll() is None:
        print(f"Stopping tiup playground (PID: {pm.playground_proc.pid})")
        pm.playground_proc.terminate()
        try:
            pm.playground_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pm.playground_proc.kill()

    for pattern in ["tiup playground", "tikv-server", "pd-server"]:
        subprocess.run(["pkill", "-f", pattern], capture_output=True)


def wait_for_port(host: str, port: int, timeout: int = 30) -> bool:
    for _ in range(timeout):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1)
            if sock.connect_ex((host, port)) == 0:
                return True
        time.sleep(1)
    return False


def check_docker() -> bool:
    result = subprocess.run(["docker", "info"], capture_output=True)
    if result.returncode != 0:
        log_error("Docker is not running. Please start Docker first.")
        return False
    
    result = subprocess.run(["docker", "compose", "version"], capture_output=True)
    if result.returncode != 0:
        log_error("Docker Compose is not available. Please install Docker Compose.")
        return False
    
    return True


def setup_docker():
    log_info("Setting up TiKV cluster with Docker...")

    compose_file = DOCKER_DIR / "docker-compose.test.yml"
    if not compose_file.exists():
        log_error(f"Docker compose file not found: {compose_file}")
        sys.exit(1)

    log_info("Starting Docker containers (PD, TiKV, pg-tikv)...")
    log_info("This may take a few minutes on first run...")

    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "up", "-d", "--build", 
         "pd", "tikv", "pg-tikv"],
        cwd=DOCKER_DIR,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        log_error("Failed to start Docker containers:")
        print(result.stderr)
        sys.exit(1)

    log_info("Waiting for pg-tikv to be ready...")
    
    if not wait_for_port(config.host, config.port, timeout=120):
        log_error(f"pg-tikv not accessible on {config.host}:{config.port}")
        log_error("Container logs:")
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "logs", "pg-tikv"],
            cwd=DOCKER_DIR,
        )
        sys.exit(1)

    log_info(f"pg-tikv is ready on port {config.port}")
    time.sleep(2)


def extract_pd_port(log_file: Path, timeout: int = 120) -> Optional[int]:
    pattern = re.compile(r"(?:PD Endpoints:|PD client).*?127\.0\.0\.1:(\d+)")

    for elapsed in range(timeout):
        if log_file.exists():
            content = log_file.read_text()
            match = pattern.search(content)
            if match:
                return int(match.group(1))

        if elapsed > 0 and elapsed % 10 == 0:
            log_info(f"Still waiting for PD to start... ({elapsed}s/{timeout}s)")
        time.sleep(1)

    return None


def detect_tiup_ctl_version() -> str:
    tiup_home = Path.home() / ".tiup" / "components" / "ctl"
    if tiup_home.exists():
        versions = [d.name for d in tiup_home.iterdir() if d.is_dir() and d.name.startswith("v")]
        if versions:
            return sorted(versions, reverse=True)[0]
    return "nightly"


def check_tiup_dependencies() -> bool:
    deps = {
        "tiup": "curl --proto '=https' --tlsv1.2 -sSf https://tiup-mirrors.pingcap.com/install.sh | sh",
        "psql": "brew install postgresql   (macOS)\n  apt install postgresql-client   (Ubuntu/Debian)",
        "cargo": "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh",
    }

    missing = False
    for cmd, install_hint in deps.items():
        if subprocess.run(["which", cmd], capture_output=True).returncode != 0:
            log_error(f"{cmd} is not installed. Install with:")
            log_error(f"  {install_hint}")
            missing = True

    return not missing


def setup_tiup():
    log_info("Setting up TiKV cluster with tiup...")

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    tikv_config = LOG_DIR / "tikv.toml"
    tikv_config.write_text("""\
[storage]
api-version = 2
enable-ttl = true
""")

    log_info("Starting tiup playground (this may take a while on first run)...")

    log_file = LOG_DIR / "playground.log"
    with open(log_file, "w") as f:
        pm.playground_proc = subprocess.Popen(
            ["tiup", "playground", "--mode", "tikv-slim", "--kv.config", str(tikv_config)],
            stdout=f,
            stderr=subprocess.STDOUT,
        )

    log_info(f"Waiting for PD to start (PID: {pm.playground_proc.pid})...")

    pm.pd_port = extract_pd_port(log_file)
    if not pm.pd_port:
        log_error("Failed to extract PD port from logs after 120 seconds")
        log_error("Log file contents:")
        print(log_file.read_text())
        sys.exit(1)

    log_info(f"PD is running on port {pm.pd_port}")

    if not wait_for_port("127.0.0.1", pm.pd_port, 60):
        log_error(f"PD port {pm.pd_port} is not accessible")
        sys.exit(1)

    time.sleep(3)
    build_pgtikv()
    start_pgtikv_tiup()


def build_pgtikv():
    log_info("Building pg-tikv...")
    result = subprocess.run(
        ["cargo", "build", "--release"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        log_error("Build failed:")
        print(result.stderr)
        sys.exit(1)


def start_pgtikv_tiup():
    log_info(f"Starting pg-tikv on port {config.port} with PD at 127.0.0.1:{pm.pd_port}...")

    log_file = LOG_DIR / "pgtikv.log"
    env = os.environ.copy()
    env.update({
        "PD_ENDPOINTS": f"127.0.0.1:{pm.pd_port}",
        "PG_PORT": str(config.port),
        "PG_PASSWORD": config.password,
    })

    with open(log_file, "w") as f:
        pm.pgtikv_proc = subprocess.Popen(
            [str(PROJECT_DIR / "target" / "release" / "pg-tikv")],
            stdout=f,
            stderr=subprocess.STDOUT,
            env=env,
        )

    log_info(f"pg-tikv started (PID: {pm.pgtikv_proc.pid})")

    if not wait_for_port("127.0.0.1", config.port, 30):
        log_error(f"pg-tikv port {config.port} is not accessible")
        print(log_file.read_text())
        sys.exit(1)

    time.sleep(2)


def check_cluster_health() -> bool:
    if config.setup_mode == SetupMode.TIUP:
        if pm.playground_proc and pm.playground_proc.poll() is not None:
            log_error(f"tiup playground died (exit code: {pm.playground_proc.returncode})")
            return False
        if pm.pgtikv_proc and pm.pgtikv_proc.poll() is not None:
            log_error(f"pg-tikv died (exit code: {pm.pgtikv_proc.returncode})")
            log_error("pg-tikv log:")
            print((LOG_DIR / "pgtikv.log").read_text()[-2000:])
            return False
    return True


def run_sql(sql: str, user: Optional[str] = None, password: Optional[str] = None, retries: int = 2) -> Tuple[str, int]:
    actual_user = user if user else config.user
    actual_password = password if password else config.password

    env = os.environ.copy()
    env["PGPASSWORD"] = actual_password
    output = ""

    for attempt in range(retries + 1):
        if config.setup_mode != SetupMode.NONE and not check_cluster_health():
            return "ERROR: Cluster unhealthy", 1

        result = subprocess.run(
            ["psql", "-h", config.host, "-p", str(config.port), "-U", actual_user, "-d", "postgres", "-c", sql],
            capture_output=True,
            text=True,
            env=env,
        )
        output = result.stdout + result.stderr

        if "Failed to connect to TiKV" not in output and "connection refused" not in output.lower():
            return output, result.returncode

        if attempt < retries:
            log_warn(f"Connection failed, retrying ({attempt + 1}/{retries})...")
            time.sleep(2)

    return output, 1


def run_sql_file(sql_file: Path, user: Optional[str] = None, password: Optional[str] = None) -> Tuple[str, int]:
    actual_user = user if user else config.user
    actual_password = password if password else config.password

    env = os.environ.copy()
    env["PGPASSWORD"] = actual_password

    result = subprocess.run(
        ["psql", "-h", config.host, "-p", str(config.port), "-U", actual_user, "-d", "postgres", "-f", str(sql_file)],
        capture_output=True,
        text=True,
        env=env,
    )
    return result.stdout + result.stderr, result.returncode


def run_sql_test_file(sql_file: Path, stats: TestStats) -> TestResult:
    expected_file = sql_file.with_suffix(".expected")
    errors_file = sql_file.with_suffix(".errors")
    out_file = sql_file.with_suffix(".out")
    setup_file = sql_file.with_name(sql_file.stem + "_setup.sql")
    load_script = sql_file.with_name(sql_file.stem + "_load.py")

    log_info(f"Running: {sql_file.name}")

    if setup_file.exists():
        log_info(f"  Running setup: {setup_file.name}")
        setup_output, setup_rc = run_sql_file(setup_file)
        if "ERROR:" in setup_output or "FATAL:" in setup_output:
            log_test(sql_file.name, TestResult.FAILED, f"setup failed")
            print(f"  {RED}Setup error: {setup_output[:200]}{NC}")
            return TestResult.FAILED

    if load_script.exists():
        log_info(f"  Running load script: {load_script.name}")
        result = subprocess.run(
            ["python3", str(load_script), "--port", str(config.port), "--user", config.user, "--password", config.password],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            log_test(sql_file.name, TestResult.FAILED, f"load script failed")
            print(f"  {RED}Load error: {result.stdout[:200]}{result.stderr[:200]}{NC}")
            return TestResult.FAILED

    output, returncode = run_sql_file(sql_file)

    out_file.write_text(output)

    if config.verbose:
        print(output)

    error_patterns = ["ERROR:", "FATAL:", "error:", "fatal:"]
    has_error = any(pattern in output for pattern in error_patterns)

    if expected_file.exists():
        expected = expected_file.read_text()
        if output.strip() == expected.strip():
            log_test(sql_file.name, TestResult.PASSED)
            return TestResult.PASSED
        else:
            log_test(sql_file.name, TestResult.FAILED, "output differs from expected")
            if config.verbose:
                print(f"--- Expected ({expected_file}):")
                print(expected[:500])
                print(f"--- Actual ({out_file}):")
                print(output[:500])
            return TestResult.FAILED
    else:
        if has_error:
            if errors_file.exists():
                expected_errors = [line.strip() for line in errors_file.read_text().strip().split("\n") if line.strip()]
                actual_errors = [line for line in output.split("\n") if any(p in line for p in error_patterns)]
                unexpected_errors = []
                for actual in actual_errors:
                    if not any(exp in actual for exp in expected_errors):
                        unexpected_errors.append(actual)
                if unexpected_errors:
                    log_test(sql_file.name, TestResult.FAILED, "unexpected SQL errors")
                    for line in unexpected_errors[:3]:
                        print(f"  {RED}{line}{NC}")
                    return TestResult.FAILED
                else:
                    log_test(sql_file.name, TestResult.PASSED, "all errors were expected")
                    return TestResult.PASSED
            else:
                log_test(sql_file.name, TestResult.FAILED, "SQL errors detected")
                for line in output.split("\n"):
                    if any(p in line for p in error_patterns):
                        print(f"  {RED}{line}{NC}")
                        break
                return TestResult.FAILED
        else:
            log_test(sql_file.name, TestResult.PASSED, "no .expected file, checked for errors only")
            return TestResult.PASSED


def run_external_tests(test_paths: List[Path]) -> TestStats:
    stats = TestStats()

    sql_files = []
    for path in test_paths:
        if path.is_file() and path.suffix == ".sql":
            sql_files.append(path)
        elif path.is_dir():
            sql_files.extend(sorted(path.glob("*.sql")))

    if not sql_files:
        log_error("No .sql test files found")
        return stats

    log_info(f"Found {len(sql_files)} test file(s)")
    log_info("=========================================")

    for sql_file in sql_files:
        result = run_sql_test_file(sql_file, stats)
        stats.add(result)

        if config.stop_on_error and result in (TestResult.FAILED, TestResult.ERROR):
            log_warn("Stopping on first error (--stop-on-error)")
            break

        time.sleep(0.1)

    return stats


class TestRunner:
    def __init__(self):
        self.stats = TestStats()

    def run_test(self, name: str, test_func) -> bool:
        try:
            if test_func():
                self.stats.passed += 1
                return True
        except Exception as e:
            log_error(f"{name}: EXCEPTION - {e}")
        self.stats.failed += 1
        return False


def test_basic_connection() -> bool:
    log_info("Testing basic connection...")
    result, _ = run_sql("SELECT 1 as test")
    if "1" in result:
        log_info("Basic connection: PASSED")
        return True
    log_error("Basic connection: FAILED")
    print(result)
    return False


def test_ddl_operations() -> bool:
    log_info("Testing DDL operations...")

    run_sql("DROP TABLE IF EXISTS test_ddl")
    run_sql("""CREATE TABLE test_ddl (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        created_at TIMESTAMP DEFAULT NOW()
    )""")

    tables, _ = run_sql("SHOW TABLES")
    if "test_ddl" not in tables:
        log_error("CREATE TABLE: FAILED")
        return False
    log_info("CREATE TABLE: PASSED")

    run_sql("ALTER TABLE test_ddl ADD COLUMN age INTEGER")
    run_sql("CREATE INDEX idx_test_name ON test_ddl (name)")
    run_sql("DROP TABLE test_ddl")

    log_info("DDL operations: PASSED")
    return True


def test_dml_operations() -> bool:
    log_info("Testing DML operations...")

    run_sql("DROP TABLE IF EXISTS test_dml")
    run_sql("CREATE TABLE test_dml (id SERIAL PRIMARY KEY, value INTEGER)")
    run_sql("INSERT INTO test_dml (value) VALUES (10), (20), (30)")

    result, _ = run_sql("SELECT SUM(value) FROM test_dml")
    if "60" not in result:
        log_error(f"INSERT + SELECT: FAILED (expected 60)")
        return False
    log_info("INSERT + SELECT: PASSED")

    run_sql("UPDATE test_dml SET value = value * 2 WHERE value > 15")
    result, _ = run_sql("SELECT SUM(value) FROM test_dml")
    if "110" not in result:
        log_error(f"UPDATE: FAILED (expected 110)")
        return False
    log_info("UPDATE: PASSED")

    run_sql("DELETE FROM test_dml WHERE value > 50")
    result, _ = run_sql("SELECT COUNT(*) FROM test_dml")
    if "1" not in result:
        log_error(f"DELETE: FAILED (expected 1 row)")
        return False
    log_info("DELETE: PASSED")

    run_sql("DROP TABLE test_dml")
    log_info("DML operations: PASSED")
    return True


def test_transactions() -> bool:
    log_info("Testing transactions...")

    run_sql("DROP TABLE IF EXISTS test_txn")
    run_sql("CREATE TABLE test_txn (id INTEGER PRIMARY KEY, value INTEGER)")
    run_sql("INSERT INTO test_txn VALUES (1, 100)")

    run_sql("BEGIN; UPDATE test_txn SET value = 200 WHERE id = 1; ROLLBACK")
    result, _ = run_sql("SELECT value FROM test_txn WHERE id = 1")
    if "100" not in result:
        log_error(f"ROLLBACK: FAILED (expected 100)")
        return False
    log_info("ROLLBACK: PASSED")

    run_sql("UPDATE test_txn SET value = 300 WHERE id = 1")
    result, _ = run_sql("SELECT value FROM test_txn WHERE id = 1")
    if "300" not in result:
        log_error(f"UPDATE (auto-commit): FAILED (expected 300)")
        return False
    log_info("UPDATE (auto-commit): PASSED")

    run_sql("DROP TABLE test_txn")
    log_info("Transaction tests: PASSED")
    return True


def test_json_operations() -> bool:
    log_info("Testing JSON operations...")

    run_sql("DROP TABLE IF EXISTS test_json")
    run_sql("CREATE TABLE test_json (id SERIAL PRIMARY KEY, data JSONB)")
    run_sql("""INSERT INTO test_json (data) VALUES ('{"name": "Alice", "age": 30}')""")
    run_sql("""INSERT INTO test_json (data) VALUES ('{"name": "Bob", "age": 25}')""")

    result, _ = run_sql("SELECT data->>'name' FROM test_json WHERE id = 1")
    if "Alice" not in result:
        log_error(f"JSON extraction: FAILED - got: {result[:200]}")
        return False
    log_info("JSON extraction: PASSED")

    run_sql("DROP TABLE test_json")
    log_info("JSON operations: PASSED")
    return True


def test_query_features() -> bool:
    log_info("Testing advanced query features...")

    run_sql("DROP TABLE IF EXISTS orders_test")
    run_sql("DROP TABLE IF EXISTS customers_test")

    run_sql("""CREATE TABLE customers_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        city TEXT
    )""")

    run_sql("""CREATE TABLE orders_test (
        id SERIAL PRIMARY KEY,
        customer_id INT,
        amount DOUBLE PRECISION
    )""")

    run_sql("INSERT INTO customers_test (name, city) VALUES ('Alice', 'NYC'), ('Bob', 'LA')")
    run_sql("INSERT INTO orders_test (customer_id, amount) VALUES (1, 100), (1, 200), (2, 150)")

    result, _ = run_sql("""
        SELECT c.name, SUM(o.amount)
        FROM customers_test c
        JOIN orders_test o ON c.id = o.customer_id
        GROUP BY c.id, c.name
    """)
    if "Alice" not in result or "300" not in result:
        log_error(f"JOIN + GROUP BY: FAILED")
        return False
    log_info("JOIN + GROUP BY: PASSED")

    result, _ = run_sql("""
        SELECT name FROM customers_test
        WHERE id IN (SELECT customer_id FROM orders_test WHERE amount > 100)
    """)
    if "Alice" not in result:
        log_error(f"Subquery: FAILED")
        return False
    log_info("Subquery: PASSED")

    run_sql("DROP TABLE orders_test")
    run_sql("DROP TABLE customers_test")

    log_info("Advanced query features: PASSED")
    return True


def run_builtin_tests() -> TestStats:
    runner = TestRunner()

    log_info("=========================================")
    log_info("Running Built-in Integration Tests")
    log_info("=========================================")

    tests = [
        ("Basic Connection", test_basic_connection),
        ("DDL Operations", test_ddl_operations),
        ("DML Operations", test_dml_operations),
        ("Transactions", test_transactions),
        ("JSON Operations", test_json_operations),
        ("Query Features", test_query_features),
    ]

    for name, test_func in tests:
        success = runner.run_test(name, test_func)
        if config.stop_on_error and not success:
            log_warn("Stopping on first error (--stop-on-error)")
            break

    return runner.stats


def parse_args() -> TestConfig:
    parser = argparse.ArgumentParser(
        description="pg-tikv Integration Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           Run built-in tests with Docker
  %(prog)s --tiup                    Run built-in tests with tiup
  %(prog)s tests/basic.sql           Run single SQL file
  %(prog)s tests/                    Run all .sql files in directory
  %(prog)s --no-setup tests/*.sql    Run tests against existing pg-tikv
  %(prog)s --verbose tests/my.sql    Show SQL output
        """,
    )
    parser.add_argument("tests", nargs="*", help="SQL test files or directories")
    parser.add_argument("--user", default="postgres", help="PostgreSQL user (default: postgres)")
    parser.add_argument("--password", default="postgres", help="PostgreSQL password (default: postgres)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"pg-tikv port (default: {DEFAULT_PORT})")
    parser.add_argument("--host", default="127.0.0.1", help="pg-tikv host (default: 127.0.0.1)")
    parser.add_argument("--docker", action="store_true", default=True, help="Use Docker for TiKV (default)")
    parser.add_argument("--tiup", action="store_true", help="Use tiup playground instead of Docker")
    parser.add_argument("--no-setup", action="store_true", help="Skip TiKV/pg-tikv setup")
    parser.add_argument("--no-cleanup", action="store_true", help="Don't cleanup after tests")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show SQL output")
    parser.add_argument("--stop-on-error", "-x", action="store_true", help="Stop on first error")

    args = parser.parse_args()

    if args.no_setup:
        setup_mode = SetupMode.NONE
    elif args.tiup:
        setup_mode = SetupMode.TIUP
    else:
        setup_mode = SetupMode.DOCKER

    cfg = TestConfig(
        user=args.user,
        password=args.password,
        port=args.port,
        host=args.host,
        setup_mode=setup_mode,
        no_cleanup=args.no_cleanup,
        verbose=args.verbose,
        stop_on_error=args.stop_on_error,
        test_files=[Path(t) for t in args.tests] if args.tests else [],
    )
    return cfg


def main():
    global config
    config = parse_args()

    log_info("pg-tikv Integration Test Runner")
    log_info("================================")

    if not config.no_cleanup:
        atexit.register(cleanup)
    signal.signal(signal.SIGINT, lambda s, f: sys.exit(1))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(1))

    if config.setup_mode == SetupMode.DOCKER:
        if not check_docker():
            sys.exit(1)
        setup_docker()
    elif config.setup_mode == SetupMode.TIUP:
        if not check_tiup_dependencies():
            sys.exit(1)
        for pattern in ["pg-tikv", "tiup playground", "tikv-server", "pd-server"]:
            subprocess.run(["pkill", "-f", pattern], capture_output=True)
        time.sleep(1)
        setup_tiup()
    else:
        log_info("Skipping setup (--no-setup), using existing pg-tikv")
        if not wait_for_port(config.host, config.port, 5):
            log_error(f"pg-tikv not accessible on {config.host}:{config.port}")
            sys.exit(1)

    if config.test_files:
        stats = run_external_tests(config.test_files)
    else:
        stats = run_builtin_tests()

    log_info("=========================================")
    log_info(f"Test Results: {stats.passed} passed, {stats.failed} failed, {stats.skipped} skipped")
    log_info("=========================================")

    if stats.failed == 0 and stats.errors == 0:
        log_info("All tests passed!")
        sys.exit(0)
    else:
        log_error("Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
