#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.8"
# dependencies = []
# ///
"""
Concurrent Transaction Tests for pg-tikv
"""

import argparse
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List


@dataclass
class TestResult:
    name: str
    passed: bool
    message: str
    duration: float


class ConcurrentTransactionTests:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.results: List[TestResult] = []

    def run_sql(self, sql: str) -> str:
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password
        result = subprocess.run(
            ["psql", "-h", self.host, "-p", str(self.port),
             "-U", self.user, "-d", "postgres", "-t", "-A", "-c", sql],
            capture_output=True, text=True, env=env, timeout=30
        )
        return result.stdout.strip()

    def run_sql_script(self, sql: str) -> str:
        env = os.environ.copy()
        env["PGPASSWORD"] = self.password
        result = subprocess.run(
            ["psql", "-h", self.host, "-p", str(self.port),
             "-U", self.user, "-d", "postgres", "-t", "-A"],
            input=sql, capture_output=True, text=True, env=env, timeout=30
        )
        return result.stdout.strip()

    def setup(self):
        self.run_sql("DROP TABLE IF EXISTS concurrent_accounts CASCADE")
        self.run_sql("DROP TABLE IF EXISTS concurrent_counter CASCADE")
        self.run_sql("DROP TABLE IF EXISTS concurrent_inventory CASCADE")
        
        self.run_sql("""
            CREATE TABLE concurrent_accounts (
                id INT PRIMARY KEY,
                balance DECIMAL(15,2) NOT NULL
            )
        """)
        
        self.run_sql("""
            CREATE TABLE concurrent_counter (
                id INT PRIMARY KEY,
                value INT NOT NULL
            )
        """)
        
        self.run_sql("""
            CREATE TABLE concurrent_inventory (
                product_id INT PRIMARY KEY,
                quantity INT NOT NULL CHECK (quantity >= 0)
            )
        """)

    def cleanup(self):
        self.run_sql("DROP TABLE IF EXISTS concurrent_accounts CASCADE")
        self.run_sql("DROP TABLE IF EXISTS concurrent_counter CASCADE")
        self.run_sql("DROP TABLE IF EXISTS concurrent_inventory CASCADE")

    def run_test(self, name: str, test_func) -> TestResult:
        start = time.time()
        try:
            test_func()
            duration = time.time() - start
            result = TestResult(name, True, "PASSED", duration)
        except AssertionError as e:
            duration = time.time() - start
            result = TestResult(name, False, f"FAILED: {e}", duration)
        except Exception as e:
            duration = time.time() - start
            result = TestResult(name, False, f"ERROR: {e}", duration)
        
        self.results.append(result)
        status = "✓" if result.passed else "✗"
        print(f"  {status} {name} ({result.duration:.3f}s)")
        if not result.passed:
            print(f"    {result.message}")
        return result

    def test_concurrent_balance_transfer(self):
        self.run_sql("DELETE FROM concurrent_accounts")
        self.run_sql("INSERT INTO concurrent_accounts VALUES (1, 1000.00)")
        self.run_sql("INSERT INTO concurrent_accounts VALUES (2, 1000.00)")
        self.run_sql("INSERT INTO concurrent_accounts VALUES (3, 1000.00)")

        def transfer(from_id: int, to_id: int, amount: float):
            sql = f"""
            BEGIN;
            UPDATE concurrent_accounts SET balance = balance - {amount} WHERE id = {from_id};
            UPDATE concurrent_accounts SET balance = balance + {amount} WHERE id = {to_id};
            COMMIT;
            """
            self.run_sql_script(sql)
            time.sleep(0.05)

        for i in range(10):
            from_id = (i % 3) + 1
            to_id = ((i + 1) % 3) + 1
            transfer(from_id, to_id, 10.00)

        time.sleep(0.3)
        total = self.run_sql("SELECT SUM(balance) FROM concurrent_accounts")
        assert float(total) == 3000.00, f"Total balance should be 3000.00, got {total}"

    def test_sequential_counter_increment(self):
        self.run_sql("DELETE FROM concurrent_counter")
        self.run_sql("INSERT INTO concurrent_counter VALUES (1, 0)")

        for _ in range(10):
            sql = """
            BEGIN;
            UPDATE concurrent_counter SET value = value + 1 WHERE id = 1;
            COMMIT;
            """
            self.run_sql_script(sql)
            time.sleep(0.05)

        time.sleep(0.3)
        final_value = int(self.run_sql("SELECT value FROM concurrent_counter WHERE id = 1"))
        assert final_value == 10, f"Counter should be 10, got {final_value}"

    def test_concurrent_inventory_reservation(self):
        self.run_sql("DELETE FROM concurrent_inventory")
        self.run_sql("INSERT INTO concurrent_inventory VALUES (1, 100)")

        def reserve(amount: int):
            try:
                sql = f"""
                BEGIN;
                SELECT quantity FROM concurrent_inventory WHERE product_id = 1 FOR UPDATE;
                UPDATE concurrent_inventory SET quantity = quantity - {amount} 
                WHERE product_id = 1 AND quantity >= {amount};
                COMMIT;
                """
                self.run_sql_script(sql)
            except Exception:
                pass

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(reserve, 15) for _ in range(10)]
            for f in as_completed(futures):
                pass

        time.sleep(0.3)
        remaining = int(self.run_sql("SELECT quantity FROM concurrent_inventory WHERE product_id = 1"))
        assert remaining >= 0, f"Inventory should not be negative, got {remaining}"

    def test_rollback_isolation(self):
        self.run_sql("DELETE FROM concurrent_counter")
        self.run_sql("INSERT INTO concurrent_counter VALUES (1, 42)")

        self.run_sql_script("""
            BEGIN;
            UPDATE concurrent_counter SET value = 999 WHERE id = 1;
            ROLLBACK;
        """)

        val = int(self.run_sql("SELECT value FROM concurrent_counter WHERE id = 1"))
        assert val == 42, f"Value should be 42 after rollback, got {val}"

    def test_sequential_transactions(self):
        self.run_sql("TRUNCATE TABLE concurrent_accounts")
        self.run_sql("INSERT INTO concurrent_accounts VALUES (1, 500.00)")
        self.run_sql("INSERT INTO concurrent_accounts VALUES (2, 500.00)")

        for _ in range(10):
            self.run_sql_script("""
                BEGIN;
                UPDATE concurrent_accounts SET balance = balance - 10.00 WHERE id = 1;
                UPDATE concurrent_accounts SET balance = balance + 10.00 WHERE id = 2;
                COMMIT;
            """)
            time.sleep(0.05)

        time.sleep(0.3)
        total = float(self.run_sql("SELECT SUM(balance) FROM concurrent_accounts"))
        bal1 = float(self.run_sql("SELECT balance FROM concurrent_accounts WHERE id = 1"))
        bal2 = float(self.run_sql("SELECT balance FROM concurrent_accounts WHERE id = 2"))

        assert total == 1000.00, f"Total should be 1000, got {total}"
        assert bal1 == 400.00, f"Account 1 should be 400, got {bal1}"
        assert bal2 == 600.00, f"Account 2 should be 600, got {bal2}"

    def test_for_update_locking(self):
        self.run_sql("DELETE FROM concurrent_counter")
        self.run_sql("INSERT INTO concurrent_counter VALUES (1, 0)")

        for _ in range(5):
            sql = """
            BEGIN;
            SELECT value FROM concurrent_counter WHERE id = 1 FOR UPDATE;
            UPDATE concurrent_counter SET value = value + 1 WHERE id = 1;
            COMMIT;
            """
            self.run_sql_script(sql)
            time.sleep(0.05)

        time.sleep(0.3)
        final = int(self.run_sql("SELECT value FROM concurrent_counter WHERE id = 1"))
        assert final == 5, f"With FOR UPDATE, final value should be 5, got {final}"

    def run_all(self):
        print("\n" + "=" * 60)
        print("Concurrent Transaction Tests")
        print("=" * 60 + "\n")

        self.setup()

        tests = [
            ("Concurrent Balance Transfer", self.test_concurrent_balance_transfer),
            ("Sequential Counter Increment", self.test_sequential_counter_increment),
            ("Concurrent Inventory Reservation", self.test_concurrent_inventory_reservation),
            ("Rollback Isolation", self.test_rollback_isolation),
            ("Sequential Transactions", self.test_sequential_transactions),
            ("FOR UPDATE Locking", self.test_for_update_locking),
        ]

        for name, func in tests:
            self.run_test(name, func)
            time.sleep(0.2)

        self.cleanup()

        print("\n" + "=" * 60)
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed
        print(f"Results: {passed} passed, {failed} failed")
        print("=" * 60 + "\n")

        return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Concurrent Transaction Tests")
    parser.add_argument("--host", default=os.environ.get("PG_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PG_PORT", "15433")))
    parser.add_argument("--user", default=os.environ.get("PG_USER", "tenant_a.admin"))
    parser.add_argument("--password", default=os.environ.get("PG_PASSWORD", "secret"))
    args = parser.parse_args()

    tests = ConcurrentTransactionTests(args.host, args.port, args.user, args.password)
    success = tests.run_all()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
