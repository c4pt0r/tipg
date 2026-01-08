-- Transaction Consistency and ACID Tests
-- Tests: Atomicity, Consistency, Isolation, Durability properties

-- ============================================================
-- Setup
-- ============================================================

DROP TABLE IF EXISTS accounts CASCADE;
DROP TABLE IF EXISTS transfer_log CASCADE;
DROP TABLE IF EXISTS inventory CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS test_isolation CASCADE;

CREATE TABLE accounts (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    balance DECIMAL(15,2) NOT NULL,
    version INT DEFAULT 1,
    CHECK (balance >= 0)
);

CREATE TABLE transfer_log (
    id SERIAL PRIMARY KEY,
    from_account INT,
    to_account INT,
    amount DECIMAL(15,2),
    transferred_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE inventory (
    product_id INT PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity INT NOT NULL CHECK (quantity >= 0),
    reserved INT DEFAULT 0 CHECK (reserved >= 0)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    product_id INT,
    quantity INT NOT NULL,
    status TEXT DEFAULT 'pending'
);

-- ============================================================
-- Test 1: Atomicity - All or Nothing
-- ============================================================

INSERT INTO accounts VALUES (1, 'Alice', 1000.00, 1);
INSERT INTO accounts VALUES (2, 'Bob', 500.00, 1);

SELECT 'Before transfer' AS test, id, name, balance FROM accounts ORDER BY id;

BEGIN;
UPDATE accounts SET balance = balance - 200.00, version = version + 1 WHERE id = 1;
UPDATE accounts SET balance = balance + 200.00, version = version + 1 WHERE id = 2;
INSERT INTO transfer_log (from_account, to_account, amount) VALUES (1, 2, 200.00);
COMMIT;

SELECT 'After committed transfer' AS test, id, name, balance FROM accounts ORDER BY id;

SELECT 'Total balance should be 1500' AS test, SUM(balance) AS total FROM accounts;

BEGIN;
UPDATE accounts SET balance = balance - 9999.00 WHERE id = 1;
UPDATE accounts SET balance = balance + 9999.00 WHERE id = 2;
ROLLBACK;

SELECT 'After rollback - should be same as before' AS test, id, name, balance FROM accounts ORDER BY id;

-- ============================================================
-- Test 2: Consistency - Constraints Must Be Satisfied
-- ============================================================

SELECT 'Testing CHECK constraint on negative balance' AS test;

BEGIN;
UPDATE accounts SET balance = -100.00 WHERE id = 1;
COMMIT;

SELECT 'Balance after constraint violation attempt' AS test, id, balance FROM accounts WHERE id = 1;

INSERT INTO accounts VALUES (3, 'Charlie', 100.00, 1);

BEGIN;
UPDATE accounts SET balance = balance - 50.00 WHERE id = 3;
UPDATE accounts SET balance = balance - 60.00 WHERE id = 3;
COMMIT;

SELECT 'Charlie balance after overdraft attempts' AS test, balance FROM accounts WHERE id = 3;

-- ============================================================
-- Test 3: Multiple Operations Atomicity
-- ============================================================

TRUNCATE TABLE transfer_log;
DELETE FROM accounts WHERE id = 3;
UPDATE accounts SET balance = 1000.00, version = 1 WHERE id = 1;
UPDATE accounts SET balance = 500.00, version = 1 WHERE id = 2;

BEGIN;
UPDATE accounts SET balance = balance - 100.00, version = version + 1 WHERE id = 1;
UPDATE accounts SET balance = balance + 100.00, version = version + 1 WHERE id = 2;
INSERT INTO transfer_log (from_account, to_account, amount) VALUES (1, 2, 100.00);
UPDATE accounts SET balance = balance - 50.00, version = version + 1 WHERE id = 1;
UPDATE accounts SET balance = balance + 50.00, version = version + 1 WHERE id = 2;
INSERT INTO transfer_log (from_account, to_account, amount) VALUES (1, 2, 50.00);
COMMIT;

SELECT 'Multi-operation transfer result' AS test, id, balance, version FROM accounts ORDER BY id;
SELECT 'Transfer log entries' AS test, COUNT(*) AS log_count FROM transfer_log;
SELECT 'Total still 1500 (accounts 1+2)' AS test, SUM(balance) AS total FROM accounts;

-- ============================================================
-- Test 4: Inventory Reservation Pattern
-- ============================================================

INSERT INTO inventory VALUES (101, 'Widget', 100, 0);

SELECT 'Initial inventory' AS test, product_id, quantity, reserved FROM inventory;

BEGIN;
UPDATE inventory SET reserved = reserved + 10 WHERE product_id = 101 AND quantity - reserved >= 10;
INSERT INTO orders (product_id, quantity, status) VALUES (101, 10, 'reserved');
COMMIT;

SELECT 'After reservation' AS test, product_id, quantity, reserved, quantity - reserved AS available FROM inventory;
SELECT 'Order created' AS test, order_id, product_id, quantity, status FROM orders;

BEGIN;
UPDATE inventory SET quantity = quantity - 10, reserved = reserved - 10 WHERE product_id = 101;
UPDATE orders SET status = 'fulfilled' WHERE order_id = 1;
COMMIT;

SELECT 'After fulfillment' AS test, product_id, quantity, reserved FROM inventory;
SELECT 'Order status' AS test, order_id, status FROM orders WHERE order_id = 1;

-- ============================================================
-- Test 5: Rollback Preserves State
-- ============================================================

SELECT 'Before rollback test' AS test, product_id, quantity FROM inventory WHERE product_id = 101;

BEGIN;
UPDATE inventory SET quantity = 0 WHERE product_id = 101;
INSERT INTO orders (product_id, quantity, status) VALUES (101, 90, 'pending');
SELECT 'Inside transaction' AS test, quantity FROM inventory WHERE product_id = 101;
ROLLBACK;

SELECT 'After rollback - quantity restored' AS test, quantity FROM inventory WHERE product_id = 101;
SELECT 'Order count (should not include rolled back)' AS test, COUNT(*) FROM orders WHERE product_id = 101 AND status = 'pending';

-- ============================================================
-- Test 6: Nested DML in Transaction
-- ============================================================

DROP TABLE IF EXISTS parent_table CASCADE;
DROP TABLE IF EXISTS child_table CASCADE;

CREATE TABLE parent_table (
    id INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE child_table (
    id INT PRIMARY KEY,
    parent_id INT,
    value INT
);

BEGIN;
INSERT INTO parent_table VALUES (1, 'Parent 1');
INSERT INTO child_table VALUES (1, 1, 100);
INSERT INTO child_table VALUES (2, 1, 200);
INSERT INTO parent_table VALUES (2, 'Parent 2');
INSERT INTO child_table VALUES (3, 2, 300);
COMMIT;

SELECT 'Parents after nested insert' AS test, COUNT(*) FROM parent_table;
SELECT 'Children after nested insert' AS test, COUNT(*) FROM child_table;
SELECT 'Children with parent' AS test, p.name, SUM(c.value) AS total_value 
FROM parent_table p 
JOIN child_table c ON p.id = c.parent_id 
GROUP BY p.name 
ORDER BY p.name;

-- ============================================================
-- Test 7: Update with Subquery in Transaction
-- ============================================================

BEGIN;
UPDATE child_table SET value = value * 2 WHERE parent_id = (SELECT id FROM parent_table WHERE name = 'Parent 1');
COMMIT;

SELECT 'After subquery update' AS test, c.id, c.value 
FROM child_table c 
JOIN parent_table p ON c.parent_id = p.id 
WHERE p.name = 'Parent 1'
ORDER BY c.id;

-- ============================================================
-- Test 8: Delete with Referential Check in Transaction
-- ============================================================

BEGIN;
DELETE FROM child_table WHERE parent_id = 2;
DELETE FROM parent_table WHERE id = 2;
COMMIT;

SELECT 'Parents after cascading delete' AS test, COUNT(*) FROM parent_table;
SELECT 'Children after cascading delete' AS test, COUNT(*) FROM child_table;

-- ============================================================
-- Test 9: Mixed Read-Write Transaction
-- ============================================================

UPDATE accounts SET balance = 1000.00 WHERE id = 1;
UPDATE accounts SET balance = 1000.00 WHERE id = 2;

BEGIN;
SELECT 'Read balance' AS test, balance FROM accounts WHERE id = 1;
UPDATE accounts SET balance = balance + 500.00 WHERE id = 1;
SELECT 'Balance after update in same txn' AS test, balance FROM accounts WHERE id = 1;
UPDATE accounts SET balance = balance - 200.00 WHERE id = 1;
SELECT 'Balance after second update' AS test, balance FROM accounts WHERE id = 1;
COMMIT;

SELECT 'Final balance after commit' AS test, balance FROM accounts WHERE id = 1;

-- ============================================================
-- Test 10: Transaction with Aggregate Verification
-- ============================================================

UPDATE accounts SET balance = 500.00 WHERE id = 1;
UPDATE accounts SET balance = 500.00 WHERE id = 2;
UPDATE accounts SET balance = 500.00 WHERE id = 3;

SELECT 'Initial total' AS test, SUM(balance) AS total FROM accounts;

BEGIN;
UPDATE accounts SET balance = balance - 100.00 WHERE id = 1;
UPDATE accounts SET balance = balance + 50.00 WHERE id = 2;
UPDATE accounts SET balance = balance + 50.00 WHERE id = 3;
SELECT 'Total in transaction (should still be 1500)' AS test, SUM(balance) AS total FROM accounts;
COMMIT;

SELECT 'Total after commit (should be 1500)' AS test, SUM(balance) AS total FROM accounts;

-- ============================================================
-- Test 11: Empty Transaction
-- ============================================================

BEGIN;
COMMIT;

BEGIN;
ROLLBACK;

SELECT 'Empty transactions completed' AS test;

-- ============================================================
-- Test 12: Transaction with Only Reads
-- ============================================================

BEGIN;
SELECT COUNT(*) AS account_count FROM accounts;
SELECT SUM(balance) AS total_balance FROM accounts;
SELECT AVG(balance) AS avg_balance FROM accounts;
COMMIT;

SELECT 'Read-only transaction completed' AS test;

-- ============================================================
-- Test 13: Large Batch Insert in Transaction
-- ============================================================

DROP TABLE IF EXISTS batch_test;
CREATE TABLE batch_test (id INT PRIMARY KEY, data TEXT);

BEGIN;
INSERT INTO batch_test VALUES (1, 'row1'), (2, 'row2'), (3, 'row3'), (4, 'row4'), (5, 'row5');
INSERT INTO batch_test VALUES (6, 'row6'), (7, 'row7'), (8, 'row8'), (9, 'row9'), (10, 'row10');
COMMIT;

SELECT 'Batch insert count' AS test, COUNT(*) FROM batch_test;

BEGIN;
INSERT INTO batch_test VALUES (11, 'row11'), (12, 'row12'), (13, 'row13');
ROLLBACK;

SELECT 'Count after rollback (should be 10)' AS test, COUNT(*) FROM batch_test;

-- ============================================================
-- Test 14: Update All Rows in Transaction
-- ============================================================

BEGIN;
UPDATE batch_test SET data = 'updated_' || data;
COMMIT;

SELECT 'Sample updated rows' AS test, id, data FROM batch_test ORDER BY id LIMIT 3;

-- ============================================================
-- Test 15: Delete All and Repopulate in Transaction
-- ============================================================

BEGIN;
DELETE FROM batch_test;
SELECT 'After delete in txn' AS test, COUNT(*) FROM batch_test;
INSERT INTO batch_test VALUES (100, 'new_row');
COMMIT;

SELECT 'After delete and insert' AS test, COUNT(*) FROM batch_test;
SELECT 'New row' AS test, id, data FROM batch_test;

-- ============================================================
-- Test 16: Sequential Transactions
-- ============================================================

BEGIN;
INSERT INTO batch_test VALUES (101, 'txn1');
COMMIT;

BEGIN;
INSERT INTO batch_test VALUES (102, 'txn2');
COMMIT;

BEGIN;
INSERT INTO batch_test VALUES (103, 'txn3');
COMMIT;

SELECT 'After sequential transactions' AS test, COUNT(*) FROM batch_test;

-- ============================================================
-- Cleanup
-- ============================================================

DROP TABLE IF EXISTS batch_test;
DROP TABLE IF EXISTS child_table;
DROP TABLE IF EXISTS parent_table;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS transfer_log;
DROP TABLE IF EXISTS accounts;

SELECT 'All transaction consistency tests completed' AS result;
