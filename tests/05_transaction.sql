-- Transaction Tests
-- Purpose: Verify BEGIN, COMMIT, ROLLBACK and Session state

DROP TABLE IF EXISTS t_txn;
CREATE TABLE t_txn (
    id INT PRIMARY KEY,
    val TEXT
);

-- Test 1: Rollback
BEGIN;
INSERT INTO t_txn (id, val) VALUES (1, 'rolled_back');
ROLLBACK;

SELECT * FROM t_txn; -- Should be empty

-- Test 2: Commit
BEGIN;
INSERT INTO t_txn (id, val) VALUES (2, 'committed');
INSERT INTO t_txn (id, val) VALUES (3, 'also_committed');
COMMIT;

SELECT * FROM t_txn ORDER BY id; -- Should have 2 and 3

-- Clean up
DROP TABLE t_txn;
