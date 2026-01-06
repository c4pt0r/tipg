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

-- Test 3: Multi-statement in single query (BEGIN; DML; COMMIT)
DROP TABLE IF EXISTS t_txn_multi;
CREATE TABLE t_txn_multi (id INT PRIMARY KEY, val TEXT);
INSERT INTO t_txn_multi VALUES (1, 'original');

-- This should work: BEGIN, UPDATE, COMMIT all in one query string
BEGIN; UPDATE t_txn_multi SET val = 'updated' WHERE id = 1; COMMIT;

SELECT * FROM t_txn_multi WHERE id = 1; -- Should show 'updated'

-- Test 4: Multi-statement with rollback
BEGIN; UPDATE t_txn_multi SET val = 'rolled_back' WHERE id = 1; ROLLBACK;

SELECT * FROM t_txn_multi WHERE id = 1; -- Should still show 'updated' (rollback worked)

-- Test 5: Multi-statement INSERT
BEGIN; INSERT INTO t_txn_multi VALUES (2, 'second'); INSERT INTO t_txn_multi VALUES (3, 'third'); COMMIT;

SELECT * FROM t_txn_multi ORDER BY id; -- Should show all 3 rows

DROP TABLE t_txn_multi;

-- Clean up
DROP TABLE t_txn;
