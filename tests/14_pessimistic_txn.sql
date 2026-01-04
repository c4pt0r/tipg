DROP TABLE IF EXISTS t_pessimistic;
CREATE TABLE t_pessimistic (
    id INT PRIMARY KEY,
    balance INT,
    version INT
);

INSERT INTO t_pessimistic VALUES (1, 1000, 1);
INSERT INTO t_pessimistic VALUES (2, 2000, 1);

SELECT * FROM t_pessimistic ORDER BY id;

BEGIN;
SELECT * FROM t_pessimistic WHERE id = 1 FOR UPDATE;
UPDATE t_pessimistic SET balance = balance - 100, version = version + 1 WHERE id = 1;
UPDATE t_pessimistic SET balance = balance + 100, version = version + 1 WHERE id = 2;
COMMIT;

SELECT * FROM t_pessimistic ORDER BY id;

BEGIN;
SELECT * FROM t_pessimistic WHERE id = 1 FOR UPDATE;
ROLLBACK;

SELECT * FROM t_pessimistic ORDER BY id;

DROP TABLE t_pessimistic;
