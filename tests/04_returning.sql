-- Returning Tests
-- Purpose: Verify INSERT RETURNING

DROP TABLE IF EXISTS t_ret;
CREATE TABLE t_ret (
    id SERIAL PRIMARY KEY,
    tag TEXT
);

-- 1. Return generated ID
INSERT INTO t_ret (tag) VALUES ('A') RETURNING id;

-- 2. Return multiple columns
INSERT INTO t_ret (tag) VALUES ('B') RETURNING id, tag;

-- 3. Return all
INSERT INTO t_ret (tag) VALUES ('C') RETURNING *;

-- Clean up
DROP TABLE t_ret;
