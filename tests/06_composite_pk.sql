-- Composite Primary Key Tests
-- Purpose: Verify tables with multiple columns in PRIMARY KEY

DROP TABLE IF EXISTS order_line;
CREATE TABLE order_line (
    w_id INT,
    d_id INT,
    o_id INT,
    info TEXT,
    PRIMARY KEY (w_id, d_id, o_id)
);

-- 1. Insert distinct rows
INSERT INTO order_line (w_id, d_id, o_id, info) VALUES (1, 1, 100, 'Order 1');
INSERT INTO order_line (w_id, d_id, o_id, info) VALUES (1, 1, 101, 'Order 2');
INSERT INTO order_line (w_id, d_id, o_id, info) VALUES (1, 2, 100, 'Order 3');

-- 2. Insert duplicate (should fail)
INSERT INTO order_line (w_id, d_id, o_id, info) VALUES (1, 1, 100, 'Duplicate');

-- 3. Select all
SELECT * FROM order_line;

-- 4. Delete by composite key (simulated via WHERE scan)
DELETE FROM order_line WHERE w_id = 1 AND d_id = 1 AND o_id = 100;

-- 5. Verify deletion
SELECT * FROM order_line;

-- Clean up
DROP TABLE order_line;
