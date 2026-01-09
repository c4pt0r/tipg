-- Test new features: UNION, UPSERT, DROP INDEX, ALTER TABLE, RIGHT/FULL JOIN, JSON operators, ARRAY

-- ============================================
-- 1. UNION / UNION ALL Tests
-- ============================================
DROP TABLE IF EXISTS union_a;
DROP TABLE IF EXISTS union_b;
CREATE TABLE union_a (id INT PRIMARY KEY, name TEXT);
CREATE TABLE union_b (id INT PRIMARY KEY, name TEXT);

INSERT INTO union_a VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO union_b VALUES (2, 'Bob'), (3, 'Charlie');

SELECT * FROM union_a UNION SELECT * FROM union_b ORDER BY id;
SELECT * FROM union_a UNION ALL SELECT * FROM union_b ORDER BY id;

SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 ORDER BY n;
SELECT 'a' AS letter UNION SELECT 'b' UNION SELECT 'a' ORDER BY letter;

DROP TABLE union_a;
DROP TABLE union_b;

-- ============================================
-- 2. INSERT ... ON CONFLICT (UPSERT) Tests
-- ============================================
DROP TABLE IF EXISTS upsert_test;
CREATE TABLE upsert_test (
    id INT PRIMARY KEY,
    name TEXT,
    counter INT DEFAULT 0
);

INSERT INTO upsert_test VALUES (1, 'first', 1);
INSERT INTO upsert_test VALUES (1, 'updated', 100) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, counter = EXCLUDED.counter;
SELECT * FROM upsert_test WHERE id = 1;

INSERT INTO upsert_test VALUES (2, 'second', 1);
INSERT INTO upsert_test VALUES (2, 'ignored', 999) ON CONFLICT (id) DO NOTHING;
SELECT * FROM upsert_test WHERE id = 2;

DROP TABLE upsert_test;

-- ============================================
-- 3. DROP INDEX Tests
-- ============================================
DROP TABLE IF EXISTS idx_test;
CREATE TABLE idx_test (id INT PRIMARY KEY, name TEXT, email TEXT);
CREATE INDEX idx_name ON idx_test(name);
CREATE INDEX idx_email ON idx_test(email);

DROP INDEX idx_name;
DROP INDEX IF EXISTS idx_name;
DROP INDEX IF EXISTS idx_email;

DROP TABLE idx_test;

-- ============================================
-- 4. ALTER TABLE DROP/RENAME COLUMN Tests
-- ============================================
DROP TABLE IF EXISTS alter_test;
CREATE TABLE alter_test (id INT PRIMARY KEY, name TEXT, old_col TEXT, age INT);

INSERT INTO alter_test VALUES (1, 'Test', 'to_drop', 25);

ALTER TABLE alter_test DROP COLUMN old_col;
SELECT * FROM alter_test;

ALTER TABLE alter_test RENAME COLUMN name TO full_name;
SELECT id, full_name, age FROM alter_test;

DROP TABLE alter_test;

-- ============================================
-- 5. RIGHT JOIN / FULL OUTER JOIN Tests
-- ============================================
DROP TABLE IF EXISTS join_left;
DROP TABLE IF EXISTS join_right;
CREATE TABLE join_left (id INT PRIMARY KEY, val TEXT);
CREATE TABLE join_right (id INT PRIMARY KEY, left_id INT, data TEXT);

INSERT INTO join_left VALUES (1, 'A'), (2, 'B'), (3, 'C');
INSERT INTO join_right VALUES (10, 1, 'X'), (20, 2, 'Y'), (30, 99, 'Z');

SELECT l.id, l.val, r.data FROM join_left l RIGHT JOIN join_right r ON l.id = r.left_id ORDER BY r.id;
SELECT l.id, l.val, r.data FROM join_left l FULL OUTER JOIN join_right r ON l.id = r.left_id ORDER BY COALESCE(l.id, 0), COALESCE(r.id, 0);

DROP TABLE join_left;
DROP TABLE join_right;

-- ============================================
-- 6. JSON Operators Tests
-- ============================================
SELECT '{"name": "Alice", "age": 30}'::jsonb -> 'name';
SELECT '{"name": "Alice", "age": 30}'::jsonb ->> 'name';
SELECT '{"name": "Alice", "age": 30}'::jsonb ->> 'age';

SELECT '{"user": {"name": "Bob"}}'::jsonb -> 'user' ->> 'name';

SELECT '[1, 2, 3]'::jsonb -> 0;
SELECT '["a", "b", "c"]'::jsonb ->> 1;

SELECT '{"items": [1, 2, 3]}'::jsonb -> 'items' -> 0;

-- ============================================
-- 7. ARRAY Tests
-- ============================================
SELECT ARRAY[1, 2, 3];
SELECT ARRAY['hello', 'world'];

SELECT (ARRAY[10, 20, 30])[1];
SELECT (ARRAY[10, 20, 30])[2];
SELECT (ARRAY['a', 'b', 'c'])[3];

SELECT array_length(ARRAY[1, 2, 3, 4, 5], 1);
SELECT cardinality(ARRAY[1, 2, 3]);

SELECT array_position(ARRAY['a', 'b', 'c'], 'b');
SELECT array_position(ARRAY[10, 20, 30], 20);

SELECT array_cat(ARRAY[1, 2], ARRAY[3, 4]);
SELECT array_append(ARRAY[1, 2], 3);
SELECT array_prepend(0, ARRAY[1, 2]);
SELECT array_remove(ARRAY[1, 2, 3, 2], 2);

SELECT array_upper(ARRAY[1, 2, 3], 1);
SELECT array_lower(ARRAY[1, 2, 3], 1);

-- ============================================
-- 8. Combined Feature Tests
-- ============================================
DROP TABLE IF EXISTS combined_test;
CREATE TABLE combined_test (
    id INT PRIMARY KEY,
    name TEXT,
    data TEXT,
    tags TEXT
);

INSERT INTO combined_test VALUES (1, 'Item1', '{"price": 100}', '{tag1,tag2}');
INSERT INTO combined_test VALUES (2, 'Item2', '{"price": 200}', '{tag2,tag3}');

SELECT id, name, data ->> 'price' AS price FROM combined_test ORDER BY id;

INSERT INTO combined_test VALUES (1, 'Updated', '{"price": 150}', '{new}')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, data = EXCLUDED.data;
SELECT * FROM combined_test WHERE id = 1;

SELECT id, name FROM combined_test WHERE id = 1
UNION
SELECT id, name FROM combined_test WHERE id = 2
ORDER BY id;

DROP TABLE combined_test;

-- ============================================
-- 9. JSON/JSONB Column Type Tests
-- ============================================
DROP TABLE IF EXISTS json_test;
CREATE TABLE json_test (
    id INT PRIMARY KEY,
    data_json JSON,
    data_jsonb JSONB
);

INSERT INTO json_test VALUES (1, '{"name": "Alice", "age": 30}', '{"name": "Alice", "age": 30}');
INSERT INTO json_test VALUES (2, '{"name": "Bob", "tags": ["a", "b"]}', '{"name": "Bob", "tags": ["a", "b"]}');

SELECT id, data_json ->> 'name' AS json_name, data_jsonb ->> 'name' AS jsonb_name FROM json_test ORDER BY id;

SELECT id, data_jsonb -> 'tags' -> 0 AS first_tag FROM json_test WHERE id = 2;

UPDATE json_test SET data_json = '{"name": "Charlie", "age": 25}' WHERE id = 1;
SELECT id, data_json ->> 'name' FROM json_test WHERE id = 1;

SELECT '{"a":1}'::json ->> 'a' AS cast_json;
SELECT '{"b":2}'::jsonb ->> 'b' AS cast_jsonb;

SELECT '{"a":1,"b":2}'::jsonb @> '{"a":1}'::jsonb AS contains_true;
SELECT '{"a":1}'::jsonb @> '{"a":1,"b":2}'::jsonb AS contains_false;

SELECT '{"a":1}'::jsonb <@ '{"a":1,"b":2}'::jsonb AS contained_true;
SELECT '{"a":1,"b":2}'::jsonb <@ '{"a":1}'::jsonb AS contained_false;

DROP TABLE json_test;

SELECT 'All new feature tests completed!' AS status;
