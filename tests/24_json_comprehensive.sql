-- Comprehensive JSON/JSONB Integration Tests
-- Based on: https://neon.tech/postgresql/postgresql-tutorial/postgresql-json

-- ============================================
-- 1. Setup: Create tables with JSON/JSONB columns
-- ============================================
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS contacts;

CREATE TABLE products(
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    properties JSONB
);

CREATE TABLE contacts(
   id SERIAL PRIMARY KEY,
   name VARCHAR(255) NOT NULL,
   phones JSONB
);

-- ============================================
-- 2. Storing JSON objects
-- ============================================
INSERT INTO products(name, properties)
VALUES('Ink Fusion T-Shirt','{"color": "white", "size": ["S","M","L","XL"]}')
RETURNING *;

INSERT INTO products(name, properties)
VALUES('ThreadVerse T-Shirt','{"color": "black", "size": ["S","M","L","XL"]}'),
      ('Design Dynamo T-Shirt','{"color": "blue", "size": ["S","M","L","XL"]}')
RETURNING *;

-- ============================================
-- 3. Retrieve JSON data
-- ============================================
SELECT id, name, properties FROM products;

-- ============================================
-- 4. Extract JSON field as JSON using ->
-- ============================================
SELECT id, name, properties -> 'color' AS color FROM products;

-- ============================================
-- 5. Extract JSON field as TEXT using ->>
-- ============================================
SELECT id, name, properties ->> 'color' AS color FROM products;

-- ============================================
-- 6. Filter by JSON field value
-- ============================================
SELECT id, name, properties ->> 'color' AS color
FROM products
WHERE properties ->> 'color' IN ('black', 'white');

SELECT id, name, properties ->> 'color' AS color
FROM products
WHERE properties ->> 'color' = 'blue';

-- ============================================
-- 7. Storing JSON arrays
-- ============================================
INSERT INTO contacts(name, phones)
VALUES
   ('John Doe','["408-111-2222", "408-111-2223"]'),
   ('Jane Doe','["212-111-2222", "212-111-2223"]')
RETURNING *;

-- ============================================
-- 8. Extract array element by index (0-based)
-- ============================================
SELECT name, phones ->> 0 AS work_phone FROM contacts;
SELECT name, phones ->> 1 AS personal_phone FROM contacts;

-- ============================================
-- 9. Nested JSON object access
-- ============================================
DROP TABLE IF EXISTS users_json;
CREATE TABLE users_json(
    id SERIAL PRIMARY KEY,
    data JSONB
);

INSERT INTO users_json(data) VALUES
    ('{"user": {"name": "Alice", "email": "alice@example.com"}, "active": true}'),
    ('{"user": {"name": "Bob", "email": "bob@example.com"}, "active": false}'),
    ('{"user": {"name": "Charlie", "email": "charlie@example.com"}, "active": true}')
RETURNING *;

SELECT id, data -> 'user' ->> 'name' AS username FROM users_json;
SELECT id, data -> 'user' ->> 'email' AS email FROM users_json WHERE data ->> 'active' = 'true';

-- ============================================
-- 10. JSON with nested arrays
-- ============================================
DROP TABLE IF EXISTS orders_json;
CREATE TABLE orders_json(
    id SERIAL PRIMARY KEY,
    order_data JSONB
);

INSERT INTO orders_json(order_data) VALUES
    ('{"customer": "Alice", "items": [{"name": "Book", "qty": 2}, {"name": "Pen", "qty": 5}], "total": 25.50}'),
    ('{"customer": "Bob", "items": [{"name": "Laptop", "qty": 1}], "total": 999.99}')
RETURNING *;

SELECT id, order_data ->> 'customer' AS customer, order_data ->> 'total' AS total FROM orders_json;
SELECT id, order_data -> 'items' -> 0 ->> 'name' AS first_item FROM orders_json;

-- ============================================
-- 11. JSON type (preserves formatting) vs JSONB
-- ============================================
DROP TABLE IF EXISTS json_vs_jsonb;
CREATE TABLE json_vs_jsonb(
    id SERIAL PRIMARY KEY,
    json_col JSON,
    jsonb_col JSONB
);

INSERT INTO json_vs_jsonb(json_col, jsonb_col) VALUES
    ('{"b": 2, "a": 1}', '{"b": 2, "a": 1}')
RETURNING *;

SELECT json_col, jsonb_col FROM json_vs_jsonb;

-- ============================================
-- 12. JSONB containment operators @> and <@
-- ============================================
SELECT * FROM products WHERE properties @> '{"color": "white"}';
SELECT * FROM products WHERE '{"color": "black"}' <@ properties;

-- ============================================
-- 13. Casting to JSON/JSONB
-- ============================================
SELECT '{"test": 123}'::json AS json_cast;
SELECT '{"test": 123}'::jsonb AS jsonb_cast;
SELECT '{"nested": {"key": "value"}}'::jsonb -> 'nested' ->> 'key' AS nested_value;

-- ============================================
-- 14. JSON with numeric values
-- ============================================
DROP TABLE IF EXISTS metrics;
CREATE TABLE metrics(
    id SERIAL PRIMARY KEY,
    data JSONB
);

INSERT INTO metrics(data) VALUES
    ('{"temperature": 72.5, "humidity": 45, "pressure": 1013.25}'),
    ('{"temperature": 68.0, "humidity": 55, "pressure": 1015.00}')
RETURNING *;

SELECT id, data ->> 'temperature' AS temp, data ->> 'humidity' AS humidity FROM metrics;

-- ============================================
-- 15. Complex query with JSON
-- ============================================
SELECT 
    p.name,
    p.properties ->> 'color' AS color,
    p.properties -> 'size' ->> 0 AS smallest_size,
    p.properties -> 'size' ->> 3 AS largest_size
FROM products p
ORDER BY p.name;

-- ============================================
-- 16. Update JSON field (full replace)
-- ============================================
UPDATE products 
SET properties = '{"color": "red", "size": ["XS","S","M","L","XL","XXL"]}'
WHERE name = 'Ink Fusion T-Shirt'
RETURNING *;

SELECT * FROM products WHERE name = 'Ink Fusion T-Shirt';

-- ============================================
-- 17. JSON in WHERE with multiple conditions
-- ============================================
INSERT INTO products(name, properties) VALUES
    ('Premium Polo', '{"color": "white", "size": ["M","L"], "premium": true}'),
    ('Basic Tee', '{"color": "white", "size": ["S","M","L"], "premium": false}')
RETURNING *;

SELECT name, properties ->> 'color' AS color 
FROM products 
WHERE properties ->> 'color' = 'white' 
  AND properties ->> 'premium' = 'true';

-- ============================================
-- 18. Cleanup
-- ============================================
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS contacts;
DROP TABLE IF EXISTS users_json;
DROP TABLE IF EXISTS orders_json;
DROP TABLE IF EXISTS json_vs_jsonb;
DROP TABLE IF EXISTS metrics;

SELECT 'JSON comprehensive tests completed' AS status;
