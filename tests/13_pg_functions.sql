-- Test PostgreSQL functions and expressions

-- String concatenation with ||
SELECT 'Hello' || ' ' || 'World';
SELECT 'Count: ' || 42;

-- CASE WHEN expressions
SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END;
SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END;
SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END;

-- String functions
SELECT UPPER('hello');
SELECT LOWER('HELLO');
SELECT LENGTH('hello');
SELECT CONCAT('a', 'b', 'c');
SELECT LEFT('hello', 2);
SELECT RIGHT('hello', 2);
SELECT REPLACE('hello world', 'world', 'there');
SELECT REVERSE('hello');
SELECT REPEAT('ab', 3);

-- Math functions
SELECT ABS(-5);
SELECT CEIL(4.3);
SELECT FLOOR(4.7);
SELECT ROUND(4.567, 2);
SELECT SQRT(16);
SELECT POWER(2, 10);
SELECT MOD(17, 5);
SELECT SIGN(-5);

-- COALESCE and NULLIF
SELECT COALESCE(NULL, NULL, 'default');
SELECT COALESCE('first', NULL, 'default');
SELECT NULLIF(5, 5);
SELECT NULLIF(5, 3);

-- GREATEST and LEAST
SELECT GREATEST(1, 5, 3);
SELECT LEAST(1, 5, 3);

-- LIKE pattern matching
CREATE TABLE test_like (id INT PRIMARY KEY, name TEXT);
INSERT INTO test_like VALUES (1, 'Alice');
INSERT INTO test_like VALUES (2, 'Bob');
INSERT INTO test_like VALUES (3, 'Charlie');
INSERT INTO test_like VALUES (4, 'David');

SELECT * FROM test_like WHERE name LIKE 'A%';
SELECT * FROM test_like WHERE name LIKE '%li%';
SELECT * FROM test_like WHERE name NOT LIKE 'A%';
SELECT * FROM test_like WHERE name ILIKE 'a%';

DROP TABLE test_like;

-- CAST expressions
SELECT CAST(123 AS TEXT);
SELECT CAST('456' AS INTEGER);
SELECT CAST(3.14 AS INTEGER);

-- TRIM functions
SELECT TRIM('  hello  ');
SELECT TRIM(LEADING ' ' FROM '  hello');
SELECT TRIM(TRAILING ' ' FROM 'hello  ');

-- POSITION/SUBSTRING
SELECT POSITION('lo' IN 'hello');
SELECT SUBSTRING('hello' FROM 2 FOR 3);
SELECT SUBSTRING('hello' FROM 2);

-- BETWEEN
SELECT 5 BETWEEN 1 AND 10;
SELECT 15 BETWEEN 1 AND 10;
SELECT 5 NOT BETWEEN 10 AND 20;

-- IN list
SELECT 5 IN (1, 3, 5, 7);
SELECT 4 IN (1, 3, 5, 7);
SELECT 4 NOT IN (1, 3, 5, 7);

-- IS NULL / IS NOT NULL
SELECT NULL IS NULL;
SELECT 5 IS NULL;
SELECT 5 IS NOT NULL;

-- GROUP BY with HAVING
CREATE TABLE test_having (id INT PRIMARY KEY, category TEXT, amount INT);
INSERT INTO test_having VALUES (1, 'A', 10);
INSERT INTO test_having VALUES (2, 'A', 20);
INSERT INTO test_having VALUES (3, 'B', 15);
INSERT INTO test_having VALUES (4, 'B', 25);
INSERT INTO test_having VALUES (5, 'C', 5);

SELECT category, SUM(amount) FROM test_having GROUP BY category HAVING SUM(amount) > 20;
SELECT category, COUNT(*) FROM test_having GROUP BY category HAVING COUNT(*) >= 2;

DROP TABLE test_having;
