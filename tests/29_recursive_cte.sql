-- Recursive CTE Tests
-- Purpose: Verify WITH RECURSIVE functionality

-- ============================================
-- 1. Basic Number Generation
-- ============================================
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 10
)
SELECT * FROM nums;

-- ============================================
-- 2. Fibonacci Sequence
-- ============================================
WITH RECURSIVE fib AS (
    SELECT 1 AS a, 1 AS b, 1 AS n
    UNION ALL
    SELECT b, a + b, n + 1 FROM fib WHERE n < 10
)
SELECT n, a as fib_n FROM fib;

-- ============================================
-- 3. Hierarchical Data - Employee Org Chart
-- ============================================
DROP TABLE IF EXISTS org_employees;
CREATE TABLE org_employees (
    id INT PRIMARY KEY,
    name TEXT,
    manager_id INT,
    title TEXT
);

INSERT INTO org_employees VALUES (1, 'CEO', NULL, 'Chief Executive Officer');
INSERT INTO org_employees VALUES (2, 'CTO', 1, 'Chief Technology Officer');
INSERT INTO org_employees VALUES (3, 'CFO', 1, 'Chief Financial Officer');
INSERT INTO org_employees VALUES (4, 'VP Engineering', 2, 'VP of Engineering');
INSERT INTO org_employees VALUES (5, 'VP Product', 2, 'VP of Product');
INSERT INTO org_employees VALUES (6, 'Senior Dev', 4, 'Senior Developer');
INSERT INTO org_employees VALUES (7, 'Junior Dev', 4, 'Junior Developer');
INSERT INTO org_employees VALUES (8, 'Accountant', 3, 'Staff Accountant');

-- Get full org hierarchy with levels
WITH RECURSIVE org_tree AS (
    SELECT id, name, manager_id, title, 0 AS level, name AS path
    FROM org_employees
    WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, e.title, t.level + 1, t.path || ' > ' || e.name
    FROM org_employees e
    JOIN org_tree t ON e.manager_id = t.id
)
SELECT level, name, title, path FROM org_tree ORDER BY path;

-- ============================================
-- 4. Count Direct Reports (Recursive)
-- ============================================
WITH RECURSIVE subordinates AS (
    SELECT id, name, manager_id, 1 AS depth
    FROM org_employees
    WHERE manager_id = 2  -- CTO's subordinates
    UNION ALL
    SELECT e.id, e.name, e.manager_id, s.depth + 1
    FROM org_employees e
    JOIN subordinates s ON e.manager_id = s.id
)
SELECT * FROM subordinates ORDER BY depth, name;

-- ============================================
-- 5. Category Tree (Self-Referencing)
-- ============================================
DROP TABLE IF EXISTS categories;
CREATE TABLE categories (
    id INT PRIMARY KEY,
    name TEXT,
    parent_id INT
);

INSERT INTO categories VALUES (1, 'Electronics', NULL);
INSERT INTO categories VALUES (2, 'Computers', 1);
INSERT INTO categories VALUES (3, 'Laptops', 2);
INSERT INTO categories VALUES (4, 'Gaming Laptops', 3);
INSERT INTO categories VALUES (5, 'Phones', 1);
INSERT INTO categories VALUES (6, 'Smartphones', 5);
INSERT INTO categories VALUES (7, 'Clothing', NULL);
INSERT INTO categories VALUES (8, 'Mens', 7);
INSERT INTO categories VALUES (9, 'Shirts', 8);

-- Full category path
WITH RECURSIVE cat_path AS (
    SELECT id, name, parent_id, name AS full_path, 1 AS depth
    FROM categories
    WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, p.full_path || ' / ' || c.name, p.depth + 1
    FROM categories c
    JOIN cat_path p ON c.parent_id = p.id
)
SELECT depth, name, full_path FROM cat_path ORDER BY full_path;

-- ============================================
-- 6. Graph Traversal - Find All Paths
-- ============================================
DROP TABLE IF EXISTS graph_edges;
CREATE TABLE graph_edges (
    from_node TEXT,
    to_node TEXT
);

INSERT INTO graph_edges VALUES ('A', 'B');
INSERT INTO graph_edges VALUES ('A', 'C');
INSERT INTO graph_edges VALUES ('B', 'D');
INSERT INTO graph_edges VALUES ('C', 'D');
INSERT INTO graph_edges VALUES ('D', 'E');

-- Find all paths from A
WITH RECURSIVE paths AS (
    SELECT from_node, to_node, from_node || ' -> ' || to_node AS path, 1 AS hops
    FROM graph_edges
    WHERE from_node = 'A'
    UNION ALL
    SELECT p.from_node, e.to_node, p.path || ' -> ' || e.to_node, p.hops + 1
    FROM paths p
    JOIN graph_edges e ON p.to_node = e.from_node
    WHERE p.hops < 5  -- Prevent infinite loops
)
SELECT path, hops FROM paths ORDER BY hops, path;

-- ============================================
-- 7. UNION vs UNION ALL (Deduplication)
-- ============================================
-- UNION ALL keeps duplicates (incrementing x to prevent infinite loop)
WITH RECURSIVE dup_test AS (
    SELECT 1 AS x
    UNION ALL
    SELECT x + 1 FROM dup_test WHERE x < 3
)
SELECT * FROM dup_test;

-- UNION removes duplicates (only returns one row since all values are 1)
WITH RECURSIVE no_dup AS (
    SELECT 1 AS x
    UNION
    SELECT 1 FROM no_dup WHERE x = 1
)
SELECT * FROM no_dup;

-- ============================================
-- 8. Date Sequence Generation
-- ============================================
WITH RECURSIVE dates AS (
    SELECT 1 AS day_num
    UNION ALL
    SELECT day_num + 1 FROM dates WHERE day_num < 7
)
SELECT day_num,
       CASE day_num
           WHEN 1 THEN 'Monday'
           WHEN 2 THEN 'Tuesday'
           WHEN 3 THEN 'Wednesday'
           WHEN 4 THEN 'Thursday'
           WHEN 5 THEN 'Friday'
           WHEN 6 THEN 'Saturday'
           WHEN 7 THEN 'Sunday'
       END AS day_name
FROM dates;

-- ============================================
-- 9. Powers of 2
-- ============================================
WITH RECURSIVE powers AS (
    SELECT 1 AS n, 2 AS power_of_2
    UNION ALL
    SELECT n + 1, power_of_2 * 2 FROM powers WHERE n < 10
)
SELECT n, power_of_2 FROM powers;

-- ============================================
-- 10. String Accumulation
-- ============================================
WITH RECURSIVE letters AS (
    SELECT 1 AS pos, 'A' AS letter, 'A' AS accumulated
    UNION ALL
    SELECT pos + 1, 
           CASE pos + 1
               WHEN 2 THEN 'B'
               WHEN 3 THEN 'C'
               WHEN 4 THEN 'D'
               WHEN 5 THEN 'E'
           END,
           accumulated || CASE pos + 1
               WHEN 2 THEN 'B'
               WHEN 3 THEN 'C'
               WHEN 4 THEN 'D'
               WHEN 5 THEN 'E'
           END
    FROM letters WHERE pos < 5
)
SELECT pos, letter, accumulated FROM letters;

-- ============================================
-- 11. Recursive with Aggregation in Main Query
-- ============================================
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 100
)
SELECT COUNT(*) as total, SUM(n) as sum_1_to_100, AVG(n) as avg FROM nums;

-- ============================================
-- 12. Multiple Recursive CTEs
-- ============================================
WITH RECURSIVE 
    evens AS (
        SELECT 2 AS n
        UNION ALL
        SELECT n + 2 FROM evens WHERE n < 10
    ),
    odds AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n + 2 FROM odds WHERE n < 10
    )
SELECT 'even' AS type, n FROM evens
UNION ALL
SELECT 'odd' AS type, n FROM odds
ORDER BY n;

-- ============================================
-- 13. Tree Depth Calculation
-- ============================================
WITH RECURSIVE tree_depth AS (
    SELECT id, name, parent_id, 0 AS depth
    FROM categories
    WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, t.depth + 1
    FROM categories c
    JOIN tree_depth t ON c.parent_id = t.id
)
SELECT MAX(depth) AS max_tree_depth FROM tree_depth;

-- ============================================
-- 14. Factorial Calculation
-- ============================================
WITH RECURSIVE factorial AS (
    SELECT 1 AS n, 1 AS fact
    UNION ALL
    SELECT n + 1, fact * (n + 1) FROM factorial WHERE n < 10
)
SELECT n, fact FROM factorial;

-- ============================================
-- 15. Ancestors Query (Bottom-Up)
-- ============================================
WITH RECURSIVE ancestors AS (
    SELECT id, name, manager_id, 0 AS level
    FROM org_employees
    WHERE id = 7  -- Junior Dev
    UNION ALL
    SELECT e.id, e.name, e.manager_id, a.level + 1
    FROM org_employees e
    JOIN ancestors a ON e.id = a.manager_id
)
SELECT level, name FROM ancestors ORDER BY level;

-- Cleanup
DROP TABLE graph_edges;
DROP TABLE categories;
DROP TABLE org_employees;
