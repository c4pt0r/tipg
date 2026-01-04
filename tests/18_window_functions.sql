-- Window Functions Tests
-- Purpose: Verify ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, and aggregate window functions

DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name TEXT,
    department TEXT,
    salary INT,
    hire_date TIMESTAMP DEFAULT NOW()
);

INSERT INTO employees (name, department, salary) VALUES ('Alice', 'Engineering', 80000);
INSERT INTO employees (name, department, salary) VALUES ('Bob', 'Engineering', 75000);
INSERT INTO employees (name, department, salary) VALUES ('Charlie', 'Engineering', 90000);
INSERT INTO employees (name, department, salary) VALUES ('Diana', 'Sales', 60000);
INSERT INTO employees (name, department, salary) VALUES ('Eve', 'Sales', 65000);
INSERT INTO employees (name, department, salary) VALUES ('Frank', 'Sales', 70000);
INSERT INTO employees (name, department, salary) VALUES ('Grace', 'HR', 55000);
INSERT INTO employees (name, department, salary) VALUES ('Henry', 'HR', 50000);

-- 1. ROW_NUMBER - sequential numbering
SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM employees;

-- 2. ROW_NUMBER with PARTITION BY
SELECT id, name, department, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank FROM employees;

-- 3. RANK - same value gets same rank, gaps after ties
SELECT id, name, salary, RANK() OVER (ORDER BY salary DESC) as salary_rank FROM employees;

-- 4. DENSE_RANK - same value gets same rank, no gaps
SELECT id, name, salary, DENSE_RANK() OVER (ORDER BY salary DESC) as dense_salary_rank FROM employees;

-- 5. RANK with PARTITION BY
SELECT id, name, department, salary, RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_salary_rank FROM employees;

-- 6. LAG - get previous row's value (default offset=1)
SELECT id, name, salary, LAG(salary) OVER (ORDER BY id) as prev_salary FROM employees;

-- 7. LAG with custom offset
SELECT id, name, salary, LAG(salary, 2) OVER (ORDER BY id) as two_rows_back FROM employees;

-- 8. LAG with default value
SELECT id, name, salary, LAG(salary, 1, 0) OVER (ORDER BY id) as prev_salary_or_zero FROM employees;

-- 9. LEAD - get next row's value
SELECT id, name, salary, LEAD(salary) OVER (ORDER BY id) as next_salary FROM employees;

-- 10. LEAD with custom offset and default
SELECT id, name, salary, LEAD(salary, 1, -1) OVER (ORDER BY id) as next_salary_or_minus1 FROM employees;

-- 11. LAG/LEAD with PARTITION BY
SELECT id, name, department, salary, 
       LAG(name) OVER (PARTITION BY department ORDER BY salary) as prev_in_dept,
       LEAD(name) OVER (PARTITION BY department ORDER BY salary) as next_in_dept
FROM employees;

-- 12. Running SUM
SELECT id, name, salary, SUM(salary) OVER (ORDER BY id) as running_total FROM employees;

-- 13. Running COUNT
SELECT id, name, COUNT(*) OVER (ORDER BY id) as running_count FROM employees;

-- 14. Running AVG
SELECT id, name, salary, AVG(salary) OVER (ORDER BY id) as running_avg FROM employees;

-- 15. Running MIN/MAX
SELECT id, name, salary, 
       MIN(salary) OVER (ORDER BY id) as running_min,
       MAX(salary) OVER (ORDER BY id) as running_max
FROM employees;

-- 16. Aggregate window with PARTITION BY
SELECT id, name, department, salary, 
       SUM(salary) OVER (PARTITION BY department ORDER BY id) as dept_running_total
FROM employees;

-- 17. Multiple window functions in same query
SELECT id, name, department, salary,
       ROW_NUMBER() OVER (ORDER BY id) as row_num,
       RANK() OVER (ORDER BY salary DESC) as salary_rank,
       LAG(salary) OVER (ORDER BY id) as prev_salary,
       LEAD(salary) OVER (ORDER BY id) as next_salary,
       SUM(salary) OVER (ORDER BY id) as running_total
FROM employees;

-- 18. Window function with WHERE clause
SELECT id, name, department, salary,
       ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM employees
WHERE department = 'Engineering';

-- 19. Window function with ORDER BY on result
SELECT id, name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM employees
ORDER BY rank;

-- 20. Window function with LIMIT
SELECT id, name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
FROM employees
LIMIT 3;

DROP TABLE employees;

-- Test with duplicate values for RANK vs DENSE_RANK
DROP TABLE IF EXISTS scores;
CREATE TABLE scores (
    id SERIAL PRIMARY KEY,
    student TEXT,
    score INT
);

INSERT INTO scores (student, score) VALUES ('A', 100);
INSERT INTO scores (student, score) VALUES ('B', 95);
INSERT INTO scores (student, score) VALUES ('C', 95);
INSERT INTO scores (student, score) VALUES ('D', 90);
INSERT INTO scores (student, score) VALUES ('E', 85);

-- RANK: 1, 2, 2, 4, 5 (gap after tie)
SELECT student, score, RANK() OVER (ORDER BY score DESC) as rank FROM scores;

-- DENSE_RANK: 1, 2, 2, 3, 4 (no gap after tie)
SELECT student, score, DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank FROM scores;

DROP TABLE scores;
