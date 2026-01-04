-- CTE (Common Table Expression) Tests
-- Purpose: Verify WITH ... AS syntax and CTE functionality

DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
    id SERIAL PRIMARY KEY,
    name TEXT,
    department TEXT,
    manager_id INT,
    salary INT
);

INSERT INTO employees (name, department, manager_id, salary) VALUES ('Alice', 'Engineering', NULL, 100000);
INSERT INTO employees (name, department, manager_id, salary) VALUES ('Bob', 'Engineering', 1, 80000);
INSERT INTO employees (name, department, manager_id, salary) VALUES ('Charlie', 'Engineering', 1, 75000);
INSERT INTO employees (name, department, manager_id, salary) VALUES ('Diana', 'Sales', NULL, 90000);
INSERT INTO employees (name, department, manager_id, salary) VALUES ('Eve', 'Sales', 4, 60000);
INSERT INTO employees (name, department, manager_id, salary) VALUES ('Frank', 'HR', NULL, 70000);

-- 1. Simple CTE
WITH eng AS (SELECT * FROM employees WHERE department = 'Engineering')
SELECT * FROM eng;

-- 2. CTE with aggregation
WITH dept_stats AS (
    SELECT department, COUNT(*) as emp_count, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
)
SELECT * FROM dept_stats;

-- 3. CTE with filtering in main query
WITH high_earners AS (SELECT * FROM employees WHERE salary > 70000)
SELECT name, salary FROM high_earners ORDER BY salary DESC;

-- 4. Multiple CTEs
WITH 
    eng AS (SELECT * FROM employees WHERE department = 'Engineering'),
    sales AS (SELECT * FROM employees WHERE department = 'Sales')
SELECT 'Engineering' as dept, COUNT(*) as count FROM eng
UNION ALL
SELECT 'Sales' as dept, COUNT(*) as count FROM sales;

-- 5. CTE referencing another CTE (chained)
WITH 
    all_emp AS (SELECT * FROM employees),
    high_salary AS (SELECT * FROM all_emp WHERE salary > 75000)
SELECT * FROM high_salary;

-- 6. CTE with ORDER BY and LIMIT
WITH top_earners AS (
    SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 3
)
SELECT * FROM top_earners;

-- 7. CTE with JOIN in main query
DROP TABLE IF EXISTS departments;
CREATE TABLE departments (
    id SERIAL PRIMARY KEY,
    name TEXT,
    budget INT
);

INSERT INTO departments (name, budget) VALUES ('Engineering', 500000);
INSERT INTO departments (name, budget) VALUES ('Sales', 300000);
INSERT INTO departments (name, budget) VALUES ('HR', 200000);

WITH emp_counts AS (
    SELECT department, COUNT(*) as cnt FROM employees GROUP BY department
)
SELECT d.name, d.budget, e.cnt
FROM departments d
JOIN emp_counts e ON d.name = e.department;

-- 8. CTE with subquery in definition
WITH managers AS (
    SELECT * FROM employees WHERE id IN (SELECT DISTINCT manager_id FROM employees WHERE manager_id IS NOT NULL)
)
SELECT * FROM managers;

-- 9. CTE used multiple times in main query
WITH dept_avg AS (
    SELECT department, AVG(salary) as avg_sal FROM employees GROUP BY department
)
SELECT e.name, e.department, e.salary, d.avg_sal,
       CASE WHEN e.salary > d.avg_sal THEN 'above' ELSE 'below' END as comparison
FROM employees e
JOIN dept_avg d ON e.department = d.department;

-- 10. CTE with window function
WITH ranked AS (
    SELECT name, department, salary,
           RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
)
SELECT * FROM ranked WHERE dept_rank = 1;

-- 11. Nested CTEs for step-by-step transformation
WITH 
    step1 AS (SELECT *, salary * 1.1 as new_salary FROM employees),
    step2 AS (SELECT name, department, new_salary FROM step1 WHERE new_salary > 80000)
SELECT * FROM step2;

-- 12. CTE with DISTINCT
WITH unique_depts AS (SELECT DISTINCT department FROM employees)
SELECT * FROM unique_depts ORDER BY department;

-- 13. CTE in complex query
WITH 
    eng_salaries AS (SELECT salary FROM employees WHERE department = 'Engineering'),
    max_eng AS (SELECT MAX(salary) as max_sal FROM eng_salaries)
SELECT e.name, e.salary
FROM employees e, max_eng m
WHERE e.salary = m.max_sal;

-- 14. CTE with NULL handling
WITH nullable AS (
    SELECT name, COALESCE(manager_id, 0) as mgr FROM employees
)
SELECT * FROM nullable WHERE mgr = 0;

-- 15. CTE for readability in aggregation
WITH monthly_equiv AS (
    SELECT name, department, salary / 12 as monthly_salary FROM employees
)
SELECT department, AVG(monthly_salary) as avg_monthly FROM monthly_equiv GROUP BY department;

DROP TABLE departments;
DROP TABLE employees;
