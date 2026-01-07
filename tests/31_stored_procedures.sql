-- Stored Procedures Tests

DROP PROCEDURE IF EXISTS simple_insert;
DROP PROCEDURE IF EXISTS update_salary;
DROP PROCEDURE IF EXISTS bulk_insert;
DROP PROCEDURE IF EXISTS conditional_update;
DROP PROCEDURE IF EXISTS transfer_funds;
DROP TABLE IF EXISTS proc_accounts;
DROP TABLE IF EXISTS proc_employees;
DROP TABLE IF EXISTS proc_logs;

CREATE TABLE proc_employees (
    id SERIAL PRIMARY KEY,
    name TEXT,
    department TEXT,
    salary INT
);

CREATE TABLE proc_accounts (
    id SERIAL PRIMARY KEY,
    owner TEXT,
    balance INT
);

CREATE TABLE proc_logs (
    id SERIAL PRIMARY KEY,
    action TEXT,
    details TEXT
);

INSERT INTO proc_employees (name, department, salary) VALUES ('Alice', 'Engineering', 80000);
INSERT INTO proc_employees (name, department, salary) VALUES ('Bob', 'Engineering', 75000);
INSERT INTO proc_employees (name, department, salary) VALUES ('Charlie', 'Sales', 60000);
INSERT INTO proc_employees (name, department, salary) VALUES ('Diana', 'Sales', 65000);

INSERT INTO proc_accounts (owner, balance) VALUES ('Alice', 10000);
INSERT INTO proc_accounts (owner, balance) VALUES ('Bob', 5000);
INSERT INTO proc_accounts (owner, balance) VALUES ('Charlie', 8000);

-- 1. Simple INSERT Procedure
CREATE PROCEDURE simple_insert(p_name TEXT, p_dept TEXT, p_salary INT)
AS BEGIN
INSERT INTO proc_employees (name, department, salary) VALUES (p_name, p_dept, p_salary)
END;

SELECT * FROM proc_employees ORDER BY id;

CALL simple_insert('Eve', 'HR', 55000);

SELECT * FROM proc_employees ORDER BY id;

-- 2. UPDATE Procedure with INT Parameter
CREATE PROCEDURE update_salary(p_department TEXT, p_increase INT)
AS BEGIN
UPDATE proc_employees SET salary = salary + p_increase WHERE department = p_department
END;

SELECT name, department, salary FROM proc_employees WHERE department = 'Engineering';

CALL update_salary('Engineering', 5000);

SELECT name, department, salary FROM proc_employees WHERE department = 'Engineering';

-- 3. Procedure with Multiple Statements
CREATE PROCEDURE bulk_insert(p_prefix TEXT, p_count INT)
AS BEGIN
INSERT INTO proc_logs (action, details) VALUES ('bulk_start', p_prefix);
INSERT INTO proc_employees (name, department, salary) VALUES (p_prefix, 'Temp', 40000)
END;

SELECT COUNT(*) as before_count FROM proc_employees;

CALL bulk_insert('TempUser', 1);

SELECT COUNT(*) as after_count FROM proc_employees;
SELECT * FROM proc_logs;

-- 4. Procedure Affecting Multiple Rows
CREATE PROCEDURE raise_all(p_percent INT)
AS BEGIN
UPDATE proc_employees SET salary = salary * (100 + p_percent) / 100
END;

SELECT name, salary FROM proc_employees ORDER BY salary;

CALL raise_all(10);

SELECT name, salary FROM proc_employees ORDER BY salary;

-- 5. Procedure with DELETE
DROP PROCEDURE IF EXISTS cleanup_temp;
CREATE PROCEDURE cleanup_temp(p_dept TEXT)
AS BEGIN
DELETE FROM proc_employees WHERE department = p_dept
END;

SELECT COUNT(*) as before_cleanup FROM proc_employees;

CALL cleanup_temp('Temp');

SELECT COUNT(*) as after_cleanup FROM proc_employees;

-- 6. Procedure for Logging
DROP PROCEDURE IF EXISTS log_action;
CREATE PROCEDURE log_action(p_action TEXT, p_details TEXT)
AS BEGIN
INSERT INTO proc_logs (action, details) VALUES (p_action, p_details)
END;

CALL log_action('USER_LOGIN', 'User Alice logged in');
CALL log_action('DATA_UPDATE', 'Salary update completed');

SELECT * FROM proc_logs ORDER BY id;

-- 7. Procedure with Numeric Calculation
DROP PROCEDURE IF EXISTS set_bonus;
CREATE PROCEDURE set_bonus(p_name TEXT, p_bonus_pct INT)
AS BEGIN
UPDATE proc_employees SET salary = salary + salary * p_bonus_pct / 100 WHERE name = p_name
END;

SELECT name, salary FROM proc_employees WHERE name = 'Alice';

CALL set_bonus('Alice', 15);

SELECT name, salary FROM proc_employees WHERE name = 'Alice';

-- 8. DROP PROCEDURE
DROP PROCEDURE simple_insert;
DROP PROCEDURE update_salary;

-- 9. DROP PROCEDURE IF EXISTS
DROP PROCEDURE IF EXISTS nonexistent_proc;

-- 10. Verify Procedure Deletion
CALL simple_insert('Test', 'Test', 1000);

-- 11. Recreate and Use Procedure
CREATE PROCEDURE simple_insert(p_name TEXT, p_dept TEXT, p_salary INT)
AS BEGIN
INSERT INTO proc_employees (name, department, salary) VALUES (p_name, p_dept, p_salary)
END;

CALL simple_insert('Frank', 'Marketing', 58000);

SELECT * FROM proc_employees WHERE name = 'Frank';

-- 12. Procedure with Account Transfer Logic
CREATE PROCEDURE transfer_funds(p_from TEXT, p_to TEXT, p_amount INT)
AS BEGIN
UPDATE proc_accounts SET balance = balance - p_amount WHERE owner = p_from;
UPDATE proc_accounts SET balance = balance + p_amount WHERE owner = p_to
END;

SELECT * FROM proc_accounts ORDER BY owner;

CALL transfer_funds('Alice', 'Bob', 2000);

SELECT * FROM proc_accounts ORDER BY owner;

-- 13. Verify Multiple Procedure Calls
CALL log_action('TRANSFER', 'Alice to Bob 2000');
CALL transfer_funds('Bob', 'Charlie', 1000);
CALL log_action('TRANSFER', 'Bob to Charlie 1000');

SELECT * FROM proc_accounts ORDER BY owner;
SELECT * FROM proc_logs WHERE action = 'TRANSFER';

-- 14. Procedure Statistics
SELECT COUNT(*) as total_employees FROM proc_employees;
SELECT SUM(balance) as total_balance FROM proc_accounts;
SELECT COUNT(*) as log_entries FROM proc_logs;

-- Cleanup
DROP PROCEDURE IF EXISTS simple_insert;
DROP PROCEDURE IF EXISTS bulk_insert;
DROP PROCEDURE IF EXISTS raise_all;
DROP PROCEDURE IF EXISTS cleanup_temp;
DROP PROCEDURE IF EXISTS log_action;
DROP PROCEDURE IF EXISTS set_bonus;
DROP PROCEDURE IF EXISTS transfer_funds;
DROP TABLE proc_logs;
DROP TABLE proc_accounts;
DROP TABLE proc_employees;
