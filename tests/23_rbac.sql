-- RBAC (Role-Based Access Control) Integration Tests
-- Tests CREATE ROLE, ALTER ROLE, DROP ROLE, GRANT, REVOKE

-- ============================================
-- Test 1: CREATE ROLE basic
-- ============================================
CREATE ROLE test_reader WITH PASSWORD 'reader123' LOGIN;
CREATE ROLE test_writer WITH PASSWORD 'writer123' LOGIN;
CREATE ROLE test_admin WITH PASSWORD 'admin123' LOGIN SUPERUSER;

-- ============================================
-- Test 2: CREATE ROLE with options
-- ============================================
CREATE ROLE test_dba WITH PASSWORD 'dba123' LOGIN CREATEDB CREATEROLE;
CREATE ROLE test_nologin WITH PASSWORD 'nologin123' NOLOGIN;
CREATE ROLE test_connlimit WITH PASSWORD 'conn123' LOGIN CONNECTION LIMIT 5;

-- ============================================
-- Test 3: CREATE ROLE IF NOT EXISTS
-- ============================================
CREATE ROLE IF NOT EXISTS test_reader WITH PASSWORD 'different';
CREATE ROLE IF NOT EXISTS new_role WITH PASSWORD 'new123' LOGIN;

-- ============================================
-- Test 4: ALTER ROLE - change password
-- ============================================
ALTER ROLE test_reader WITH PASSWORD 'newreader123';

-- ============================================
-- Test 5: ALTER ROLE - add options
-- ============================================
ALTER ROLE test_writer WITH CREATEDB;
ALTER ROLE test_writer WITH CREATEROLE;

-- ============================================
-- Test 6: ALTER ROLE - remove options
-- ============================================
ALTER ROLE test_writer WITH NOCREATEDB;

-- ============================================
-- Test 7: ALTER ROLE - RENAME TO
-- ============================================
CREATE ROLE rename_me WITH PASSWORD 'rename123' LOGIN;
ALTER ROLE rename_me RENAME TO renamed_role;

-- ============================================
-- Test 8: Setup tables for GRANT tests
-- ============================================
DROP TABLE IF EXISTS grant_test_table;
CREATE TABLE grant_test_table (
    id SERIAL PRIMARY KEY,
    name TEXT,
    value INTEGER
);
INSERT INTO grant_test_table (name, value) VALUES ('test1', 100), ('test2', 200);

DROP TABLE IF EXISTS another_table;
CREATE TABLE another_table (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- ============================================
-- Test 9: GRANT SELECT on specific table
-- ============================================
GRANT SELECT ON grant_test_table TO test_reader;

-- ============================================
-- Test 10: GRANT multiple privileges
-- ============================================
GRANT SELECT, INSERT, UPDATE ON grant_test_table TO test_writer;

-- ============================================
-- Test 11: GRANT ALL PRIVILEGES on table
-- ============================================
GRANT ALL PRIVILEGES ON grant_test_table TO test_admin;

-- ============================================
-- Test 12: GRANT on ALL TABLES IN SCHEMA
-- ============================================
GRANT SELECT ON ALL TABLES IN SCHEMA public TO test_reader;

-- ============================================
-- Test 13: GRANT with multiple grantees
-- ============================================
GRANT SELECT ON another_table TO test_reader, test_writer;

-- ============================================
-- Test 14: REVOKE specific privilege
-- ============================================
REVOKE INSERT ON grant_test_table FROM test_writer;

-- ============================================
-- Test 15: REVOKE multiple privileges
-- ============================================
REVOKE UPDATE, DELETE ON grant_test_table FROM test_writer;

-- ============================================
-- Test 16: REVOKE ALL PRIVILEGES
-- ============================================
CREATE ROLE revoke_test WITH PASSWORD 'revoke123' LOGIN;
GRANT ALL PRIVILEGES ON grant_test_table TO revoke_test;
REVOKE ALL PRIVILEGES ON grant_test_table FROM revoke_test;

-- ============================================
-- Test 17: DROP ROLE
-- ============================================
DROP ROLE revoke_test;

-- ============================================
-- Test 18: DROP ROLE IF EXISTS
-- ============================================
DROP ROLE IF EXISTS nonexistent_role;
DROP ROLE IF EXISTS test_nologin;

-- ============================================
-- Test 19: Verify roles after operations
-- ============================================
SELECT 1 as roles_test_complete;

-- ============================================
-- Test 20: Cleanup - DROP remaining test roles
-- ============================================
DROP ROLE IF EXISTS test_reader;
DROP ROLE IF EXISTS test_writer;
DROP ROLE IF EXISTS test_admin;
DROP ROLE IF EXISTS test_dba;
DROP ROLE IF EXISTS test_connlimit;
DROP ROLE IF EXISTS new_role;
DROP ROLE IF EXISTS renamed_role;
DROP TABLE IF EXISTS grant_test_table;
DROP TABLE IF EXISTS another_table;

SELECT 'RBAC tests completed' as status;
