# pg-tikv vs Neon PostgreSQL Tutorial 功能评估

基于 https://neon.tech/postgresql/tutorial 的完整功能评估

## 评估摘要

| 状态 | 数量 | 说明 |
|------|------|------|
| ✅ 已支持 | 47 | 无需修改 |
| 🟡 部分支持 | 8 | 需要小改动 |
| ❌ 未支持 | 18 | 需要实现 |

---

## 按实现难度/收益排序的功能清单

### Tier 1: 高收益 + 低难度 (Quick Wins)

| # | 功能 | 难度 | 收益 | 说明 |
|---|------|------|------|------|
| 1 | **FETCH FIRST N ROWS** | 🟢 低 | ⭐⭐⭐ | SQL标准分页语法，直接映射到 LIMIT，1小时 |
| 2 | **NATURAL JOIN** | 🟢 低 | ⭐⭐ | 自动匹配同名列，2小时 |
| 3 | **CROSS JOIN** | 🟢 低 | ⭐⭐ | 已解析，需完善处理，1小时 |
| 4 | **UPDATE ... FROM** | 🟢 低 | ⭐⭐⭐ | UPDATE JOIN，常用语法，3小时 |
| 5 | **CREATE TABLE AS** | 🟢 低 | ⭐⭐⭐ | 从查询创建表，3小时 |
| 6 | **SELECT INTO** | 🟢 低 | ⭐⭐ | 类似 CREATE TABLE AS，1小时 |
| 7 | **TEMP TABLE** | 🟡 中 | ⭐⭐⭐ | 会话级临时表，需要session管理，4小时 |

**预计总工时**: ~15小时

### Tier 2: 高收益 + 中等难度 (High Value)

| # | 功能 | 难度 | 收益 | 说明 |
|---|------|------|------|------|
| 8 | **ANY/SOME operator** | 🟡 中 | ⭐⭐⭐ | `= ANY(subquery)`，常用，4小时 |
| 9 | **ALL operator** | 🟡 中 | ⭐⭐⭐ | `> ALL(subquery)`，4小时 |
| 10 | **Correlated Subquery** | 🟡 中 | ⭐⭐⭐ | 外层引用内层，需要context传递，6小时 |
| 11 | **NUMERIC/DECIMAL** | 🟡 中 | ⭐⭐⭐ | 精确数值类型，需要新的Value variant，6小时 |
| 12 | **DATE type** | 🟢 低 | ⭐⭐⭐ | 独立日期类型（非TIMESTAMP），3小时 |
| 13 | **TIME type** | 🟢 低 | ⭐⭐ | 独立时间类型，3小时 |
| 14 | **Sequence (NEXTVAL)** | 🟡 中 | ⭐⭐⭐ | CREATE SEQUENCE, NEXTVAL, CURRVAL，6小时 |

**预计总工时**: ~32小时

### Tier 3: 中等收益 + 中等难度 (Standard Features)

| # | 功能 | 难度 | 收益 | 说明 |
|---|------|------|------|------|
| 15 | **GROUPING SETS** | 🟡 中 | ⭐⭐ | 多维聚合，6小时 |
| 16 | **CUBE** | 🟡 中 | ⭐⭐ | 所有组合聚合，基于GROUPING SETS，2小时 |
| 17 | **ROLLUP** | 🟡 中 | ⭐⭐ | 层级聚合，基于GROUPING SETS，2小时 |
| 18 | **IDENTITY column** | 🟡 中 | ⭐⭐ | GENERATED ALWAYS AS IDENTITY，4小时 |
| 19 | **RENAME TABLE** | 🟢 低 | ⭐ | ALTER TABLE RENAME TO，2小时 |
| 20 | **COPY TABLE** | 🟡 中 | ⭐⭐ | CREATE TABLE new AS SELECT * FROM old，2小时 |
| 21 | **CHECK constraint** | 🟡 中 | ⭐⭐ | 约束检查（已解析，需执行），4小时 |

**预计总工时**: ~22小时

### Tier 4: 中等收益 + 高难度 (Complex Features)

| # | 功能 | 难度 | 收益 | 说明 |
|---|------|------|------|------|
| 22 | **WITH RECURSIVE** | 🔴 高 | ⭐⭐⭐ | 递归CTE，需要迭代执行，12小时 |
| 23 | **Foreign Key enforcement** | 🔴 高 | ⭐⭐⭐ | 外键约束执行（已解析），8小时 |
| 24 | **DELETE CASCADE** | 🔴 高 | ⭐⭐ | 级联删除，依赖FK，6小时 |
| 25 | **Array functions** | 🟡 中 | ⭐⭐ | array_agg, unnest等，8小时 |

**预计总工时**: ~34小时

### Tier 5: 低收益或高难度 (Advanced/Optional)

| # | 功能 | 难度 | 收益 | 说明 |
|---|------|------|------|------|
| 26 | **hstore** | 🔴 高 | ⭐ | key-value类型，12小时 |
| 27 | **ENUM type** | 🟡 中 | ⭐⭐ | CREATE TYPE AS ENUM，6小时 |
| 28 | **User-defined types** | 🔴 高 | ⭐ | CREATE DOMAIN, CREATE TYPE，12小时 |
| 29 | **Composite types** | 🔴 高 | ⭐ | 复合类型，10小时 |
| 30 | **XML type** | 🔴 高 | ⭐ | XML数据类型，10小时 |
| 31 | **EXPLAIN** | 🟡 中 | ⭐⭐ | 查询执行计划，8小时 |

**预计总工时**: ~58小时

---

## 已完全支持的功能 ✅

### Section 1: Querying Data
- [x] SELECT
- [x] Column aliases (AS)
- [x] ORDER BY
- [x] SELECT DISTINCT

### Section 2: Filtering Data
- [x] WHERE
- [x] AND operator
- [x] OR operator
- [x] LIMIT
- [x] IN (list)
- [x] IN (subquery)
- [x] BETWEEN
- [x] LIKE / ILIKE
- [x] IS NULL / IS NOT NULL

### Section 3: Joining Tables
- [x] Table aliases
- [x] INNER JOIN
- [x] LEFT JOIN
- [x] RIGHT JOIN (新增)
- [x] FULL OUTER JOIN (新增)
- [x] Self-join

### Section 4: Grouping Data
- [x] GROUP BY
- [x] HAVING

### Section 5: Set Operations
- [x] UNION / UNION ALL
- [x] INTERSECT
- [x] EXCEPT

### Section 7: Subquery
- [x] Basic Subquery
- [x] EXISTS / NOT EXISTS
- [x] Scalar subquery

### Section 8: Common Table Expressions
- [x] Basic CTE (WITH ... AS)

### Section 9: Modifying Data
- [x] INSERT (single row)
- [x] INSERT (multiple rows)
- [x] UPDATE
- [x] DELETE
- [x] UPSERT (ON CONFLICT)

### Section 10: Transactions
- [x] BEGIN / COMMIT / ROLLBACK
- [x] SELECT FOR UPDATE

### Section 11: Import & Export
- [x] COPY FROM stdin

### Section 12: Managing Tables
- [x] CREATE TABLE
- [x] SERIAL / BIGSERIAL
- [x] ALTER TABLE ADD COLUMN
- [x] ALTER TABLE DROP COLUMN
- [x] ALTER TABLE RENAME COLUMN
- [x] DROP TABLE
- [x] TRUNCATE TABLE

### Section 13: Constraints
- [x] PRIMARY KEY
- [x] UNIQUE (parsed)
- [x] NOT NULL
- [x] DEFAULT

### Section 14: Data Types
- [x] BOOLEAN
- [x] CHAR, VARCHAR, TEXT
- [x] INTEGER, BIGINT
- [x] REAL, DOUBLE PRECISION
- [x] TIMESTAMP / TIMESTAMPTZ
- [x] INTERVAL
- [x] UUID
- [x] BYTEA
- [x] JSON / JSONB

### Section 15: Conditional Expressions
- [x] CASE WHEN
- [x] COALESCE
- [x] NULLIF
- [x] CAST

---

## 测试用例来源

每个功能的测试用例将基于 Neon 教程中的示例：

### Tier 1 测试示例

```sql
-- FETCH (SQL标准分页)
SELECT * FROM film ORDER BY title FETCH FIRST 5 ROWS ONLY;
SELECT * FROM film ORDER BY title OFFSET 5 ROWS FETCH FIRST 5 ROWS ONLY;

-- NATURAL JOIN
SELECT * FROM products NATURAL JOIN categories;

-- CROSS JOIN
SELECT * FROM colors CROSS JOIN sizes;

-- UPDATE FROM (UPDATE JOIN)
UPDATE product p SET price = p.price * 1.1
FROM product_segment ps WHERE p.segment_id = ps.id AND ps.discount > 0;

-- CREATE TABLE AS
CREATE TABLE action_film AS SELECT * FROM film WHERE category = 'Action';

-- SELECT INTO
SELECT * INTO film_backup FROM film WHERE rating = 'R';

-- TEMPORARY TABLE
CREATE TEMP TABLE temp_results (id INT, name TEXT);
```

### Tier 2 测试示例

```sql
-- ANY operator
SELECT * FROM employees WHERE salary = ANY (SELECT salary FROM managers);
SELECT * FROM employees WHERE salary > ANY (SELECT salary FROM managers);

-- ALL operator
SELECT * FROM employees WHERE salary > ALL (SELECT salary FROM managers);

-- Correlated Subquery
SELECT * FROM employees e 
WHERE salary > (SELECT AVG(salary) FROM employees WHERE department_id = e.department_id);

-- NUMERIC
CREATE TABLE products (price NUMERIC(10,2));
INSERT INTO products VALUES (19.99), (1234.5678);

-- DATE type
CREATE TABLE events (event_date DATE);
INSERT INTO events VALUES ('2024-01-15');
SELECT event_date + INTERVAL '1 day' FROM events;

-- Sequence
CREATE SEQUENCE order_seq START 1000;
SELECT NEXTVAL('order_seq');
INSERT INTO orders (id, name) VALUES (NEXTVAL('order_seq'), 'Test');
```

### Tier 3 测试示例

```sql
-- GROUPING SETS
SELECT brand, segment, SUM(quantity) FROM sales
GROUP BY GROUPING SETS ((brand, segment), (brand), (segment), ());

-- CUBE
SELECT brand, segment, SUM(quantity) FROM sales
GROUP BY CUBE (brand, segment);

-- ROLLUP
SELECT brand, segment, SUM(quantity) FROM sales
GROUP BY ROLLUP (brand, segment);

-- IDENTITY column
CREATE TABLE colors (
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(100)
);

-- RENAME TABLE
ALTER TABLE old_name RENAME TO new_name;

-- CHECK constraint
CREATE TABLE products (
    price NUMERIC CHECK (price > 0),
    discount NUMERIC CHECK (discount >= 0 AND discount <= 100)
);
```

### Tier 4 测试示例

```sql
-- WITH RECURSIVE
WITH RECURSIVE subordinates AS (
    SELECT employee_id, manager_id, full_name FROM employees WHERE employee_id = 2
    UNION
    SELECT e.employee_id, e.manager_id, e.full_name 
    FROM employees e INNER JOIN subordinates s ON s.employee_id = e.manager_id
)
SELECT * FROM subordinates;

-- Foreign Key enforcement
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id)
);
INSERT INTO orders (customer_id) VALUES (999); -- should fail

-- DELETE CASCADE
CREATE TABLE orders (
    customer_id INT REFERENCES customers(id) ON DELETE CASCADE
);
DELETE FROM customers WHERE id = 1; -- should cascade to orders
```

---

## 推荐实现顺序

### Phase 1: Quick Wins (1-2周)
1. FETCH FIRST N ROWS
2. NATURAL JOIN
3. CROSS JOIN (完善)
4. UPDATE ... FROM
5. CREATE TABLE AS
6. SELECT INTO

### Phase 2: Core Features (2-3周)
7. TEMP TABLE
8. ANY/SOME operator
9. ALL operator
10. NUMERIC type
11. DATE type
12. TIME type

### Phase 3: Advanced (3-4周)
13. Correlated Subquery
14. Sequence
15. GROUPING SETS / CUBE / ROLLUP
16. IDENTITY column
17. CHECK constraint enforcement

### Phase 4: Complex (按需)
18. WITH RECURSIVE
19. Foreign Key enforcement
20. DELETE CASCADE
21. ENUM type

---

## 估算总工时

| Phase | 功能数 | 预计工时 |
|-------|--------|----------|
| Phase 1 | 6 | ~15小时 |
| Phase 2 | 6 | ~26小时 |
| Phase 3 | 5 | ~24小时 |
| Phase 4 | 4 | ~32小时 |
| **总计** | **21** | **~97小时** |

---

## 备注

1. **难度评估标准**:
   - 🟢 低: 纯解析层面修改或简单映射，<4小时
   - 🟡 中: 需要修改执行逻辑，4-8小时
   - 🔴 高: 需要新的核心机制，>8小时

2. **收益评估标准**:
   - ⭐⭐⭐: 高频使用，兼容性关键
   - ⭐⭐: 中等使用频率
   - ⭐: 低频或特殊场景

3. **依赖关系**:
   - CUBE/ROLLUP 依赖 GROUPING SETS
   - DELETE CASCADE 依赖 Foreign Key enforcement
   - SELECT INTO 可复用 CREATE TABLE AS 逻辑
