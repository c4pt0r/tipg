import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';

describe('TypeORM Advanced SQL Features [pg-tikv]', () => {
  let dataSource: DataSource;

  beforeAll(async () => {
    dataSource = createDataSource({ synchronize: false });
    await dataSource.initialize();

    await dataSource.query(`DROP TABLE IF EXISTS adv_sales CASCADE`);
    await dataSource.query(`DROP TABLE IF EXISTS adv_employees CASCADE`);
    await dataSource.query(`DROP TABLE IF EXISTS adv_departments CASCADE`);
    await dataSource.query(`DROP VIEW IF EXISTS adv_dept_summary`);

    await dataSource.query(`
      CREATE TABLE adv_departments (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        budget DECIMAL(15,2) DEFAULT 0
      )
    `);

    await dataSource.query(`
      CREATE TABLE adv_employees (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        email VARCHAR(255),
        department_id INTEGER REFERENCES adv_departments(id),
        salary DECIMAL(10,2) NOT NULL,
        hire_date TIMESTAMPTZ DEFAULT NOW(),
        metadata JSONB,
        manager_id INTEGER REFERENCES adv_employees(id)
      )
    `);

    await dataSource.query(`
      CREATE TABLE adv_sales (
        id SERIAL PRIMARY KEY,
        employee_id INTEGER REFERENCES adv_employees(id),
        amount DECIMAL(10,2) NOT NULL,
        sale_date TIMESTAMPTZ DEFAULT NOW(),
        region VARCHAR(50)
      )
    `);
  });

  afterAll(async () => {
    if (dataSource?.isInitialized) {
      await dataSource.query(`DROP VIEW IF EXISTS adv_dept_summary`);
      await dataSource.query(`DROP TABLE IF EXISTS adv_sales CASCADE`);
      await dataSource.query(`DROP TABLE IF EXISTS adv_employees CASCADE`);
      await dataSource.query(`DROP TABLE IF EXISTS adv_departments CASCADE`);
      await dataSource.destroy();
    }
  });

  beforeEach(async () => {
    await dataSource.query(`DELETE FROM adv_sales`);
    await dataSource.query(`DELETE FROM adv_employees`);
    await dataSource.query(`DELETE FROM adv_departments`);

    await dataSource.query(`
      INSERT INTO adv_departments (id, name, budget) VALUES
      (1, 'Engineering', 500000),
      (2, 'Sales', 300000),
      (3, 'Marketing', 200000)
    `);

    await dataSource.query(`
      INSERT INTO adv_employees (id, name, email, department_id, salary, metadata, manager_id) VALUES
      (1, 'Alice', 'alice@test.com', 1, 80000, '{"level": "senior", "skills": ["rust", "python"]}', NULL),
      (2, 'Bob', 'bob@test.com', 1, 70000, '{"level": "mid", "skills": ["javascript"]}', 1),
      (3, 'Charlie', 'charlie@test.com', 2, 60000, '{"level": "junior", "skills": ["sales"]}', NULL),
      (4, 'Diana', 'diana@test.com', 2, 75000, '{"level": "senior", "skills": ["sales", "marketing"]}', 3),
      (5, 'Eve', 'eve@test.com', 3, 55000, '{"level": "mid", "skills": ["design"]}', NULL)
    `);

    await dataSource.query(`
      INSERT INTO adv_sales (employee_id, amount, sale_date, region) VALUES
      (3, 1000, '2024-01-15', 'North'),
      (3, 1500, '2024-01-20', 'South'),
      (4, 2000, '2024-01-18', 'North'),
      (4, 2500, '2024-02-10', 'East'),
      (4, 1800, '2024-02-15', 'North')
    `);
  });

  describe('window functions', () => {
    it('should support ROW_NUMBER()', async () => {
      const result = await dataSource.query(`
        SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rank
        FROM adv_employees
        ORDER BY rank
      `);

      expect(result).toHaveLength(5);
      expect(result[0].name).toBe('Alice');
      expect(Number(result[0].rank)).toBe(1);
      expect(result[4].name).toBe('Eve');
      expect(Number(result[4].rank)).toBe(5);
    });

    it('should support RANK() with ties', async () => {
      await dataSource.query(`UPDATE adv_employees SET salary = 70000 WHERE name = 'Charlie'`);

      const result = await dataSource.query(`
        SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as rank
        FROM adv_employees
        ORDER BY rank, name
      `);

      expect(result).toHaveLength(5);
      const bobRank = result.find((r: { name: string }) => r.name === 'Bob');
      const charlieRank = result.find((r: { name: string }) => r.name === 'Charlie');
      expect(Number(bobRank.rank)).toBe(Number(charlieRank.rank));
    });

    it('should support DENSE_RANK()', async () => {
      const result = await dataSource.query(`
        SELECT name, department_id, salary,
               DENSE_RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as dept_rank
        FROM adv_employees
        ORDER BY department_id, dept_rank
      `);

      expect(result).toHaveLength(5);
      const engEmployees = result.filter((r: { department_id: number }) => r.department_id === 1);
      expect(engEmployees).toHaveLength(2);
      expect(Number(engEmployees[0].dept_rank)).toBe(1);
    });

    it('should support LEAD() and LAG()', async () => {
      const result = await dataSource.query(`
        SELECT name, salary,
               LAG(salary) OVER (ORDER BY salary) as prev_salary,
               LEAD(salary) OVER (ORDER BY salary) as next_salary
        FROM adv_employees
        ORDER BY salary
      `);

      expect(result).toHaveLength(5);
      expect(result[0].prev_salary).toBeNull();
      expect(result[4].next_salary).toBeNull();
      expect(Number(result[1].prev_salary)).toBe(Number(result[0].salary));
    });

    it('should support SUM() OVER with PARTITION BY', async () => {
      const result = await dataSource.query(`
        SELECT name, department_id, salary,
               SUM(salary) OVER (PARTITION BY department_id) as dept_total
        FROM adv_employees
        ORDER BY department_id, name
      `);

      expect(result).toHaveLength(5);
      const engEmployees = result.filter((r: { department_id: number }) => r.department_id === 1);
      expect(Number(engEmployees[0].dept_total)).toBe(150000);
      expect(Number(engEmployees[1].dept_total)).toBe(150000);
    });

    it('should support running SUM() OVER with ORDER BY', async () => {
      const result = await dataSource.query(`
        SELECT name, salary,
               SUM(salary) OVER (ORDER BY salary) as running_total
        FROM adv_employees
        ORDER BY salary
      `);

      expect(result).toHaveLength(5);
      expect(Number(result[0].running_total)).toBe(Number(result[0].salary));
      const totalSalary = result.reduce(
        (sum: number, r: { salary: string }) => sum + Number(r.salary),
        0
      );
      expect(Number(result[4].running_total)).toBe(totalSalary);
    });

    it('should support AVG() OVER', async () => {
      const result = await dataSource.query(`
        SELECT name, department_id, salary,
               AVG(salary) OVER (PARTITION BY department_id) as dept_avg
        FROM adv_employees
        WHERE department_id = 1
      `);

      expect(result).toHaveLength(2);
      expect(Number(result[0].dept_avg)).toBe(75000);
    });

    it('should support COUNT() OVER', async () => {
      const result = await dataSource.query(`
        SELECT name, department_id,
               COUNT(*) OVER (PARTITION BY department_id) as dept_count
        FROM adv_employees
        ORDER BY department_id
      `);

      const engCount = result.filter((r: { department_id: number }) => r.department_id === 1);
      expect(Number(engCount[0].dept_count)).toBe(2);
    });
  });

  describe('CTE (WITH clause)', () => {
    it('should support simple CTE', async () => {
      const result = await dataSource.query(`
        WITH high_earners AS (
          SELECT * FROM adv_employees WHERE salary > 65000
        )
        SELECT name, salary FROM high_earners ORDER BY salary DESC
      `);

      expect(result).toHaveLength(3);
      expect(result[0].name).toBe('Alice');
    });

    it('should support multiple CTEs', async () => {
      const result = await dataSource.query(`
        WITH dept_totals AS (
          SELECT department_id, SUM(salary) as total
          FROM adv_employees
          GROUP BY department_id
        ),
        high_budget_depts AS (
          SELECT d.name, dt.total
          FROM adv_departments d
          JOIN dept_totals dt ON d.id = dt.department_id
          WHERE dt.total > 100000
        )
        SELECT * FROM high_budget_depts ORDER BY total DESC
      `);

      expect(result).toHaveLength(2);
    });

    it('should support CTE with aggregation', async () => {
      const result = await dataSource.query(`
        WITH sales_by_employee AS (
          SELECT employee_id, SUM(amount) as total_sales
          FROM adv_sales
          GROUP BY employee_id
        )
        SELECT e.name, COALESCE(s.total_sales, 0) as total_sales
        FROM adv_employees e
        LEFT JOIN sales_by_employee s ON e.id = s.employee_id
        ORDER BY total_sales DESC
      `);

      expect(result).toHaveLength(5);
      expect(result[0].name).toBe('Diana');
    });
  });

  describe('recursive CTE', () => {
    it('should support recursive CTE for hierarchy', async () => {
      const result = await dataSource.query(`
        WITH RECURSIVE emp_hierarchy AS (
          SELECT id, name, manager_id, 1 as level
          FROM adv_employees
          WHERE manager_id IS NULL
          UNION ALL
          SELECT e.id, e.name, e.manager_id, eh.level + 1
          FROM adv_employees e
          JOIN emp_hierarchy eh ON e.manager_id = eh.id
        )
        SELECT * FROM emp_hierarchy ORDER BY level, name
      `);

      expect(result.length).toBeGreaterThanOrEqual(5);
      const level1 = result.filter((r: { level: number }) => r.level === 1);
      expect(level1.length).toBeGreaterThanOrEqual(3);
    });

    it('should support recursive CTE with depth limit', async () => {
      const result = await dataSource.query(`
        WITH RECURSIVE numbers AS (
          SELECT 1 as n
          UNION ALL
          SELECT n + 1 FROM numbers WHERE n < 5
        )
        SELECT * FROM numbers
      `);

      expect(result).toHaveLength(5);
      expect(result.map((r: { n: number }) => r.n)).toEqual([1, 2, 3, 4, 5]);
    });
  });

  describe('subqueries', () => {
    it('should support subquery in WHERE with IN', async () => {
      const result = await dataSource.query(`
        SELECT name, salary
        FROM adv_employees
        WHERE department_id IN (
          SELECT id FROM adv_departments WHERE budget > 250000
        )
        ORDER BY salary DESC
      `);

      expect(result).toHaveLength(4);
    });

    it('should support subquery with EXISTS', async () => {
      const result = await dataSource.query(`
        SELECT name
        FROM adv_employees e
        WHERE EXISTS (
          SELECT 1 FROM adv_sales s WHERE s.employee_id = e.id
        )
        ORDER BY name
      `);

      expect(result).toHaveLength(2);
      expect(result.map((r: { name: string }) => r.name)).toContain('Charlie');
      expect(result.map((r: { name: string }) => r.name)).toContain('Diana');
    });

    it('should support scalar subquery in SELECT', async () => {
      const result = await dataSource.query(`
        SELECT name,
               (SELECT COUNT(*) FROM adv_sales WHERE employee_id = e.id) as sale_count
        FROM adv_employees e
        ORDER BY sale_count DESC, name
      `);

      expect(result).toHaveLength(5);
      expect(Number(result[0].sale_count)).toBe(3);
    });

    it('should support subquery in FROM (derived table)', async () => {
      const result = await dataSource.query(`
        SELECT dept_name, employee_count
        FROM (
          SELECT d.name as dept_name, COUNT(e.id) as employee_count
          FROM adv_departments d
          LEFT JOIN adv_employees e ON d.id = e.department_id
          GROUP BY d.name
        ) as dept_stats
        ORDER BY employee_count DESC
      `);

      expect(result).toHaveLength(3);
    });

    it('should support NOT EXISTS', async () => {
      const result = await dataSource.query(`
        SELECT name
        FROM adv_employees e
        WHERE NOT EXISTS (
          SELECT 1 FROM adv_sales s WHERE s.employee_id = e.id
        )
        ORDER BY name
      `);

      expect(result).toHaveLength(3);
    });
  });

  describe('views', () => {
    it('should create and query a view', async () => {
      await dataSource.query(`DROP VIEW IF EXISTS adv_dept_summary`);
      await dataSource.query(`
        CREATE VIEW adv_dept_summary AS
        SELECT d.name as dept_name,
               COUNT(e.id) as employee_count,
               COALESCE(SUM(e.salary), 0) as total_salary
        FROM adv_departments d
        LEFT JOIN adv_employees e ON d.id = e.department_id
        GROUP BY d.name
      `);

      const result = await dataSource.query(`
        SELECT * FROM adv_dept_summary ORDER BY total_salary DESC
      `);

      expect(result).toHaveLength(3);
      expect(result[0].dept_name).toBe('Engineering');
    });

    it('should filter on view', async () => {
      const result = await dataSource.query(`
        SELECT * FROM adv_dept_summary WHERE employee_count > 1
      `);

      expect(result).toHaveLength(2);
    });
  });

  describe('JSONB operations', () => {
    it('should extract JSONB field with ->', async () => {
      const result = await dataSource.query(`
        SELECT name, metadata->'level' as level
        FROM adv_employees
        WHERE metadata IS NOT NULL
        ORDER BY name
      `);

      expect(result).toHaveLength(5);
      expect(result[0].level).toBe('senior');
    });

    it('should extract JSONB text with ->>', async () => {
      const result = await dataSource.query(`
        SELECT name, metadata->>'level' as level
        FROM adv_employees
        WHERE metadata->>'level' = 'senior'
        ORDER BY name
      `);

      expect(result).toHaveLength(2);
    });

    it('should query JSONB array', async () => {
      const result = await dataSource.query(`
        SELECT name, metadata->'skills' as skills
        FROM adv_employees
        WHERE metadata IS NOT NULL
        ORDER BY name
      `);

      expect(result).toHaveLength(5);
      expect(result[0].skills).toBeDefined();
    });

    it('should use JSONB containment @>', async () => {
      const result = await dataSource.query(`
        SELECT name
        FROM adv_employees
        WHERE metadata @> '{"level": "senior"}'
        ORDER BY name
      `);

      expect(result).toHaveLength(2);
    });
  });

  describe('string functions', () => {
    it('should support UPPER and LOWER', async () => {
      const result = await dataSource.query(`
        SELECT UPPER(name) as upper_name, LOWER(email) as lower_email
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(result[0].upper_name).toBe('ALICE');
      expect(result[0].lower_email).toBe('alice@test.com');
    });

    it('should support CONCAT', async () => {
      const result = await dataSource.query(`
        SELECT CONCAT(name, ' - ', email) as full_info
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(result[0].full_info).toBe('Alice - alice@test.com');
    });

    it('should support SUBSTRING', async () => {
      const result = await dataSource.query(`
        SELECT SUBSTRING(email FROM 1 FOR 5) as prefix
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(result[0].prefix).toBe('alice');
    });

    it('should support LENGTH', async () => {
      const result = await dataSource.query(`
        SELECT name, LENGTH(name) as name_len
        FROM adv_employees
        ORDER BY name_len DESC
        LIMIT 1
      `);

      expect(result[0].name).toBe('Charlie');
      expect(Number(result[0].name_len)).toBe(7);
    });

    it('should support TRIM, LTRIM, RTRIM', async () => {
      const result = await dataSource.query(`
        SELECT TRIM('  hello  ') as trimmed,
               LTRIM('  hello') as ltrimmed,
               RTRIM('hello  ') as rtrimmed
      `);

      expect(result[0].trimmed).toBe('hello');
      expect(result[0].ltrimmed).toBe('hello');
      expect(result[0].rtrimmed).toBe('hello');
    });

    it('should support REPLACE', async () => {
      const result = await dataSource.query(`
        SELECT REPLACE(email, '@test.com', '@company.com') as new_email
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(result[0].new_email).toBe('alice@company.com');
    });

    it('should support SPLIT_PART', async () => {
      const result = await dataSource.query(`
        SELECT SPLIT_PART(email, '@', 1) as username
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(result[0].username).toBe('alice');
    });

    it('should support INITCAP', async () => {
      const result = await dataSource.query(`SELECT INITCAP('hello world') as result`);
      expect(result[0].result).toBe('Hello World');
    });

    it('should support POSITION', async () => {
      const result = await dataSource.query(`
        SELECT POSITION('@' IN email) as at_pos
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(Number(result[0].at_pos)).toBe(6);
    });
  });

  describe('date/time functions', () => {
    it('should support NOW() and CURRENT_TIMESTAMP', async () => {
      const result = await dataSource.query(`
        SELECT NOW() as now_time, CURRENT_TIMESTAMP as current_ts
      `);

      expect(result[0].now_time).toBeInstanceOf(Date);
      expect(result[0].current_ts).toBeInstanceOf(Date);
    });

    it('should support DATE_TRUNC', async () => {
      const result = await dataSource.query(`
        SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total
        FROM adv_sales
        GROUP BY DATE_TRUNC('month', sale_date)
        ORDER BY month
      `);

      expect(result).toHaveLength(2);
    });

    it('should support EXTRACT', async () => {
      const result = await dataSource.query(`
        SELECT EXTRACT(MONTH FROM sale_date) as month, COUNT(*) as count
        FROM adv_sales
        GROUP BY EXTRACT(MONTH FROM sale_date)
        ORDER BY month
      `);

      expect(result).toHaveLength(2);
    });

    it('should support interval arithmetic', async () => {
      const result = await dataSource.query(`
        SELECT name
        FROM adv_employees
        WHERE hire_date > NOW() - INTERVAL '1 year'
        ORDER BY name
      `);

      expect(result.length).toBeGreaterThanOrEqual(0);
    });
  });

  describe('aggregate with HAVING', () => {
    it('should filter groups with HAVING', async () => {
      const result = await dataSource.query(`
        SELECT department_id, COUNT(*) as emp_count, AVG(salary) as avg_salary
        FROM adv_employees
        GROUP BY department_id
        HAVING COUNT(*) > 1
        ORDER BY avg_salary DESC
      `);

      expect(result).toHaveLength(2);
    });

    it('should use aggregate function in HAVING', async () => {
      const result = await dataSource.query(`
        SELECT employee_id, SUM(amount) as total
        FROM adv_sales
        GROUP BY employee_id
        HAVING SUM(amount) > 3000
      `);

      expect(result).toHaveLength(1);
      expect(Number(result[0].total)).toBe(6300);
    });
  });

  describe('DISTINCT ON', () => {
    it('should support DISTINCT ON', async () => {
      const result = await dataSource.query(`
        SELECT DISTINCT ON (department_id) department_id, name, salary
        FROM adv_employees
        ORDER BY department_id, salary DESC
      `);

      expect(result).toHaveLength(3);
      const eng = result.find((r: { department_id: number }) => r.department_id === 1);
      expect(eng.name).toBe('Alice');
    });
  });

  describe('foreign key constraints', () => {
    it('should enforce foreign key on INSERT', async () => {
      await expect(
        dataSource.query(`
          INSERT INTO adv_employees (name, email, department_id, salary)
          VALUES ('Test', 'test@test.com', 999, 50000)
        `)
      ).rejects.toThrow();
    });

    it('should reject delete when referenced', async () => {
      await expect(
        dataSource.query(`DELETE FROM adv_employees WHERE id = 3`)
      ).rejects.toThrow();
    });
  });

  describe('CASE expressions', () => {
    it('should support simple CASE', async () => {
      const result = await dataSource.query(`
        SELECT name,
               CASE
                 WHEN salary > 70000 THEN 'high'
                 WHEN salary > 55000 THEN 'medium'
                 ELSE 'low'
               END as salary_level
        FROM adv_employees
        ORDER BY salary DESC
      `);

      expect(result[0].salary_level).toBe('high');
      expect(result[4].salary_level).toBe('low');
    });

    it('should support CASE in ORDER BY', async () => {
      const result = await dataSource.query(`
        SELECT name, department_id
        FROM adv_employees
        ORDER BY
          CASE department_id
            WHEN 1 THEN 3
            WHEN 2 THEN 1
            WHEN 3 THEN 2
          END
      `);

      expect(result[0].department_id).toBe(2);
    });
  });

  describe('COALESCE and NULLIF', () => {
    it('should support COALESCE', async () => {
      await dataSource.query(`UPDATE adv_employees SET email = NULL WHERE name = 'Alice'`);

      const result = await dataSource.query(`
        SELECT name, COALESCE(email, 'no-email@default.com') as email
        FROM adv_employees
        WHERE name = 'Alice'
      `);

      expect(result[0].email).toBe('no-email@default.com');
    });

    it('should support NULLIF', async () => {
      const result = await dataSource.query(`
        SELECT NULLIF(10, 10) as null_result, NULLIF(10, 5) as not_null_result
      `);

      expect(result[0].null_result).toBeNull();
      expect(Number(result[0].not_null_result)).toBe(10);
    });
  });

  describe('math functions', () => {
    it('should support ABS, CEIL, FLOOR, ROUND', async () => {
      const result = await dataSource.query(`
        SELECT ABS(-5) as abs_val,
               CEIL(4.2) as ceil_val,
               FLOOR(4.8) as floor_val,
               ROUND(4.567, 2) as round_val
      `);

      expect(Number(result[0].abs_val)).toBe(5);
      expect(Number(result[0].ceil_val)).toBe(5);
      expect(Number(result[0].floor_val)).toBe(4);
      expect(Number(result[0].round_val)).toBeCloseTo(4.57);
    });

    it('should support SQRT, POWER', async () => {
      const result = await dataSource.query(`
        SELECT SQRT(16) as sqrt_val, POWER(2, 10) as power_val
      `);

      expect(Number(result[0].sqrt_val)).toBe(4);
      expect(Number(result[0].power_val)).toBe(1024);
    });

    it('should support MOD', async () => {
      const result = await dataSource.query(`SELECT MOD(17, 5) as mod_val`);
      expect(Number(result[0].mod_val)).toBe(2);
    });
  });
});
