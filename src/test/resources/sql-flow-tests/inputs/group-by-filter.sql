-- Test filter clause for aggregate expression.

-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
  (1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

CREATE OR REPLACE TEMPORARY VIEW EMP AS SELECT * FROM VALUES
  (100, "emp 1", date "2005-01-01", 100.00D, 10),
  (100, "emp 1", date "2005-01-01", 100.00D, 10),
  (200, "emp 2", date "2003-01-01", 200.00D, 10),
  (300, "emp 3", date "2002-01-01", 300.00D, 20),
  (400, "emp 4", date "2005-01-01", 400.00D, 30),
  (500, "emp 5", date "2001-01-01", 400.00D, NULL),
  (600, "emp 6 - no dept", date "2001-01-01", 400.00D, 100),
  (700, "emp 7", date "2010-01-01", 400.00D, 100),
  (800, "emp 8", date "2016-01-01", 150.00D, 70)
AS EMP(id, emp_name, hiredate, salary, dept_id);

CREATE OR REPLACE TEMPORARY VIEW DEPT AS SELECT * FROM VALUES
  (10, "dept 1", "CA"),
  (20, "dept 2", "NY"),
  (30, "dept 3", "TX"),
  (40, "dept 4 - unassigned", "OR"),
  (50, "dept 5 - unassigned", "NJ"),
  (70, "dept 7", "FL")
AS DEPT(dept_id, dept_name, state);

-- Aggregate with filter and empty GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t1 AS
  SELECT FIRST(a), COUNT(b) FILTER (WHERE a >= 2) FROM testData;
CREATE OR REPLACE TEMPORARY VIEW t2 AS
  SELECT COUNT(a) FILTER (WHERE a = 1), COUNT(b) FILTER (WHERE a > 1) FROM testData;
CREATE OR REPLACE TEMPORARY VIEW t3 AS
  SELECT COUNT(id) FILTER (WHERE hiredate = date "2001-01-01") FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t4 AS
  SELECT COUNT(id) FILTER (WHERE hiredate = to_timestamp("2001-01-01 00:00:00")) FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t5 AS
  SELECT COUNT(DISTINCT id) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") = "2001-01-01 00:00:00") FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t6 AS
  SELECT COUNT(DISTINCT id), COUNT(DISTINCT id) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") = "2001-01-01 00:00:00") FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t7 AS
  SELECT COUNT(DISTINCT id) FILTER (WHERE hiredate = to_timestamp("2001-01-01 00:00:00")), COUNT(DISTINCT id) FILTER (WHERE hiredate = to_date('2001-01-01 00:00:00')) FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t8 AS
  SELECT SUM(salary), COUNT(DISTINCT id), COUNT(DISTINCT id) FILTER (WHERE hiredate = date "2001-01-01") FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t9 AS
  SELECT COUNT(DISTINCT 1) FILTER (WHERE a = 1) FROM testData;
CREATE OR REPLACE TEMPORARY VIEW t10 AS
  SELECT COUNT(DISTINCT id) FILTER (WHERE true) FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t11 AS
  SELECT COUNT(DISTINCT id) FILTER (WHERE false) FROM emp;
CREATE OR REPLACE TEMPORARY VIEW t12 AS
  SELECT COUNT(DISTINCT 2), COUNT(DISTINCT 2,3) FILTER (WHERE dept_id = 40) FROM emp;

-- Aggregate with filter and non-empty GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t13 AS
  SELECT a, COUNT(b) FILTER (WHERE a >= 2) FROM testData GROUP BY a;
CREATE OR REPLACE TEMPORARY VIEW t14 AS
  SELECT COUNT(a) FILTER (WHERE a >= 0), COUNT(b) FILTER (WHERE a >= 3) FROM testData GROUP BY a;
CREATE OR REPLACE TEMPORARY VIEW t15 AS
  SELECT dept_id, SUM(salary) FILTER (WHERE hiredate > to_timestamp("2003-01-01 00:00:00")) FROM emp GROUP BY dept_id;
CREATE OR REPLACE TEMPORARY VIEW t16 AS
  SELECT dept_id, SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") > "2001-01-01 00:00:00") FROM emp GROUP BY dept_id;
CREATE OR REPLACE TEMPORARY VIEW t17 AS
  SELECT dept_id, SUM(DISTINCT salary), SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") > "2001-01-01 00:00:00") FROM emp GROUP BY dept_id;
CREATE OR REPLACE TEMPORARY VIEW t18 AS
  SELECT dept_id, SUM(DISTINCT salary) FILTER (WHERE hiredate > date "2001-01-01"), SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd HH:mm:ss") > "2001-01-01 00:00:00") FROM emp GROUP BY dept_id;
CREATE OR REPLACE TEMPORARY VIEW t19 AS
  SELECT dept_id, COUNT(id), SUM(DISTINCT salary), SUM(DISTINCT salary) FILTER (WHERE date_format(hiredate, "yyyy-MM-dd") > "2001-01-01") FROM emp GROUP BY dept_id;
CREATE OR REPLACE TEMPORARY VIEW t20 AS
  SELECT b, COUNT(DISTINCT 1) FILTER (WHERE a = 1) FROM testData GROUP BY b;

-- Aggregate with filter and grouped by literals.
CREATE OR REPLACE TEMPORARY VIEW t21 AS
  SELECT 'foo', COUNT(a) FILTER (WHERE b <= 2) FROM testData GROUP BY 1;
CREATE OR REPLACE TEMPORARY VIEW t22 AS
  SELECT 'foo', SUM(salary) FILTER (WHERE hiredate >= to_timestamp("2003-01-01")) FROM emp GROUP BY 1;

-- Aggregate with filter, more than one aggregate function goes with distinct.
CREATE OR REPLACE TEMPORARY VIEW t22 AS
  select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary), sum(salary) filter (where id > 200) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t23 AS
  select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary), sum(salary) filter (where id + dept_id > 500) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t24 AS
  select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary) filter (where salary < 400.00D), sum(salary) filter (where id > 200) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t25 AS
  select dept_id, count(distinct emp_name), count(distinct hiredate), sum(salary) filter (where salary < 400.00D), sum(salary) filter (where id + dept_id > 500) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t26 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t27 AS
  select dept_id, count(distinct emp_name) filter (where id + dept_id > 500), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t28 AS
  select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id > 200), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t29 AS
  select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id + dept_id > 500), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t30 AS
  select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id > 200), sum(salary), sum(salary) filter (where id > 200) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t31 AS
  select dept_id, count(distinct emp_name), count(distinct emp_name) filter (where id + dept_id > 500), sum(salary), sum(salary) filter (where id > 200) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t32 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t33 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t34 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) filter (where salary < 400.00D) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t35 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) filter (where salary < 400.00D), sum(salary) filter (where id > 200) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t36 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct emp_name), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t37 AS
  select dept_id, count(distinct emp_name) filter (where id > 200), count(distinct emp_name) filter (where hiredate > date "2003-01-01"), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t38 AS
  select dept_id, sum(distinct (id + dept_id)) filter (where id > 200), count(distinct hiredate), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t39 AS
  select dept_id, sum(distinct (id + dept_id)) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t40 AS
  select dept_id, avg(distinct (id + dept_id)) filter (where id > 200), count(distinct hiredate) filter (where hiredate > date "2003-01-01"), sum(salary) filter (where salary < 400.00D) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t41 AS
  select dept_id, count(distinct emp_name, hiredate) filter (where id > 200), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t42 AS
  select dept_id, count(distinct emp_name, hiredate) filter (where id > 0), sum(salary) from emp group by dept_id;
CREATE OR REPLACE TEMPORARY VIEW t43 AS
  select dept_id, count(distinct 1), count(distinct 1) filter (where id > 200), sum(salary) from emp group by dept_id;

-- Aggregate with filter and grouped by literals (hash aggregate), here the input table is filtered using WHERE.
CREATE OR REPLACE TEMPORARY VIEW t44 AS
  SELECT 'foo', APPROX_COUNT_DISTINCT(a) FILTER (WHERE b >= 0) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and grouped by literals (sort aggregate), here the input table is filtered using WHERE.
CREATE OR REPLACE TEMPORARY VIEW t45 AS
  SELECT 'foo', MAX(STRUCT(a)) FILTER (WHERE b >= 1) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with filter and complex GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t46 AS
  SELECT a + b, COUNT(b) FILTER (WHERE b >= 2) FROM testData GROUP BY a + b;

-- Aggregate with filter, foldable input and multiple distinct groups.
CREATE OR REPLACE TEMPORARY VIEW t47 AS
  SELECT COUNT(DISTINCT b) FILTER (WHERE b > 0), COUNT(DISTINCT b, c) FILTER (WHERE b > 0 AND c > 2)
  FROM (SELECT 1 AS a, 2 AS b, 3 AS c) GROUP BY a;

-- Check analysis exceptions
CREATE OR REPLACE TEMPORARY VIEW t48 AS
  SELECT a AS k, COUNT(b) FILTER (WHERE b > 0) FROM testData GROUP BY k;

-- Aggregate with filter contains exists subquery
CREATE OR REPLACE TEMPORARY VIEW t49 AS
  SELECT emp.dept_id,
         avg(salary),
         avg(salary) FILTER (WHERE id > (SELECT 200))
  FROM emp
  GROUP BY dept_id;

CREATE OR REPLACE TEMPORARY VIEW t50 AS
  SELECT emp.dept_id,
         avg(salary),
         avg(salary) FILTER (WHERE emp.dept_id = (SELECT dept_id FROM dept LIMIT 1))
  FROM emp
  GROUP BY dept_id;

-- Aggregate with filter is subquery
CREATE OR REPLACE TEMPORARY VIEW t51 AS
  SELECT t1.b FROM (SELECT COUNT(b) FILTER (WHERE a >= 2) AS b FROM testData) t1;