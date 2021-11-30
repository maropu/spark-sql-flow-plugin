-- Test data.
CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
  (1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2), (null, 1), (3, null), (null, null)
AS testData(a, b);

-- Aggregate with empty GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t1 AS
  SELECT COUNT(a), COUNT(b) FROM testData;

-- Aggregate with non-empty GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t2 AS
  SELECT a, COUNT(b) FROM testData GROUP BY a;
CREATE OR REPLACE TEMPORARY VIEW t3 AS
  SELECT COUNT(a), COUNT(b) FROM testData GROUP BY a;

-- Aggregate grouped by literals.
CREATE OR REPLACE TEMPORARY VIEW t4 AS
  SELECT 'foo', COUNT(a) FROM testData GROUP BY 1;

-- Aggregate grouped by literals (whole stage code generation).
CREATE OR REPLACE TEMPORARY VIEW t5 AS
  SELECT 'foo' FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate grouped by literals (hash aggregate).
CREATE OR REPLACE TEMPORARY VIEW t6 AS
  SELECT 'foo', APPROX_COUNT_DISTINCT(a) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate grouped by literals (sort aggregate).
CREATE OR REPLACE TEMPORARY VIEW t7 AS
  SELECT 'foo', MAX(STRUCT(a)) FROM testData WHERE a = 0 GROUP BY 1;

-- Aggregate with complex GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t8 AS
  SELECT a + b, COUNT(b) FROM testData GROUP BY a + b;

-- Aggregate with nulls.
CREATE OR REPLACE TEMPORARY VIEW t9 AS
  SELECT SKEWNESS(a), KURTOSIS(a), MIN(a), MAX(a), AVG(a), VARIANCE(a), STDDEV(a), SUM(a), COUNT(a)
  FROM testData;

-- Aggregate with foldable input and multiple distinct groups.
CREATE OR REPLACE TEMPORARY VIEW t10 AS
  SELECT COUNT(DISTINCT b), COUNT(DISTINCT b, c) FROM (SELECT 1 AS a, 2 AS b, 3 AS c) t GROUP BY a;

-- Aliases in SELECT could be used in GROUP BY
CREATE OR REPLACE TEMPORARY VIEW t11 AS
  SELECT a AS k, COUNT(b) FROM testData GROUP BY k;
CREATE OR REPLACE TEMPORARY VIEW t12 AS
  SELECT a AS k, COUNT(b) FROM testData GROUP BY k HAVING k > 1;

-- Aggregate with empty input and non-empty GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t13 AS
  SELECT a, COUNT(1) FROM testData WHERE false GROUP BY a;

-- Aggregate with empty input and empty GroupBy expressions.
CREATE OR REPLACE TEMPORARY VIEW t14 AS
  SELECT COUNT(1) FROM testData WHERE false;
CREATE OR REPLACE TEMPORARY VIEW t15 AS
  SELECT 1 FROM (SELECT COUNT(1) FROM testData WHERE false) t;

-- Aggregate with empty GroupBy expressions and filter on top
CREATE OR REPLACE TEMPORARY VIEW t16 AS
  SELECT 1 from (
                    SELECT 1 AS z,
                           MIN(a.x)
                    FROM (select 1 as x) a
                    WHERE false
                ) b
  where b.z != b.z;

-- SPARK-24369 multiple distinct aggregations having the same argument set
CREATE OR REPLACE TEMPORARY VIEW t17 AS
  SELECT corr(DISTINCT x, y), corr(DISTINCT y, x), count(*)
  FROM (VALUES (1, 1), (2, 2), (2, 2)) t(x, y);

-- Test data
CREATE OR REPLACE TEMPORARY VIEW test_agg AS SELECT * FROM VALUES
    (1, true), (1, false),
               (2, true),
               (3, false), (3, null),
               (4, null), (4, null),
               (5, null), (5, true), (5, false) AS test_agg(k, v);

-- having
CREATE OR REPLACE TEMPORARY VIEW t18 AS
  SELECT k, every(v) FROM test_agg GROUP BY k HAVING every(v) = false;
CREATE OR REPLACE TEMPORARY VIEW t19 AS
  SELECT k, every(v) FROM test_agg GROUP BY k HAVING every(v) IS NULL;

-- basic subquery path to make sure rewrite happens in both parent and child plans.
CREATE OR REPLACE TEMPORARY VIEW t20 AS
  SELECT k,
         Every(v) AS every
  FROM   test_agg
  WHERE  k = 2
    AND v IN (SELECT Any(v)
  FROM   test_agg
  WHERE  k = 1)
  GROUP  BY k;

-- basic subquery path to make sure rewrite happens in both parent and child plans.
CREATE OR REPLACE TEMPORARY VIEW t21 AS
  SELECT k,
         Every(v) AS every
  FROM   test_agg
  WHERE  k = 2
    AND v IN (SELECT Every(v)
              FROM   test_agg
              WHERE  k = 1)
  GROUP  BY k;
