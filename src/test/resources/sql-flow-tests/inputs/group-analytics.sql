CREATE OR REPLACE TEMPORARY VIEW testData AS SELECT * FROM VALUES
  (1, 1), (1, 2), (2, 1), (2, 2), (3, 1), (3, 2)
AS testData(a, b);

-- CUBE on overlapping columns
CREATE OR REPLACE TEMPORARY VIEW t1 AS
  SELECT a + b, b, SUM(a - b) FROM testData GROUP BY a + b, b WITH CUBE;

CREATE OR REPLACE TEMPORARY VIEW t2 AS
  SELECT a, b, SUM(b) FROM testData GROUP BY a, b WITH CUBE;

-- ROLLUP on overlapping columns
CREATE OR REPLACE TEMPORARY VIEW t3 AS
  SELECT a + b, b, SUM(a - b) FROM testData GROUP BY a + b, b WITH ROLLUP;

CREATE OR REPLACE TEMPORARY VIEW t4 AS
  SELECT a, b, SUM(b) FROM testData GROUP BY a, b WITH ROLLUP;

CREATE OR REPLACE TEMPORARY VIEW courseSales AS SELECT * FROM VALUES
  ("dotNET", 2012, 10000), ("Java", 2012, 20000), ("dotNET", 2012, 5000), ("dotNET", 2013, 48000), ("Java", 2013, 30000)
AS courseSales(course, year, earnings);

-- ROLLUP
CREATE OR REPLACE TEMPORARY VIEW t5 AS
  SELECT course, year, SUM(earnings) FROM courseSales GROUP BY ROLLUP(course, year) ORDER BY course, year;

-- CUBE
CREATE OR REPLACE TEMPORARY VIEW t6 AS
  SELECT course, year, SUM(earnings) FROM courseSales GROUP BY CUBE(course, year) ORDER BY course, year;

-- GROUPING SETS
CREATE OR REPLACE TEMPORARY VIEW t7 AS
  SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course, year);
CREATE OR REPLACE TEMPORARY VIEW t8 AS
  SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(course);
CREATE OR REPLACE TEMPORARY VIEW t9 AS
  SELECT course, year, SUM(earnings) FROM courseSales GROUP BY course, year GROUPING SETS(year);

-- GROUPING SETS with aggregate functions containing groupBy columns
CREATE OR REPLACE TEMPORARY VIEW t10 AS
  SELECT course, SUM(earnings) AS sum FROM courseSales
  GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, sum;
CREATE OR REPLACE TEMPORARY VIEW t11 AS
  SELECT course, SUM(earnings) AS sum, GROUPING_ID(course, earnings) FROM courseSales
  GROUP BY course, earnings GROUPING SETS((), (course), (course, earnings)) ORDER BY course, sum;

-- GROUPING/GROUPING_ID
CREATE OR REPLACE TEMPORARY VIEW t12 AS
  SELECT course, year, GROUPING(course), GROUPING(year), GROUPING_ID(course, year) FROM courseSales
  GROUP BY CUBE(course, year);
CREATE OR REPLACE TEMPORARY VIEW t13 AS
SELECT course, year, grouping__id FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, year;

-- GROUPING/GROUPING_ID in having clause
CREATE OR REPLACE TEMPORARY VIEW t14 AS
  SELECT course, year FROM courseSales GROUP BY CUBE(course, year)
  HAVING GROUPING(year) = 1 AND GROUPING_ID(course, year) > 0 ORDER BY course, year;

-- GROUPING/GROUPING_ID in orderBy clause
CREATE OR REPLACE TEMPORARY VIEW t15 AS
  SELECT course, year, GROUPING(course), GROUPING(year) FROM courseSales GROUP BY CUBE(course, year)
  ORDER BY GROUPING(course), GROUPING(year), course, year;
CREATE OR REPLACE TEMPORARY VIEW t16 AS
  SELECT course, year, GROUPING_ID(course, year) FROM courseSales GROUP BY CUBE(course, year)
  ORDER BY GROUPING(course), GROUPING(year), course, year;
CREATE OR REPLACE TEMPORARY VIEW t17 AS
  SELECT course, year FROM courseSales GROUP BY CUBE(course, year) ORDER BY grouping__id, course, year;

-- Aliases in SELECT could be used in ROLLUP/CUBE/GROUPING SETS
CREATE OR REPLACE TEMPORARY VIEW t18 AS
  SELECT a + b AS k1, b AS k2, SUM(a - b) FROM testData GROUP BY CUBE(k1, k2);
CREATE OR REPLACE TEMPORARY VIEW t19 AS
  SELECT a + b AS k, b, SUM(a - b) FROM testData GROUP BY ROLLUP(k, b);
CREATE OR REPLACE TEMPORARY VIEW t20 AS
  SELECT a + b, b AS k, SUM(a - b) FROM testData GROUP BY a + b, k GROUPING SETS(k)
