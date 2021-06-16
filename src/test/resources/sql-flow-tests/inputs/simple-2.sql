CREATE OR REPLACE TEMPORARY VIEW v1 AS
  SELECT key, SUM(value) s FROM testdata GROUP BY key HAVING s > 100;

CACHE TABLE v1;

CREATE TEMPORARY VIEW v2 AS
  SELECT testdata.*, v1.s FROM testdata
  LEFT OUTER JOIN v1 ON testdata.key = v1.key;
