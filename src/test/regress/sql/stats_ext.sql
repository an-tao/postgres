-- Generic extended statistics support

-- Ensure stats are dropped sanely
CREATE TABLE ab1 (a INTEGER, b INTEGER, c INTEGER);
CREATE STATISTICS ab1_a_b_stats ON (a, b) FROM ab1;
DROP STATISTICS ab1_a_b_stats;

CREATE SCHEMA regress_schema_2;
CREATE STATISTICS regress_schema_2.ab1_a_b_stats ON (a, b) FROM ab1;

-- Let's also verify the pg_get_statisticsextdef output looks sane.
SELECT pg_get_statisticsextdef(oid) FROM pg_statistic_ext WHERE staname = 'ab1_a_b_stats';

DROP STATISTICS regress_schema_2.ab1_a_b_stats;

-- Ensure statistics are dropped when columns are
CREATE STATISTICS ab1_b_c_stats ON (b, c) FROM ab1;
CREATE STATISTICS ab1_a_b_c_stats ON (a, b, c) FROM ab1;
CREATE STATISTICS ab1_a_b_stats ON (a, b) FROM ab1;
ALTER TABLE ab1 DROP COLUMN a;
\d ab1
DROP TABLE ab1;

-- Ensure things work sanely with SET STATISTICS 0
CREATE TABLE ab1 (a INTEGER, b INTEGER);
ALTER TABLE ab1 ALTER a SET STATISTICS 0;
INSERT INTO ab1 SELECT a, a%23 FROM generate_series(1, 1000) a;
CREATE STATISTICS ab1_a_b_stats ON (a, b) FROM ab1;
ANALYZE ab1;
ALTER TABLE ab1 ALTER a SET STATISTICS -1;
ANALYZE ab1;
DROP TABLE ab1;


-- n-distinct tests
CREATE TABLE ndistinct (
    filler1 TEXT,
    filler2 NUMERIC,
    a INT,
    b INT,
    filler3 DATE,
    c INT,
    d INT
);

-- unknown column
CREATE STATISTICS s10 ON (unknown_column) FROM ndistinct;

-- single column
CREATE STATISTICS s10 ON (a) FROM ndistinct;

-- single column, duplicated
CREATE STATISTICS s10 ON (a,a) FROM ndistinct;

-- two columns, one duplicated
CREATE STATISTICS s10 ON (a, a, b) FROM ndistinct;

-- correct command
CREATE STATISTICS s10 ON (a, b, c) FROM ndistinct;

-- perfectly correlated groups
INSERT INTO ndistinct (a, b, c, filler1)
     SELECT i/100, i/100, i/100, cash_words(i::money)
       FROM generate_series(1,10000) s(i);

ANALYZE ndistinct;

SELECT staenabled, standistinct
  FROM pg_statistic_ext WHERE starelid = 'ndistinct'::regclass;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN (COSTS off)
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

TRUNCATE TABLE ndistinct;

-- partially correlated groups
INSERT INTO ndistinct (a, b, c)
     SELECT i/50, i/100, i/200 FROM generate_series(1,10000) s(i);

ANALYZE ndistinct;

SELECT staenabled, standistinct
  FROM pg_statistic_ext WHERE starelid = 'ndistinct'::regclass;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, b, c, d;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY b, c, d;

EXPLAIN
 SELECT COUNT(*) FROM ndistinct GROUP BY a, d;

DROP TABLE ndistinct;

-- functional dependencies tests
CREATE TABLE functional_dependencies (
    filler1 TEXT,
    filler2 NUMERIC,
    a INT,
    b TEXT,
    filler3 DATE,
    c INT,
    d TEXT
);

SET random_page_cost = 1.2;

CREATE INDEX fdeps_ab_idx ON functional_dependencies (a, b);
CREATE INDEX fdeps_abc_idx ON functional_dependencies (a, b, c);

-- random data (no functional dependencies)
INSERT INTO functional_dependencies (a, b, c, filler1)
     SELECT mod(i, 23), mod(i, 29), mod(i, 31), i FROM generate_series(1,5000) s(i);

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- create statistics
CREATE STATISTICS func_deps_stat WITH (dependencies) ON (a, b, c) FROM functional_dependencies;

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- a => b, a => c, b => c
TRUNCATE functional_dependencies;
DROP STATISTICS func_deps_stat;

INSERT INTO functional_dependencies (a, b, c, filler1)
     SELECT mod(i,100), mod(i,50), mod(i,25), i FROM generate_series(1,5000) s(i);

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

-- create statistics
CREATE STATISTICS func_deps_stat WITH (dependencies) ON (a, b, c) FROM functional_dependencies;

ANALYZE functional_dependencies;

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1';

EXPLAIN (COSTS OFF)
 SELECT * FROM functional_dependencies WHERE a = 1 AND b = '1' AND c = 1;

RESET random_page_cost;
DROP TABLE functional_dependencies;
