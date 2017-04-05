
CREATE TABLE updtst_tab1 (a integer unique, b int, c text, d text);
CREATE INDEX updtst_indx1 ON updtst_tab1 (b);
INSERT INTO updtst_tab1
       SELECT generate_series(1,10000), generate_series(70001, 80000), 'foo', 'bar';

-- This should be a HOT update as non-index key is updated, but the
-- page won't have any free space, so probably a non-HOT update
UPDATE updtst_tab1 SET c = 'foo1' WHERE a = 1;

-- Next update should be a HOT update as dead space is recycled
UPDATE updtst_tab1 SET c = 'foo2' WHERE a = 1;

-- And next too
UPDATE updtst_tab1 SET c = 'foo3' WHERE a = 1;

-- Now update one of the index key columns
UPDATE updtst_tab1 SET b = b + 70000 WHERE a = 1;

-- Ensure that the correct row is fetched
SELECT * FROM updtst_tab1 WHERE a = 1;
SELECT * FROM updtst_tab1 WHERE b = 70001 + 70000;

-- Even when seqscan is disabled and indexscan is forced
SET enable_seqscan = false;
EXPLAIN (costs off) SELECT * FROM updtst_tab1 WHERE b = 70001 + 70000;
SELECT * FROM updtst_tab1 WHERE b = 70001 + 70000;

-- Check if index only scan works correctly
EXPLAIN (costs off) SELECT b FROM updtst_tab1 WHERE b = 70001 + 70000;
SELECT b FROM updtst_tab1 WHERE b = 70001 + 70000;

-- Table must be vacuumed to force index-only scan
VACUUM updtst_tab1;
EXPLAIN (costs off) SELECT b FROM updtst_tab1 WHERE b = 70001 + 70000;
SELECT b FROM updtst_tab1 WHERE b = 70001 + 70000;

SET enable_seqscan = true;

DROP TABLE updtst_tab1;

------------------

CREATE TABLE updtst_tab2 (a integer unique, b int, c text, d text) WITH (fillfactor = 80);
CREATE INDEX updtst_indx2 ON updtst_tab2 (b);
INSERT INTO updtst_tab2
       SELECT generate_series(1,100), generate_series(701, 800), 'foo', 'bar';

UPDATE updtst_tab2 SET b = b + 700 WHERE a = 1;
UPDATE updtst_tab2 SET c = 'foo1'  WHERE a = 1;
UPDATE updtst_tab2 SET c = 'foo2'  WHERE a = 1;
UPDATE updtst_tab2 SET c = 'foo3'  WHERE a = 1;
UPDATE updtst_tab2 SET b = b - 700 WHERE a = 1;
UPDATE updtst_tab2 SET c = 'foo4'  WHERE a = 1;
UPDATE updtst_tab2 SET c = 'foo5'  WHERE a = 1;
UPDATE updtst_tab2 SET c = 'foo6'  WHERE a = 1;

SELECT count(*) FROM updtst_tab2 WHERE c = 'foo';
SELECT * FROM updtst_tab2 WHERE c = 'foo6';

EXPLAIN (costs off) SELECT * FROM updtst_tab2 WHERE b = 701;
SELECT * FROM updtst_tab2 WHERE a = 1;

SET enable_seqscan = false;
EXPLAIN (costs off) SELECT * FROM updtst_tab2 WHERE b = 701;
SELECT * FROM updtst_tab2 WHERE b = 701;

VACUUM updtst_tab2;
EXPLAIN (costs off) SELECT b FROM updtst_tab2 WHERE b = 701;
SELECT b FROM updtst_tab2 WHERE b = 701;

SET enable_seqscan = true;

DROP TABLE updtst_tab2;
------------------

CREATE TABLE updtst_tab3 (a integer unique, b int, c text, d text) WITH (fillfactor = 80);
CREATE INDEX updtst_indx3 ON updtst_tab3 (b);
INSERT INTO updtst_tab3
       SELECT generate_series(1,100), generate_series(701, 800), 'foo', 'bar';

BEGIN;
UPDATE updtst_tab3 SET c = 'foo1', b = b + 700 WHERE a = 1;
UPDATE updtst_tab3 SET c = 'foo2'  WHERE a = 1;
UPDATE updtst_tab3 SET c = 'foo3'  WHERE a = 1;
UPDATE updtst_tab3 SET b = b - 700 WHERE a = 1;
UPDATE updtst_tab3 SET c = 'foo4'  WHERE a = 1;
UPDATE updtst_tab3 SET c = 'foo5'  WHERE a = 1;
UPDATE updtst_tab3 SET c = 'foo6'  WHERE a = 1;

-- Abort the transaction and ensure the original tuple is visible correctly
ROLLBACK;

BEGIN;
UPDATE updtst_tab3 SET c = 'foo11', b = b + 750 WHERE b = 701;
UPDATE updtst_tab3 SET c = 'foo12'  WHERE a = 1;
UPDATE updtst_tab3 SET b = b - 30 WHERE a = 1;
COMMIT;

SELECT count(*) FROM updtst_tab3 WHERE c = 'foo';
SELECT * FROM updtst_tab3 WHERE c = 'foo6';
SELECT * FROM updtst_tab3 WHERE c = 'foo12';

SELECT * FROM updtst_tab3 WHERE b = 701;
SELECT * FROM updtst_tab3 WHERE b = 1421;
SELECT * FROM updtst_tab3 WHERE a = 1;

SELECT * FROM updtst_tab3 WHERE b = 701;
SELECT * FROM updtst_tab3 WHERE b = 1421;

VACUUM updtst_tab3;
EXPLAIN (costs off) SELECT b FROM updtst_tab3 WHERE b = 701;
SELECT b FROM updtst_tab3 WHERE b = 701;
SELECT b FROM updtst_tab3 WHERE b = 1421;

BEGIN;
UPDATE updtst_tab3 SET c = 'foo21', b = b + 700 WHERE a = 2;
UPDATE updtst_tab3 SET c = 'foo22'  WHERE a = 2;
UPDATE updtst_tab3 SET c = 'foo23'  WHERE a = 2;
UPDATE updtst_tab3 SET b = b - 700 WHERE a = 2;
UPDATE updtst_tab3 SET c = 'foo24'  WHERE a = 2;
UPDATE updtst_tab3 SET c = 'foo25'  WHERE a = 2;
UPDATE updtst_tab3 SET c = 'foo26'  WHERE a = 2;

-- Abort the transaction and ensure the original tuple is visible correctly
ROLLBACK;

SET enable_seqscan = false;

BEGIN;
UPDATE updtst_tab3 SET c = 'foo21', b = b + 750 WHERE b = 702;
UPDATE updtst_tab3 SET c = 'foo22'  WHERE a = 2;
UPDATE updtst_tab3 SET b = b - 30 WHERE a = 2;
COMMIT;

SELECT count(*) FROM updtst_tab3 WHERE c = 'foo';
SELECT * FROM updtst_tab3 WHERE c = 'foo26';
SELECT * FROM updtst_tab3 WHERE c = 'foo22';

SELECT * FROM updtst_tab3 WHERE b = 702;
SELECT * FROM updtst_tab3 WHERE b = 1422;
SELECT * FROM updtst_tab3 WHERE a = 2;

-- Try fetching both old and new value using updtst_indx3
SELECT * FROM updtst_tab3 WHERE b = 702;
SELECT * FROM updtst_tab3 WHERE b = 1422;

VACUUM updtst_tab3;
EXPLAIN (costs off) SELECT b FROM updtst_tab3 WHERE b = 702;
SELECT b FROM updtst_tab3 WHERE b = 702;
SELECT b FROM updtst_tab3 WHERE b = 1422;

SET enable_seqscan = true;

DROP TABLE updtst_tab3;
------------------

CREATE TABLE test_warm (a text unique, b text);
CREATE INDEX test_warmindx ON test_warm (lower(a));
INSERT INTO test_warm values ('test', 'foo');
UPDATE test_warm SET a = 'TEST';
select *, ctid from test_warm where lower(a) = 'test';
explain select * from test_warm where lower(a) = 'test';
select *, ctid from test_warm where lower(a) = 'test';
select *, ctid from test_warm where a = 'test';
select *, ctid from test_warm where a = 'TEST';
set enable_bitmapscan TO false;
explain select * from test_warm where lower(a) = 'test';
select *, ctid from test_warm where lower(a) = 'test';
DROP TABLE test_warm;

--- Test with toast data types

CREATE TABLE test_toast_warm (a int unique, b text, c int);
CREATE INDEX test_toast_warm_index ON test_toast_warm(b);

-- insert a large enough value to cause index datum compression
INSERT INTO test_toast_warm VALUES (1, repeat('a', 600), 100);
INSERT INTO test_toast_warm VALUES (2, repeat('b', 2), 100);
INSERT INTO test_toast_warm VALUES (3, repeat('c', 4), 100);
INSERT INTO test_toast_warm VALUES (4, repeat('d', 63), 100);
INSERT INTO test_toast_warm VALUES (5, repeat('e', 126), 100);
INSERT INTO test_toast_warm VALUES (6, repeat('f', 127), 100);
INSERT INTO test_toast_warm VALUES (7, repeat('g', 128), 100);
INSERT INTO test_toast_warm VALUES (8, repeat('h', 3200), 100);

UPDATE test_toast_warm SET b = repeat('q', 600) WHERE a = 1;
UPDATE test_toast_warm SET b = repeat('r', 2) WHERE a = 2;
UPDATE test_toast_warm SET b = repeat('s', 4) WHERE a = 3;
UPDATE test_toast_warm SET b = repeat('t', 63) WHERE a = 4;
UPDATE test_toast_warm SET b = repeat('u', 126) WHERE a = 5;
UPDATE test_toast_warm SET b = repeat('v', 127) WHERE a = 6;
UPDATE test_toast_warm SET b = repeat('w', 128) WHERE a = 7;
UPDATE test_toast_warm SET b = repeat('x', 3200) WHERE a = 8;


SET enable_seqscan TO false;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE a = 1;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE b = repeat('a', 600);
EXPLAIN (costs off) SELECT b FROM test_toast_warm WHERE b = repeat('a', 600);
SELECT a, b FROM test_toast_warm WHERE a = 1;
SELECT a, b FROM test_toast_warm WHERE b = repeat('a', 600);
SELECT b FROM test_toast_warm WHERE b = repeat('a', 600);
SELECT a, b FROM test_toast_warm WHERE b = repeat('q', 600);
SELECT b FROM test_toast_warm WHERE b = repeat('q', 600);

SELECT a, b FROM test_toast_warm WHERE b = repeat('r', 2);
SELECT a, b FROM test_toast_warm WHERE b = repeat('s', 4);
SELECT a, b FROM test_toast_warm WHERE b = repeat('t', 63);
SELECT a, b FROM test_toast_warm WHERE b = repeat('u', 126);
SELECT a, b FROM test_toast_warm WHERE b = repeat('v', 127);
SELECT a, b FROM test_toast_warm WHERE b = repeat('w', 128);
SELECT a, b FROM test_toast_warm WHERE b = repeat('x', 3200);

SET enable_seqscan TO true;
SET enable_indexscan TO false;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE b = repeat('q', 600);
EXPLAIN (costs off) SELECT b FROM test_toast_warm WHERE b = repeat('q', 600);
SELECT a, b FROM test_toast_warm WHERE b = repeat('q', 600);
SELECT b FROM test_toast_warm WHERE b = repeat('q', 600);

SELECT a, b FROM test_toast_warm WHERE b = repeat('r', 2);
SELECT a, b FROM test_toast_warm WHERE b = repeat('s', 4);
SELECT a, b FROM test_toast_warm WHERE b = repeat('t', 63);
SELECT a, b FROM test_toast_warm WHERE b = repeat('u', 126);
SELECT a, b FROM test_toast_warm WHERE b = repeat('v', 127);
SELECT a, b FROM test_toast_warm WHERE b = repeat('w', 128);
SELECT a, b FROM test_toast_warm WHERE b = repeat('x', 3200);

DROP TABLE test_toast_warm;

-- Test with numeric data type

CREATE TABLE test_toast_warm (a int unique, b numeric(10,2), c int);
CREATE INDEX test_toast_warm_index ON test_toast_warm(b);

INSERT INTO test_toast_warm VALUES (1, 10.2, 100);
INSERT INTO test_toast_warm VALUES (2, 11.22, 100);
INSERT INTO test_toast_warm VALUES (3, 12.222, 100);
INSERT INTO test_toast_warm VALUES (4, 13.20, 100);
INSERT INTO test_toast_warm VALUES (5, 14.201, 100);

UPDATE test_toast_warm SET b = 100.2 WHERE a = 1;
UPDATE test_toast_warm SET b = 101.22 WHERE a = 2;
UPDATE test_toast_warm SET b = 102.222 WHERE a = 3;
UPDATE test_toast_warm SET b = 103.20 WHERE a = 4;
UPDATE test_toast_warm SET b = 104.201 WHERE a = 5;

SELECT * FROM test_toast_warm;

SET enable_seqscan TO false;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE a = 1;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE b = 10.2;
EXPLAIN (costs off) SELECT b FROM test_toast_warm WHERE b = 100.2;
SELECT a, b FROM test_toast_warm WHERE a = 1;
SELECT a, b FROM test_toast_warm WHERE b = 10.2;
SELECT b FROM test_toast_warm WHERE b = 10.2;
SELECT a, b FROM test_toast_warm WHERE b = 100.2;
SELECT b FROM test_toast_warm WHERE b = 100.2;

SELECT a, b FROM test_toast_warm WHERE b = 101.22;
SELECT a, b FROM test_toast_warm WHERE b = 102.222;
SELECT a, b FROM test_toast_warm WHERE b = 102.22;
SELECT a, b FROM test_toast_warm WHERE b = 103.20;
SELECT a, b FROM test_toast_warm WHERE b = 104.201;
SELECT a, b FROM test_toast_warm WHERE b = 104.20;

SET enable_seqscan TO true;
SET enable_indexscan TO false;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE a = 1;
EXPLAIN (costs off) SELECT a, b FROM test_toast_warm WHERE b = 10.2;
EXPLAIN (costs off) SELECT b FROM test_toast_warm WHERE b = 100.2;
SELECT a, b FROM test_toast_warm WHERE a = 1;
SELECT a, b FROM test_toast_warm WHERE b = 10.2;
SELECT b FROM test_toast_warm WHERE b = 10.2;
SELECT a, b FROM test_toast_warm WHERE b = 100.2;
SELECT b FROM test_toast_warm WHERE b = 100.2;

SELECT a, b FROM test_toast_warm WHERE b = 101.22;
SELECT a, b FROM test_toast_warm WHERE b = 102.222;
SELECT a, b FROM test_toast_warm WHERE b = 102.22;
SELECT a, b FROM test_toast_warm WHERE b = 103.20;
SELECT a, b FROM test_toast_warm WHERE b = 104.201;
SELECT a, b FROM test_toast_warm WHERE b = 104.20;

DROP TABLE test_toast_warm;

-- Toasted heap attributes
CREATE TABLE toasttest(descr text , cnt int DEFAULT 0, f1 text, f2 text);
CREATE INDEX testindx1 ON toasttest(descr);
CREATE INDEX testindx2 ON toasttest(f1);

INSERT INTO toasttest(descr, f1, f2) VALUES('two-compressed', repeat('1234567890',1000), repeat('1234567890',1000));
INSERT INTO toasttest(descr, f1, f2) VALUES('two-toasted', repeat('1234567890',20000), repeat('1234567890',50000));
INSERT INTO toasttest(descr, f1, f2) VALUES('one-compressed,one-toasted', repeat('1234567890',1000), repeat('1234567890',50000));

SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;

-- UPDATE f1 by doing string manipulation, but the updated value remains the
-- same as the old value
UPDATE toasttest SET cnt = cnt +1, f1 = trim(leading '-' from '-'||f1) RETURNING substring(toasttest::text, 1, 200);
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SET enable_seqscan TO false;
SET seq_page_cost = 10000;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SET enable_seqscan TO true;
SET seq_page_cost TO default;

-- UPDATE f1 for real this time
UPDATE toasttest SET cnt = cnt +1, f1 = '-'||f1 RETURNING substring(toasttest::text, 1, 200);
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SET enable_seqscan TO false;
SET seq_page_cost = 10000;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SET enable_seqscan TO true;
SET seq_page_cost TO default;

-- UPDATE f1 from toasted to compressed
UPDATE toasttest SET cnt = cnt +1, f1 = repeat('1234567890',1000) WHERE descr = 'two-toasted';
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SET enable_seqscan TO false;
SET seq_page_cost = 10000;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SET enable_seqscan TO true;
SET seq_page_cost TO default;

-- UPDATE f1 from compressed to toasted
UPDATE toasttest SET cnt = cnt +1, f1 = repeat('1234567890',2000) WHERE descr = 'one-compressed,one-toasted';
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest;
SET enable_seqscan TO false;
SET seq_page_cost = 10000;
EXPLAIN (analyze, costs off, timing off, summary off) SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SELECT ctid, substring(toasttest::text, 1, 200) FROM toasttest ORDER BY f1;
SET enable_seqscan TO true;
SET seq_page_cost TO default;

DROP TABLE toasttest;

-- Test enable_warm reloption
CREATE TABLE testrelopt (a int unique, b int, c int) WITH (enable_warm = true);
ALTER TABLE testrelopt SET (enable_warm = true);
ALTER TABLE testrelopt RESET (enable_warm);
-- should fail since we don't allow turning WARM off
ALTER TABLE testrelopt SET (enable_warm = false);
DROP TABLE testrelopt;

CREATE TABLE testrelopt (a int unique, b int, c int) WITH (enable_warm = false);
-- should be ok since the default is ON and we support turning WARM ON
ALTER TABLE testrelopt RESET (enable_warm);
ALTER TABLE testrelopt SET (enable_warm = true);
-- should fail since we don't allow turning WARM off
ALTER TABLE testrelopt SET (enable_warm = false);
DROP TABLE testrelopt;
