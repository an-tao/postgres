# MERGE UPDATE
#
# This test exercises atypical cases
# 1. UPDATEs of PKs that change the join in the ON clause
# 2. UPDATEs with WHEN AND conditions that would fail after concurrent update
# 3. UPDATEs with extra ON conditions that would fail after concurrent update

setup
{
  CREATE TABLE target (key int primary key, val text);
  INSERT INTO target VALUES (1, 'setup1');
}

teardown
{
  DROP TABLE target;
}

session "s1"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "merge1" { MERGE INTO target t USING (SELECT 1 as key, 'merge1' as val) s ON s.key = t.key WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.val) WHEN MATCHED THEN UPDATE set key = t.key + 1, val = t.val || ' updated by ' || s.val; }
step "c1" { COMMIT; }
step "a1" { ABORT; }

session "s2"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "merge2a" { MERGE INTO target t USING (SELECT 1 as key, 'merge2a' as val) s ON s.key = t.key WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.val) WHEN MATCHED THEN UPDATE set key = t.key + 1, val = t.val || ' updated by ' || s.val; }
step "merge2b" { MERGE INTO target t USING (SELECT 1 as key, 'merge2b' as val) s ON s.key = t.key WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.val) WHEN MATCHED AND t.key < 2 THEN UPDATE set key = t.key + 1, val = t.val || ' updated by ' || s.val; }
step "merge2c" { MERGE INTO target t USING (SELECT 1 as key, 'merge2c' as val) s ON s.key = t.key AND t.key < 2 WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.val) WHEN MATCHED THEN UPDATE set key = t.key + 1, val = t.val || ' updated by ' || s.val; }
step "select2" { SELECT * FROM target; }
step "c2" { COMMIT; }

permutation "merge1" "c1" "select2" "c2"
permutation "merge1" "c1" "merge2a" "select2" "c2"
permutation "merge1" "merge2a" "c1" "select2" "c2"
permutation "merge1" "merge2a" "a1" "select2" "c2"
permutation "merge1" "merge2b" "c1" "select2" "c2"
permutation "merge1" "merge2c" "c1" "select2" "c2"
