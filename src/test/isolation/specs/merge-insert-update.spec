# MERGE UPSERT
#
# This test tries to expose problems between concurrent sessions

setup
{
  CREATE TABLE upsert (key int primary key, val text);
}

teardown
{
  DROP TABLE upsert;
}

session "s1"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "insert1" { INSERT INTO upsert VALUES (1, 'insert1'); }
step "merge1" { MERGE INTO upsert t USING (SELECT 1 as key, 'merge1' as val) s ON s.key = t.key WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.val) WHEN MATCHED THEN UPDATE set val = t.val || ' updated by merge1'; }
step "c1" { COMMIT; }
step "a1" { ABORT; }

session "s2"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "merge2" { MERGE INTO upsert t USING (SELECT 1 as key, 'merge2' as val) s ON s.key = t.key WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.val) WHEN MATCHED THEN UPDATE set val = t.val || ' updated by merge2'; }
step "select2" { SELECT * FROM upsert; }
step "c2" { COMMIT; }
step "a2" { ABORT; }

permutation "merge1" "merge2" "c1" "select2" "c2"
permutation "merge1" "merge2" "a1" "select2" "c2"
permutation "merge1" "c1" "merge2" "select2" "c2"
permutation "insert1" "merge1" "merge2" "c1" "select2" "c2"
permutation "insert1" "merge1" "merge2" "a1" "select2" "c2"
