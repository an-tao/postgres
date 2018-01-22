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
step "merge1" { MERGE INTO upsert t USING (SELECT 1 as key, 'insert1' as val) s ON s.key = t.key WHEN NOT MATCHED INSERT VALUES (s.key, s.val) WHEN MATCHED UPDATE set val = t.val || ' updated by merge1'; }
step "c1" { COMMIT; }
step "a1" { ABORT; }

session "s2"
setup
{
  BEGIN ISOLATION LEVEL READ COMMITTED;
}
step "merge2" { MERGE INTO upsert t USING (SELECT 1 as key, 'insert2' as val) s ON s.key = t.key WHEN NOT MATCHED INSERT VALUES (s.key, s.val) WHEN MATCHED UPDATE set val = t.val || ' updated by merge2'; }
step "select2" { SELECT * FROM upsert; }
step "c2" { COMMIT; }
step "a2" { ABORT; }
