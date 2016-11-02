# Testing of logical decoding using SQL interface and/or pg_recvlogical
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 6;
use IPC::Run;

my $benchtime = '20';

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->append_conf(
		'postgresql.conf', qq(
max_replication_slots = 4
wal_level = logical
max_prepared_transactions = 10
max_connections = 300
shared_buffers = '1GB'
));
$node_master->start;
my $backup_name = 'master_backup';

$node_master->safe_psql('postgres', qq[CREATE TABLE decoding_test(x integer, y text);]);

# Start a tx that we'll leave running. The simplest way to do that is to use a prepared xact:
$node_master->safe_psql('postgres', qq[BEGIN; INSERT INTO decoding_test(x,y) VALUES (42, 'prepared'); PREPARE TRANSACTION 'prep';]);

# Now with our long running xact we can run pgbench against the server to
# hammer it a while. We want TPS not write scale here.
$node_master->command_ok(['pgbench', '-d', 'postgres', '-i', '-s', '5']);

#$node_master->safe_psql('postgres', qq[SELECT pg_create_logical_replication_slot('test_slot', 'test_decoding');]);

# We have to run the slot creation in the background since it won't complete
# so use IPC::Run directly
diag "starting slot creation";
my ($stdin, $stdout, $stderr, $timeout) = (undef, '', '', IPC::Run::timeout(60 * 10));
my @cmd = ('pg_recvlogical', '--create-slot', '-d', $node_master->connstr('postgres'), '-S', 'test_slot', '-P', 'test_decoding');
my $recvlogical_handle = IPC::Run::start(\@cmd, \$stdin, \$stdout, \$stderr, $timeout);

sleep(1);

my $walsender_pid = $node_master->safe_psql('postgres', 'SELECT pid FROM pg_stat_replication');
diag "walsender PID is $walsender_pid";

diag "starting perf on the walsender";
my $perf_handle  = IPC::Run::start(['perf', 'record', '-p', $walsender_pid, '-g']);

# Hammer pgbench again after we start slot creation
diag "running pgbench for $benchtime seconds";
$node_master->command_ok(['pgbench', '-M', 'simple', '-N', '-c', '150', '-P', '1', '-T', $benchtime]);

# Slot creation should be waiting
diag $node_master->safe_psql('postgres', 'SELECT * FROM pg_replication_slots;');

my $starttime = time();
diag "committing prepared";
$node_master->safe_psql('postgres', qq[COMMIT PREPARED 'prep';]);

# Slot creation should be done (soon anyway)
diag $node_master->safe_psql('postgres', 'SELECT * FROM pg_replication_slots;');

diag 'waiting for slot creation to finish';
ok(IPC::Run::finish($recvlogical_handle), 'pg_recvlogical completed ok');

my $finishtime = time();
diag "slot creation took " . ($finishtime - $starttime) . "s (rounded down) after last xact committed";

ok(IPC::Run::finish($perf_handle), 'perf finished ok');

diag "pg_recvlogical stdout: $stdout";
diag "pg_recvlogical stderr: $stderr";

# Slot creation should be done (soon anyway)
diag $node_master->safe_psql('postgres', 'SELECT * FROM pg_replication_slots;');

$node_master->safe_psql('postgres', qq[INSERT INTO decoding_test(x,y) SELECT s, s::text FROM generate_series(1,10) s;]);

# Basic decoding works
my($result) = $node_master->safe_psql('postgres', qq[SELECT pg_logical_slot_get_changes('test_slot', NULL, NULL);]);
is(scalar(split /^/m, $result), 12, 'Decoding produced 12 rows inc BEGIN/COMMIT');

# If we immediately crash the server we might lose the progress we just made
# and replay the same changes again. But a clean shutdown should never repeat
# the same changes when we use the SQL decoding interface.
$node_master->restart('fast');

# There are no new writes, so the result should be empty.
$result = $node_master->safe_psql('postgres', qq[SELECT pg_logical_slot_get_changes('test_slot', NULL, NULL);]);
chomp($result);
is($result, '', 'Decoding after fast restart repeats no rows');

# done with the node
$node_master->stop;
