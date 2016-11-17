# Demonstrate that logical can follow timeline switches.
#
# Test logical decoding on a standby.
#
use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More tests => 43;
use RecursiveCopy;
use File::Copy;

my ($stdout, $stderr, $ret);
my $backup_name;

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1, has_archiving => 1);
$node_master->append_conf('postgresql.conf', "wal_level = 'logical'\n");
$node_master->append_conf('postgresql.conf', "max_replication_slots = 4\n");
$node_master->append_conf('postgresql.conf', "max_wal_senders = 4\n");
$node_master->append_conf('postgresql.conf', "log_min_messages = 'debug2'\n");
$node_master->append_conf('postgresql.conf', "log_error_verbosity = verbose\n");
$node_master->append_conf('postgresql.conf', "hot_standby_feedback = on\n");
# send status rapidly so we promptly advance xmin on master
$node_master->append_conf('postgresql.conf', "wal_receiver_status_interval = 1\n");
$node_master->dump_info;
$node_master->start;

$node_master->psql('postgres', q[CREATE DATABASE testdb]);

$node_master->safe_psql('testdb', q[SELECT * FROM pg_create_physical_replication_slot('decoding_standby');]);
$backup_name = 'b1';
my $backup_dir = $node_master->backup_dir . "/" . $backup_name;
TestLib::system_or_bail('pg_basebackup', '-D', $backup_dir, '-d', $node_master->connstr('testdb'), '--xlog-method=stream', '--write-recovery-conf', '--slot=decoding_standby');

open(my $fh, "<", $backup_dir . "/recovery.conf")
  or die "can't open recovery.conf";

my $found = 0;
while (my $line = <$fh>)
{
	chomp($line);
	if ($line eq "primary_slot_name = 'decoding_standby'")
	{
		$found = 1;
		last;
	}
}
ok($found, "using physical slot for standby");

sub print_phys_xmin
{
	my $slot = $node_master->slot('decoding_standby');
	return ($slot->{'xmin'}, $slot->{'catalog_xmin'});
}

my ($xmin, $catalog_xmin) = print_phys_xmin();
# without the catalog_xmin hot standby feedback patch, catalog_xmin is always null
# and xmin is the min(xmin, catalog_xmin) of all slots on the standby + anything else
# holding down xmin.
ok(!$xmin, "xmin null");
ok(!$catalog_xmin, "catalog_xmin null");

my $node_replica = get_new_node('replica');
$node_replica->init_from_backup(
	$node_master, $backup_name,
	has_streaming => 1,
	has_restoring => 1);

$node_replica->start;

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

($xmin, $catalog_xmin) = print_phys_xmin();
ok($xmin, "xmin not null");
ok(!$catalog_xmin, "catalog_xmin null");

# Create new slots on the replica, ignoring the ones on the master completely.
is($node_replica->psql('testdb', qq[SELECT * FROM pg_create_logical_replication_slot('standby_logical', 'test_decoding')]),
   0, 'logical slot creation on standby succeeded');

sub print_logical_xmin
{
	my $slot = $node_replica->slot('standby_logical');
	return ($slot->{'xmin'}, $slot->{'catalog_xmin'});
}

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

($xmin, $catalog_xmin) = print_phys_xmin();
isnt($xmin, '', "physical xmin not null");
isnt($catalog_xmin, '', "physical catalog_xmin not null");

($xmin, $catalog_xmin) = print_logical_xmin();
is($xmin, '', "logical xmin null");
isnt($catalog_xmin, '', "logical catalog_xmin not null");

$node_master->safe_psql('testdb', 'CREATE TABLE test_table(id serial primary key, blah text)');
$node_master->safe_psql('testdb', q[INSERT INTO test_table(blah) values ('itworks')]);

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

($xmin, $catalog_xmin) = print_phys_xmin();
isnt($xmin, '', "physical xmin not null");
isnt($catalog_xmin, '', "physical catalog_xmin not null");

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

($ret, $stdout, $stderr) = $node_replica->psql('testdb', qq[SELECT data FROM pg_logical_slot_get_changes('standby_logical', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'include-timestamp', '0')]);
is($ret, 0, 'replay from slot succeeded');
is($stdout, q{BEGIN
table public.test_table: INSERT: id[integer]:1 blah[text]:'itworks'
COMMIT}, 'replay results match');
is($stderr, 'psql:<stdin>:1: WARNING:  logical decoding during recovery is experimental', 'stderr is warning');

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

my ($physical_xmin, $physical_catalog_xmin) = print_phys_xmin();
isnt($physical_xmin, '', "physical xmin not null");
isnt($physical_catalog_xmin, '', "physical catalog_xmin not null");

my ($logical_xmin, $logical_catalog_xmin) = print_logical_xmin();
is($logical_xmin, '', "logical xmin null");
isnt($logical_catalog_xmin, '', "logical catalog_xmin not null");

# Ok, do a pile of tx's and make sure xmin advances.
# Ideally we'd just hold catalog_xmin, but since hs_feedback currently uses the slot,
# we hold down xmin.
for my $i (0 .. 1000)
{
    $node_master->safe_psql('testdb', qq[INSERT INTO test_table(blah) VALUES ('entry $i')]);
}
$node_master->safe_psql('testdb', 'VACUUM');

($ret, $stdout, $stderr) = $node_replica->psql('testdb', qq[SELECT data FROM pg_logical_slot_get_changes('standby_logical', NULL, NULL, 'include-xids', '0', 'skip-empty-xacts', '1', 'include-timestamp', '0')]);
is($ret, 0, 'replay of big series succeeded');

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

my ($new_logical_xmin, $new_logical_catalog_xmin) = print_logical_xmin();
is($new_logical_xmin, '', "logical xmin null");
isnt($new_logical_catalog_xmin, '', "logical catalog_xmin not null");
isnt($new_logical_catalog_xmin, $logical_catalog_xmin, "logical catalog_xmin changed");

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

my ($new_physical_xmin, $new_physical_catalog_xmin) = print_phys_xmin();
isnt($new_physical_xmin, '', "physical xmin not null");
# hot standby feedback should advance phys xmin now the standby's slot doesn't
# hold it down as far.
isnt($new_physical_xmin, $physical_xmin, "physical xmin changed");
isnt($new_physical_catalog_xmin, '', "physical catalog_xmin not null");

$node_replica->psql('testdb', q[SELECT pg_drop_replication_slot('standby_logical')]);

is($node_replica->slot('standby_logical')->{'slot_type'}, '', 'slot on standby dropped manually');

$node_master->wait_for_catchup($node_replica);
sleep(2); # ensure walreceiver feedback sent

($xmin, $catalog_xmin) = print_phys_xmin();
isnt($xmin, '', "physical xmin not null");
is($catalog_xmin, '', "physical catalog_xmin null");



# Create a couple of slots on the DB to ensure they are dropped when we drop
# the DB on the upstream if they're on the right DB, or not dropped if on
# another DB.
diag "Testing dropdb when downstream slot is not in-use";
$node_replica->command_ok(['pg_recvlogical', '-d', $node_replica->connstr('testdb'), '-P', 'test_decoding', '-S', 'dodropslot', '--create-slot']);
$node_replica->command_ok(['pg_recvlogical', '-d', $node_replica->connstr('postgres'), '-P', 'test_decoding', '-S', 'otherslot', '--create-slot']);

is($node_replica->slot('dodropslot')->{'slot_type'}, 'logical', 'slot dodropslot on standby created');
is($node_replica->slot('otherslot')->{'slot_type'}, 'logical', 'slot otherslot on standby created');

# dropdb on the master to verify slots are dropped on standby
$node_master->safe_psql('postgres', q[DROP DATABASE testdb]);

$node_master->wait_for_catchup($node_replica);

is($node_replica->safe_psql('postgres', q[SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'testdb')]), 'f',
  'database dropped on standby');

is($node_replica->slot('dodropslot2')->{'slot_type'}, '', 'slot on standby dropped');
is($node_replica->slot('otherslot')->{'slot_type'}, 'logical', 'otherslot on standby not dropped');



# This time, have the slot in-use on the downstream DB when we drop it.
diag "Testing dropdb when downstream slot is in-use";
$node_master->psql('postgres', q[CREATE DATABASE testdb2]);

$node_replica->command_ok(['pg_recvlogical', '-d', $node_replica->connstr('testdb2'), '-P', 'test_decoding', '-S', 'dodropslot2', '--create-slot']);
is($node_replica->slot('dodropslot2')->{'slot_type'}, 'logical', 'slot dodropslot2 on standby created');

# make sure the slot is in use
diag "starting pg_recvlogical";
my $handle = IPC::Run::start(['pg_recvlogical', '-d', $node_replica->connstr('testdb2'), '-S', 'dodropslot2', '-f', '-', '--start'], '>', \$stdout, '2>', \$stderr);
sleep(1);
$handle->reap_nb;
$handle->pump;

if (!$handle->pumpable)
{
	$handle->finish;
	BAIL_OUT("pg_recvlogical already exited with " . (($handle->results())[0]) . " and stderr '$stderr'");
}

is($node_replica->slot('dodropslot2')->{'active'}, 't', 'slot on standby is active')
  or BAIL_OUT("slot not active on standby, cannot continue. pg_recvlogical exited with '$stdout', '$stderr'");

diag "pg_recvlogical backend pid is " . $node_replica->slot('dodropslot2')->{'active_pid'};

# Master doesn't know the replica's slot is busy so dropdb should succeed
$node_master->safe_psql('postgres', q[DROP DATABASE testdb2]);
ok(1, 'dropdb finished');

# replication won't catch up, we'll error on apply while the slot is in use
# TODO check for error

$node_master->wait_for_catchup($node_replica);

sleep(1);

# our client should've terminated
do {
	local $@;
	eval {
		$handle->finish;
	};
	my $return = $?;
	my $save_exc = $@;
	if ($@) {
		diag "pg_recvlogical terminated with $? and stderr '$stderr'";	
		is($return, 1, "pg_recvlogical terminated by server");
	}
	else
	{
		fail("pg_recvlogical not terminated? $save_exc");
	}
};

is($node_replica->safe_psql('postgres', q[SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'testdb2')]), 'f',
  'database dropped on standby');

is($node_replica->slot('dodropslot2')->{'slot_type'}, '', 'slot on standby dropped');
