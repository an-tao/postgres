# Minimal test testing synchronous replication sync_state transition
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 2;

# Query checking sync_priority and sync_state of each standby
my $check_sql =
"SELECT application_name, sync_priority, sync_state FROM pg_stat_replication ORDER BY application_name;";

# Check that sync_state of each standby is expected.
# If $setting is given, synchronous_standby_names is set to it and
# the configuration file is reloaded before the test.
sub test_sync_state
{
	my ($self, $expected, $msg, $setting) = @_;

	if (defined($setting))
	{
		$self->psql('postgres',
			"ALTER SYSTEM SET synchronous_standby_names = '$setting';");
		$self->reload;
	}

	my $timeout_max = 30;
	my $timeout     = 0;
	my $result;

	# A reload may take some time to take effect on busy machines,
	# hence use a loop with a timeout to give some room for the test
	# to pass.
	while ($timeout < $timeout_max)
	{
		$result = $self->safe_psql('postgres', $check_sql);

		last if ($result eq $expected);

		$timeout++;
		sleep 1;
	}

	is($result, $expected, $msg);
}

# Initialize master node
my $node_master = get_new_node('master');
$node_master->init(allows_streaming => 1);
$node_master->start;
my $backup_name = 'master_backup';

# Take backup
$node_master->backup($backup_name);

# Create standby1 linking to master
my $node_standby_1 = get_new_node('standby1');
$node_standby_1->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_1->start;

# Create standby2 linking to master
my $node_standby_2 = get_new_node('standby2');
$node_standby_2->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_2->start;

# Create standby3 linking to master
my $node_standby_3 = get_new_node('standby3');
$node_standby_3->init_from_backup($node_master, $backup_name,
	has_streaming => 1);
$node_standby_3->start;

# Check that sync_state is determined correctly when
# synchronous_standby_names is specified in old syntax.
test_sync_state(
	$node_master, qq(standby1|1|sync
standby2|2|potential
standby3|0|async),
	'old syntax of synchronous_standby_names',
	'standby1,standby2');

# Check that all the standbys are considered as either sync or
# potential when * is specified in synchronous_standby_names.
# Note that standby1 is chosen as sync standby because
# it's stored in the head of WalSnd array which manages
# all the standbys though they have the same priority.
test_sync_state(
	$node_master, qq(standby1|1|sync
standby2|1|potential
standby3|1|potential),
	'asterisk in synchronous_standby_names',
	'*');
