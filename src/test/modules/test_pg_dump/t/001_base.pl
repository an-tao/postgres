use strict;
use warnings;

use Config;
use PostgresNode;
use TestLib;
use Test::More;

my $tempdir       = TestLib::tempdir;
my $tempdir_short = TestLib::tempdir_short;

###############################################################
# This structure is based off of the src/bin/pg_dump/t test
# suite.
###############################################################
# Definition of the pg_dump runs to make.
#
# Each of these runs are named and those names are used below
# to define how each test should (or shouldn't) treat a result
# from a given run.
#
# test_key indicates that a given run should simply use the same
# set of like/unlike tests as another run, and which run that is.
#
# dump_cmd is the pg_dump command to run, which is an array of
# the full command and arguments to run.  Note that this is run
# using $node->command_ok(), so the port does not need to be
# specified and is pulled from $PGPORT, which is set by the
# PostgresNode system.
#
# restore_cmd is the pg_restore command to run, if any.  Note
# that this should generally be used when the pg_dump goes to
# a non-text file and that the restore can then be used to
# generate a text file to run through the tests from the
# non-text file generated by pg_dump.
#
# TODO: Have pg_restore actually restore to an independent
# database and then pg_dump *that* database (or something along
# those lines) to validate that part of the process.

my %pgdump_runs = (
	binary_upgrade => {
		dump_cmd => [
			'pg_dump',       "--file=$tempdir/binary_upgrade.sql",
			'--schema-only', '--binary-upgrade',
			'--dbname=postgres', ], },
	clean => {
		dump_cmd => [
			'pg_dump', "--file=$tempdir/clean.sql",
			'-c',      '--dbname=postgres', ], },
	clean_if_exists => {
		dump_cmd => [
			'pg_dump',
			"--file=$tempdir/clean_if_exists.sql",
			'-c',
			'--if-exists',
			'--encoding=UTF8',    # no-op, just tests that option is accepted
			'postgres', ], },
	column_inserts => {
		dump_cmd => [
			'pg_dump', "--file=$tempdir/column_inserts.sql",
			'-a',      '--column-inserts',
			'postgres', ], },
	createdb => {
		dump_cmd => [
			'pg_dump',
			"--file=$tempdir/createdb.sql",
			'-C',
			'-R',                 # no-op, just for testing
			'postgres', ], },
	data_only => {
		dump_cmd => [
			'pg_dump',
			"--file=$tempdir/data_only.sql",
			'-a',
			'-v',                 # no-op, just make sure it works
			'postgres', ], },
	defaults => {
		dump_cmd => [ 'pg_dump', '-f', "$tempdir/defaults.sql", 'postgres', ],
	},
	defaults_custom_format => {
		test_key => 'defaults',
		dump_cmd => [
			'pg_dump', '-Fc', '-Z6',
			"--file=$tempdir/defaults_custom_format.dump", 'postgres', ],
		restore_cmd => [
			'pg_restore',
			"--file=$tempdir/defaults_custom_format.sql",
			"$tempdir/defaults_custom_format.dump", ], },
	defaults_dir_format => {
		test_key => 'defaults',
		dump_cmd => [
			'pg_dump',                             '-Fd',
			"--file=$tempdir/defaults_dir_format", 'postgres', ],
		restore_cmd => [
			'pg_restore',
			"--file=$tempdir/defaults_dir_format.sql",
			"$tempdir/defaults_dir_format", ], },
	defaults_parallel => {
		test_key => 'defaults',
		dump_cmd => [
			'pg_dump', '-Fd', '-j2', "--file=$tempdir/defaults_parallel",
			'postgres', ],
		restore_cmd => [
			'pg_restore',
			"--file=$tempdir/defaults_parallel.sql",
			"$tempdir/defaults_parallel", ], },
	defaults_tar_format => {
		test_key => 'defaults',
		dump_cmd => [
			'pg_dump',                                 '-Ft',
			"--file=$tempdir/defaults_tar_format.tar", 'postgres', ],
		restore_cmd => [
			'pg_restore',
			"--file=$tempdir/defaults_tar_format.sql",
			"$tempdir/defaults_tar_format.tar", ], },
	pg_dumpall_globals => {
		dump_cmd =>
		  [ 'pg_dumpall', "--file=$tempdir/pg_dumpall_globals.sql", '-g', ],
	},
	no_privs => {
		dump_cmd =>
		  [ 'pg_dump', "--file=$tempdir/no_privs.sql", '-x', 'postgres', ], },
	no_owner => {
		dump_cmd =>
		  [ 'pg_dump', "--file=$tempdir/no_owner.sql", '-O', 'postgres', ], },
	schema_only => {
		dump_cmd =>
		  [ 'pg_dump', "--file=$tempdir/schema_only.sql", '-s', 'postgres', ],
	},
	section_pre_data => {
		dump_cmd => [
			'pg_dump',            "--file=$tempdir/section_pre_data.sql",
			'--section=pre-data', 'postgres', ], },
	section_data => {
		dump_cmd => [
			'pg_dump',        "--file=$tempdir/section_data.sql",
			'--section=data', 'postgres', ], },
	section_post_data => {
		dump_cmd => [
			'pg_dump',             "--file=$tempdir/section_post_data.sql",
			'--section=post-data', 'postgres', ], },);

###############################################################
# Definition of the tests to run.
#
# Each test is defined using the log message that will be used.
#
# A regexp should be defined for each test which provides the
# basis for the test.  That regexp will be run against the output
# file of each of the runs which the test is to be run against
# and the success of the result will depend on if the regexp
# result matches the expected 'like' or 'unlike' case.
#
# For each test, there are two sets of runs defined, one for
# the 'like' tests and one for the 'unlike' tests.  'like'
# essentially means "the regexp for this test must match the
# output file".  'unlike' is the opposite.
#
# There are a few 'catch-all' tests which can be used to have
# a single, simple, test to over a range of other tests.  For
# example, there is a '^CREATE ' test, which is used for the
# 'data-only' test as there should never be any kind of CREATE
# statement in a 'data-only' run.  Without the catch-all, we
# would have to list the 'data-only' run in each and every
# 'CREATE xxxx' test, which would be a lot of additional tests.
#
# Note that it makes no sense for the same run to ever be listed
# in both 'like' and 'unlike' categories.
#
# There can then be a 'create_sql' and 'create_order' for a
# given test.  The 'create_sql' commands are collected up in
# 'create_order' and then run against the database prior to any
# of the pg_dump runs happening.  This is what "seeds" the
# system with objects to be dumped out.
#
# Building of this hash takes a bit of time as all of the regexps
# included in it are compiled.  This greatly improves performance
# as the regexps are used for each run the test applies to.

my %tests = (
	'ALTER EXTENSION test_pg_dump' => {
		create_order => 9,
		create_sql =>
'ALTER EXTENSION test_pg_dump ADD TABLE regress_pg_dump_table_added;',
		regexp => qr/^
			\QCREATE TABLE regress_pg_dump_table_added (\E
			\n\s+\Qcol1 integer NOT NULL,\E
			\n\s+\Qcol2 integer\E
			\n\);\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE EXTENSION test_pg_dump' => {
		create_order => 2,
		create_sql   => 'CREATE EXTENSION test_pg_dump;',
		regexp       => qr/^
			\QCREATE EXTENSION IF NOT EXISTS test_pg_dump WITH SCHEMA public;\E
			\n/xm,
		like => {
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_privs         => 1,
			no_owner         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			binary_upgrade     => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'CREATE ROLE regress_dump_test_role' => {
		create_order => 1,
		create_sql   => 'CREATE ROLE regress_dump_test_role;',
		regexp       => qr/^CREATE ROLE regress_dump_test_role;\n/m,
		like         => { pg_dumpall_globals => 1, },
		unlike       => {
			binary_upgrade    => 1,
			clean             => 1,
			clean_if_exists   => 1,
			createdb          => 1,
			defaults          => 1,
			no_privs          => 1,
			no_owner          => 1,
			schema_only       => 1,
			section_pre_data  => 1,
			section_post_data => 1, }, },

	'CREATE SEQUENCE regress_pg_dump_table_col1_seq' => {
		regexp => qr/^
                    \QCREATE SEQUENCE regress_pg_dump_table_col1_seq\E
                    \n\s+\QAS integer\E
                    \n\s+\QSTART WITH 1\E
                    \n\s+\QINCREMENT BY 1\E
                    \n\s+\QNO MINVALUE\E
                    \n\s+\QNO MAXVALUE\E
                    \n\s+\QCACHE 1;\E
                    \n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE TABLE regress_pg_dump_table_added' => {
		create_order => 7,
		create_sql =>
'CREATE TABLE regress_pg_dump_table_added (col1 int not null, col2 int);',
		regexp => qr/^
			\QCREATE TABLE regress_pg_dump_table_added (\E
			\n\s+\Qcol1 integer NOT NULL,\E
			\n\s+\Qcol2 integer\E
			\n\);\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE SEQUENCE regress_pg_dump_seq' => {
		regexp => qr/^
                    \QCREATE SEQUENCE regress_pg_dump_seq\E
                    \n\s+\QSTART WITH 1\E
                    \n\s+\QINCREMENT BY 1\E
                    \n\s+\QNO MINVALUE\E
                    \n\s+\QNO MAXVALUE\E
                    \n\s+\QCACHE 1;\E
                    \n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'SETVAL SEQUENCE regress_seq_dumpable' => {
		create_order => 6,
		create_sql   => qq{SELECT nextval('regress_seq_dumpable');},
		regexp       => qr/^
			\QSELECT pg_catalog.setval('regress_seq_dumpable', 1, true);\E
			\n/xm,
		like => {
			clean           => 1,
			clean_if_exists => 1,
			createdb        => 1,
			data_only       => 1,
			defaults        => 1,
			no_owner        => 1,
			no_privs        => 1, },
		unlike => {
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE TABLE regress_pg_dump_table' => {
		regexp => qr/^
			\QCREATE TABLE regress_pg_dump_table (\E
			\n\s+\Qcol1 integer NOT NULL,\E
			\n\s+\Qcol2 integer\E
			\n\);\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE ACCESS METHOD regress_test_am' => {
		regexp => qr/^
			\QCREATE ACCESS METHOD regress_test_am TYPE INDEX HANDLER bthandler;\E
			\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'COMMENT ON EXTENSION test_pg_dump' => {
		regexp => qr/^
			\QCOMMENT ON EXTENSION test_pg_dump \E
			\QIS 'Test pg_dump with an extension';\E
			\n/xm,
		like => {
			binary_upgrade   => 1,
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_privs         => 1,
			no_owner         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'GRANT SELECT regress_pg_dump_table_added pre-ALTER EXTENSION' => {
		create_order => 8,
		create_sql =>
'GRANT SELECT ON regress_pg_dump_table_added TO regress_dump_test_role;',
		regexp => qr/^
			\QGRANT SELECT ON TABLE regress_pg_dump_table_added TO regress_dump_test_role;\E
			\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'REVOKE SELECT regress_pg_dump_table_added post-ALTER EXTENSION' => {
		create_order => 10,
		create_sql =>
'REVOKE SELECT ON regress_pg_dump_table_added FROM regress_dump_test_role;',
		regexp => qr/^
			\QREVOKE SELECT ON TABLE regress_pg_dump_table_added FROM regress_dump_test_role;\E
			\n/xm,
		like => {
			binary_upgrade   => 1,
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_owner         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			no_privs           => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'GRANT SELECT ON TABLE regress_pg_dump_table' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT SELECT ON TABLE regress_pg_dump_table TO regress_dump_test_role;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'GRANT SELECT(col1) ON regress_pg_dump_table' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT SELECT(col1) ON TABLE regress_pg_dump_table TO PUBLIC;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'GRANT SELECT(col2) ON regress_pg_dump_table TO regress_dump_test_role'
	  => {
		create_order => 4,
		create_sql   => 'GRANT SELECT(col2) ON regress_pg_dump_table
						   TO regress_dump_test_role;',
		regexp => qr/^
			\QGRANT SELECT(col2) ON TABLE regress_pg_dump_table TO regress_dump_test_role;\E
			\n/xm,
		like => {
			binary_upgrade   => 1,
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_owner         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			no_privs           => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'GRANT USAGE ON regress_pg_dump_table_col1_seq TO regress_dump_test_role'
	  => {
		create_order => 5,
		create_sql => 'GRANT USAGE ON SEQUENCE regress_pg_dump_table_col1_seq
		                   TO regress_dump_test_role;',
		regexp => qr/^
			\QGRANT USAGE ON SEQUENCE regress_pg_dump_table_col1_seq TO regress_dump_test_role;\E
			\n/xm,
		like => {
			binary_upgrade   => 1,
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_owner         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			no_privs           => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

	'GRANT USAGE ON regress_pg_dump_seq TO regress_dump_test_role' => {
		regexp => qr/^
			\QGRANT USAGE ON SEQUENCE regress_pg_dump_seq TO regress_dump_test_role;\E
			\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'REVOKE SELECT(col1) ON regress_pg_dump_table' => {
		create_order => 3,
		create_sql   => 'REVOKE SELECT(col1) ON regress_pg_dump_table
						   FROM PUBLIC;',
		regexp => qr/^
			\QREVOKE SELECT(col1) ON TABLE regress_pg_dump_table FROM PUBLIC;\E
			\n/xm,
		like => {
			binary_upgrade   => 1,
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_owner         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			no_privs           => 1,
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },

 # Objects included in extension part of a schema created by this extension */
	'CREATE TABLE regress_pg_dump_schema.test_table' => {
		regexp => qr/^
			\QCREATE TABLE test_table (\E
			\n\s+\Qcol1 integer,\E
			\n\s+\Qcol2 integer\E
			\n\);\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'GRANT SELECT ON regress_pg_dump_schema.test_table' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT SELECT ON TABLE test_table TO regress_dump_test_role;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE SEQUENCE regress_pg_dump_schema.test_seq' => {
		regexp => qr/^
                    \QCREATE SEQUENCE test_seq\E
                    \n\s+\QSTART WITH 1\E
                    \n\s+\QINCREMENT BY 1\E
                    \n\s+\QNO MINVALUE\E
                    \n\s+\QNO MAXVALUE\E
                    \n\s+\QCACHE 1;\E
                    \n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'GRANT USAGE ON regress_pg_dump_schema.test_seq' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT USAGE ON SEQUENCE test_seq TO regress_dump_test_role;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE TYPE regress_pg_dump_schema.test_type' => {
		regexp => qr/^
                    \QCREATE TYPE test_type AS (\E
                    \n\s+\Qcol1 integer\E
                    \n\);\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'GRANT USAGE ON regress_pg_dump_schema.test_type' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT ALL ON TYPE test_type TO regress_dump_test_role;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE FUNCTION regress_pg_dump_schema.test_func' => {
		regexp => qr/^
            \QCREATE FUNCTION test_func() RETURNS integer\E
            \n\s+\QLANGUAGE sql\E
            \n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'GRANT ALL ON regress_pg_dump_schema.test_func' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT ALL ON FUNCTION test_func() TO regress_dump_test_role;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'CREATE AGGREGATE regress_pg_dump_schema.test_agg' => {
		regexp => qr/^
            \QCREATE AGGREGATE test_agg(smallint) (\E
            \n\s+\QSFUNC = int2_sum,\E
            \n\s+\QSTYPE = bigint\E
            \n\);\n/xm,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_privs           => 1,
			no_owner           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	'GRANT ALL ON regress_pg_dump_schema.test_agg' => {
		regexp => qr/^
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(true);\E\n
			\QGRANT ALL ON FUNCTION test_agg(smallint) TO regress_dump_test_role;\E\n
			\QSELECT pg_catalog.binary_upgrade_set_record_init_privs(false);\E
			\n/xms,
		like   => { binary_upgrade => 1, },
		unlike => {
			clean              => 1,
			clean_if_exists    => 1,
			createdb           => 1,
			defaults           => 1,
			no_owner           => 1,
			no_privs           => 1,
			pg_dumpall_globals => 1,
			schema_only        => 1,
			section_pre_data   => 1,
			section_post_data  => 1, }, },

	# Objects not included in extension, part of schema created by extension
	'CREATE TABLE regress_pg_dump_schema.external_tab' => {
		create_order => 4,
		create_sql   => 'CREATE TABLE regress_pg_dump_schema.external_tab
						   (col1 int);',
		regexp => qr/^
			\QCREATE TABLE external_tab (\E
			\n\s+\Qcol1 integer\E
			\n\);\n/xm,
		like => {
			binary_upgrade   => 1,
			clean            => 1,
			clean_if_exists  => 1,
			createdb         => 1,
			defaults         => 1,
			no_owner         => 1,
			no_privs         => 1,
			schema_only      => 1,
			section_pre_data => 1, },
		unlike => {
			pg_dumpall_globals => 1,
			section_post_data  => 1, }, },);

#########################################
# Create a PG instance to test actually dumping from

my $node = get_new_node('main');
$node->init;
$node->start;

my $port = $node->port;

my $num_tests = 0;

foreach my $run (sort keys %pgdump_runs)
{
	my $test_key = $run;

	# Each run of pg_dump is a test itself
	$num_tests++;

	# If there is a restore cmd, that's another test
	if ($pgdump_runs{$run}->{restore_cmd})
	{
		$num_tests++;
	}

	if ($pgdump_runs{$run}->{test_key})
	{
		$test_key = $pgdump_runs{$run}->{test_key};
	}

	# Then count all the tests run against each run
	foreach my $test (sort keys %tests)
	{
		if ($tests{$test}->{like}->{$test_key})
		{
			$num_tests++;
		}

		if ($tests{$test}->{unlike}->{$test_key})
		{
			$num_tests++;
		}
	}
}
plan tests => $num_tests;

#########################################
# Set up schemas, tables, etc, to be dumped.

# Build up the create statements
my $create_sql = '';

foreach my $test (
	sort {
		if ($tests{$a}->{create_order} and $tests{$b}->{create_order})
		{
			$tests{$a}->{create_order} <=> $tests{$b}->{create_order};
		}
		elsif ($tests{$a}->{create_order})
		{
			-1;
		}
		elsif ($tests{$b}->{create_order})
		{
			1;
		}
		else
		{
			0;
		}
	} keys %tests)
{
	if ($tests{$test}->{create_sql})
	{
		$create_sql .= $tests{$test}->{create_sql};
	}
}

# Send the combined set of commands to psql
$node->safe_psql('postgres', $create_sql);

#########################################
# Run all runs

foreach my $run (sort keys %pgdump_runs)
{

	my $test_key = $run;

	$node->command_ok(\@{ $pgdump_runs{$run}->{dump_cmd} },
		"$run: pg_dump runs");

	if ($pgdump_runs{$run}->{restore_cmd})
	{
		$node->command_ok(\@{ $pgdump_runs{$run}->{restore_cmd} },
			"$run: pg_restore runs");
	}

	if ($pgdump_runs{$run}->{test_key})
	{
		$test_key = $pgdump_runs{$run}->{test_key};
	}

	my $output_file = slurp_file("$tempdir/${run}.sql");

	#########################################
	# Run all tests where this run is included
	# as either a 'like' or 'unlike' test.

	foreach my $test (sort keys %tests)
	{
		if ($tests{$test}->{like}->{$test_key})
		{
			like($output_file, $tests{$test}->{regexp}, "$run: dumps $test");
		}

		if ($tests{$test}->{unlike}->{$test_key})
		{
			unlike(
				$output_file,
				$tests{$test}->{regexp},
				"$run: does not dump $test");
		}
	}
}

#########################################
# Stop the database instance, which will be removed at the end of the tests.

$node->stop('fast');
