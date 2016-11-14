use strict;
use warnings;
use Test::More tests => 42;
use Scalar::Util qw(blessed);

use pg_lsn qw(parse_lsn);

ok(!defined(parse_lsn('')), 'parse_lsn of empty string is undef');
ok(!defined(parse_lsn(undef)), 'parse_lsn of undef is undef');

my $zero_lsn = parse_lsn('0/0');
ok(blessed($zero_lsn), 'zero lsn blessed');
ok($zero_lsn->isa("pg_lsn"), 'zero lsn isa pg_lsn');
is($zero_lsn->{'_high'}, 0, 'zero lsn high word zero');
is($zero_lsn->{'_low'}, 0, 'zero lsn low word zero');
cmp_ok($zero_lsn, "==", pg_lsn->new_num(0, 0), 'parse_lsn of 0/0');

cmp_ok(parse_lsn('0/FFFFFFFF'), "==", pg_lsn->new_num(0, 0xFFFFFFFF), 'parse_lsn of 0/FFFFFFFF');
cmp_ok(parse_lsn('FFFFFFFF/0'), "==", pg_lsn->new_num(0xFFFFFFFF, 0), 'parse_lsn of FFFFFFFF/0');
cmp_ok(parse_lsn('FFFFFFFF/FFFFFFFF'), "==", pg_lsn->new_num(0xFFFFFFFF, 0xFFFFFFFF), 'parse_lsn of 0xFFFFFFFF/0xFFFFFFFF');

is(parse_lsn('2/2') <=> parse_lsn('2/3'), -1);
is(parse_lsn('2/2') <=> parse_lsn('2/2'), 0);
is(parse_lsn('2/2') <=> parse_lsn('2/1'), 1);
is(parse_lsn('2/2') <=> parse_lsn('3/2'), -1);
is(parse_lsn('2/2') <=> parse_lsn('1/2'), 1);

cmp_ok(parse_lsn('0/1'), "==", parse_lsn('0/1'));
ok(!(parse_lsn('0/1') == parse_lsn('0/2')), "! 0/1 == 0/2");
ok(!(parse_lsn('0/1') == parse_lsn('0/0')), "! 0/1 == 0/0");
cmp_ok(parse_lsn('1/0'), "==", parse_lsn('1/0'));
cmp_ok(parse_lsn('1/0'), "!=", parse_lsn('1/1'));
cmp_ok(parse_lsn('1/0'), "!=", parse_lsn('2/0'));
cmp_ok(parse_lsn('1/0'), "!=", parse_lsn('0/0'));
cmp_ok(parse_lsn('1/0'), "!=", parse_lsn('0/1'));

cmp_ok(parse_lsn('0/1'), ">=", parse_lsn('0/1'));
cmp_ok(parse_lsn('0/1'), "<=", parse_lsn('0/1'));
cmp_ok(parse_lsn('0/1'), "<=", parse_lsn('0/2'));
cmp_ok(parse_lsn('0/1'), ">=", parse_lsn('0/0'));
cmp_ok(parse_lsn('1/0'), ">=", parse_lsn('1/0'));
cmp_ok(parse_lsn('1/0'), "<=", parse_lsn('1/0'));
cmp_ok(parse_lsn('1/0'), "<=", parse_lsn('2/0'));
cmp_ok(parse_lsn('1/0'), ">=", parse_lsn('0/0'));
cmp_ok(parse_lsn('1/1'), ">=", parse_lsn('1/1'));
cmp_ok(parse_lsn('1/1'), "<=", parse_lsn('1/1'));
cmp_ok(parse_lsn('1/1'), "<=", parse_lsn('1/2'));
cmp_ok(parse_lsn('1/2'), ">=", parse_lsn('1/1'));

ok(parse_lsn('1/1'), 'bool conversion');
ok(! $zero_lsn, 'bool negation');

# implicit string conversions
cmp_ok(parse_lsn('0/0'), "==", "0/0");
cmp_ok(parse_lsn('FFFFFFFF/FFFFFFFF'), "==", "FFFFFFFF/FFFFFFFF");
# swapped string conversions
cmp_ok("0/0", "==", parse_lsn('0/0'));
cmp_ok("FFFFFFFF/FFFFFFFF", "==", parse_lsn('FFFFFFFF/FFFFFFFF'));

# negation makes no sense for a uint64
eval {
	- parse_lsn('0/1');
};
if ($@) {
	ok('negation raises error');
} else {
	fail('negation did not raise error');
}
