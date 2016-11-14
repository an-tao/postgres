package pg_lsn;

use strict;
use warnings;

our (@ISA, @EXPORT_OK);
BEGIN {
	require Exporter;
	@ISA = qw(Exporter);
	@EXPORT_OK = qw(parse_lsn);
}

use Scalar::Util qw(blessed looks_like_number);
use Carp;

use overload
	'""' => \&Str,
	'<=>' => \&NumCmp,
	'bool' => \&Bool,
	'-' => \&Negate,
	fallback => 1;

=pod package pg_lsn

A class to encapsulate a PostgreSQL log-sequence number (LSN) and handle conversion
of its hex representation.

Provides equality and inequality operators.

Calling 'new' on undef or empty string argument returns undef, not an instance.

=cut

sub new_num
{
	my ($class, $high, $low) = @_;
	my $self = bless { '_low' => $low, '_high' => $high } => $class;
	$self->_constraint;
	return $self;
}

sub new
{
	my ($class, $lsn_str) = @_;
	return undef if !defined($lsn_str) || $lsn_str eq '';
	my ($high, $low) = split('/', $lsn_str, 2);
	die "malformed LSN" if ($high eq '' || $low eq '');
	return $class->new_num(hex($high), hex($low));
}

sub NumCmp
{
	my ($self, $other, $swap) = @_;
	$self->_constraint;
	die "comparison with undef" unless defined($other);
	if (!blessed($other))
	{
		# coerce from string if needed. Try to coerce any non-object.
		$other = pg_lsn->new($other) if !blessed($other);
	}
	$other->_constraint;
	# and compare
	my $ret;
	if ($self->{'_high'} < $other->{'_high'})
	{
		$ret = -1;
	}
	elsif ($self->{'_high'} == $other->{'_high'})
	{
		if ($self->{'_low'} < $other->{'_low'})
		{
			$ret = -1;
		}
		elsif ($self->{'_low'} == $other->{'_low'})
		{
			$ret = 0;
		}
		else
		{
			$ret = 1;
		}
	}
	else
	{
		$ret = 1;
	}
	$ret = -$ret if $swap;
	return $ret;
}

sub _constraint
{
	my $self = shift;
	die "high word must be defined" unless (defined($self->{'_high'}));
	die "high word must be numeric" unless (looks_like_number($self->{'_high'}));
	die "high word must be in uint32 range" unless ($self->{'_high'} >= 0 && $self->{'_high'} <= 0xFFFFFFFF);
	die "low word must be defined" unless (defined($self->{'_low'}));
	die "low word must be numeric" unless (looks_like_number($self->{'_low'}));
	die "low word must be in uint32 range" unless ($self->{'_low'} >= 0 && $self->{'_low'} <= 0xFFFFFFFF);
}

sub Bool
{
	my $self = shift;
	$self->_constraint;
	return $self->{'_high'} || $self->{'_low'};
}

sub Negate
{
	die "cannot negate pg_lsn";
}

sub Str
{
	my $self = shift;
	return sprintf("%X/%X", $self->high, $self->low);
}

sub high
{
	my $self = shift;
	return $self->{'_high'};
}

sub low
{
	my $self = shift;
	return $self->{'_low'};
}

# Todo: addition/subtraction. Needs to handle wraparound and carrying.

=pod parse_lsn(lsn)

Returns a 2-array of the high and low words of the passed LSN as numbers,
or undef if argument is the empty string or undef.

=cut 

sub parse_lsn
{
	return pg_lsn->new($_[0]);
}
