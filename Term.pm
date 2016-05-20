package MIRO::Term;

use strict;    ## remove this line upon penalty of death :=|
use Moose;
use Carp;
use namespace::autoclean;
use DateTime;

use overload '>' => \&_term_gt, '<' => \&_term_lt, '<=>' => \&_spaceship;

my $bannercode_format = qr/^([12]\d\d\d)([1-4])([0-5])$/;
my $semyriro_format = qr/^([12]\d\d\d)-([158])$/;

has 'code' => ( is  => 'ro' );
has 'type' => ( is  => 'ro', builder => '_get_type', lazy => 1, init_arg => undef );
has 'name' => ( is  => 'ro', builder => '_get_name', lazy => 1, init_arg => undef );
has 'abbr' => ( is  => 'ro', builder => '_get_abbr', lazy => 1, init_arg => undef );
has 'year' => ( is  => 'ro', builder => '_get_year', lazy => 1, init_arg => undef );
has 'tcode' => ( is  => 'ro', builder => '_get_tcode', lazy => 1, init_arg => undef );
has 'entrydate' => ( is  => 'ro', builder => '_get_entrydate', lazy => 1, init_arg => undef );
has 'graddate' => ( is  => 'ro', builder => '_get_graddate', lazy => 1, init_arg => undef );

## Specialized terms (final digit not zero) don't need to specify boundary dates
##    in case they are the same as for the "normal" term (with final zero). Also,
##    entry and grad dates are inherited separately, so if only one is different
##    from normal term, you can specify only that one (cf. summer terms)
my %Terms = (
     8 => { name => 'Fall', abbr => 'F', entry_date => '08/15', grad_date => '12/30' },
    10 => { name => 'Fall', abbr => 'F', entry_date => '08/15', grad_date => '12/30' },
    11 => { name => 'Fall Apprenticeship', abbr => 'FApp' },
    13 => { name => 'Fall Extension', abbr => 'FExt' },
    15 => { name => 'Fall Accelerated', abbr => 'FAcc' },
    20 => { name => 'Winter', abbr => 'W', entry_date => '12/15', grad_date => '01/15' },
    23 => { name => 'Winter Extension', abbr => 'WExt' },
    25 => { name => 'Winter Accelerated', abbr => 'WAcc' },
     1 => { name => 'Spring', abbr => 'Sp', entry_date => '01/01', grad_date => '05/15' },
    30 => { name => 'Spring', abbr => 'Sp', entry_date => '01/01', grad_date => '05/15' },
    31 => { name => 'Spring Apprenticeship', abbr => 'SpApp' },
    33 => { name => 'Spring Extension', abbr => 'SpExt' },
    35 => { name => 'Spring Accelerated', abbr => 'SpAcc' },
     5 => { name => 'Summer', abbr => 'Su', entry_date => '05/15', grad_date => '08/15' },
    40 => { name => 'Summer', abbr => 'Su', entry_date => '05/15', grad_date => '08/15' },
    42 => { name => 'Summer I',  abbr => 'Su1', grad_date => '06/30' },
    43 => { name => 'Summer Extension',  abbr => 'SuExt' },
    44 => { name => 'Summer II', abbr => 'Su2', entry_date => '07/01' },
    45 => { name => 'Summer Accelerated',  abbr => 'SuAcc' },
);

###############################################################
##
###############################################################
sub BUILD {
    my $self = shift;
    my $args = shift;

    if ($self->code =~ $bannercode_format) {
        unless (exists $Terms{$2.$3}) {
            croak "Invalid Banner code = ".$self->code;
        }
    }
    elsif ($self->code !~ $semyriro_format) {
        croak "Unrecognized code = ".$self->code;
    }
}

###############################################################
##
###############################################################
sub _get_type {
    my $self = shift;
    $self->code =~ $bannercode_format && return 'banner';
    $self->code =~ $semyriro_format && return 'semyr';
}

###############################################################
##
###############################################################
sub _get_year {
    my $self = shift;
    $self->code =~ $bannercode_format && return $1;
    $self->code =~ $semyriro_format && return $1;
}

###############################################################
##
###############################################################
sub _get_tcode {
    my $self = shift;
    $self->code =~ $bannercode_format && return $2.$3;
    $self->code =~ $semyriro_format && return $2;
}

###############################################################
##
###############################################################
sub _get_entrydate {
    my $self = shift;
    my $code = $self->tcode;
    my $calyear = ($code >= 30) ? $self->year : ($self->year - 1);
    my $norm_term = $code - ($code % 10);
    my $e_date = $Terms{$code}{entry_date} || $Terms{$norm_term}{entry_date};
    my($m, $d) = split /[.\/-]/, $e_date;
    return new DateTime( year => $calyear, month => $m, day => $d );
}

###############################################################
##
###############################################################
sub _get_graddate {
    my $self = shift;
    my $code = $self->tcode;
    ##                 yes, 20, not 30!
    my $calyear = ($code >= 20) ? $self->year : ($self->year - 1);
    my $norm_term = $code - ($code % 10);
    my $g_date = $Terms{$code}{grad_date} || $Terms{$norm_term}{grad_date};
    my($m, $d) = split /[.\/-]/, $g_date;
    return new DateTime( year => $calyear, month => $m, day => $d );
}

###############################################################
## Class method
###############################################################
sub get_delta_days {
    my $self = shift;
    my($start, $end, $type) = @_;
    unless (ref($start) eq __PACKAGE__ && ref($end) eq __PACKAGE__) {
        croak "getGradTime: Parameter not of correct type.";
    }
    unless (_term_gte($end, $start)) {
    ### change this comparison back to operator overload syntax if/when it works?
        croak "getGradTime: Entry term comes first, grad term second.";
    }
    my $enddate = $type eq 's' ? $end->entrydate : $end->graddate;
    my $dur = $start->entrydate->delta_days($enddate);
    return $dur->in_units('days');
}

###############################################################
##
###############################################################
sub _term_gte {
    my($self, $other, $swap) = @_;
    return ($self->entrydate >= $other->entrydate);
}

###############################################################
##
###############################################################
sub _term_gt {
    my($self, $other, $swap) = @_;
    return ($self->entrydate > $other->entrydate);
}

###############################################################
##
###############################################################
sub _term_lt {
    my($self, $other, $swap) = @_;
    return ($self->entrydate < $other->entrydate);
}

###############################################################
##
###############################################################
sub _spaceship {
    my($self, $other, $swap) = @_;
    return DateTime->compare($self->entrydate , $other->entrydate);
}

###############################################################
##
###############################################################
sub convert_to_sem_yr {
    my $self = shift;
    my $term = $self->code;
    if ($self->type eq 'semyr') {
        return $term;
    }
    my %Codes = (1 => 8, 2 => 1, 3 => 1, 4 => 5, 35 => 5, 45 => 8);

    my $y = $self->year;
    my $tcode = $self->tcode;
    my $tens = int($tcode / 10);
    my $sem = $Codes{$tcode} || $Codes{$tens};
    croak "Conversion of term=$term failed" unless $sem;
    $y = $y-1 if $tens == 1;
    return "$y-$sem";
}

###############################################################
##
###############################################################
sub convert_to_fiscal_yr {
    my $self = shift;
    my $semyr = $self->convert_to_sem_yr;
    $semyr =~ $semyriro_format || croak "Match of semyr=$semyr failed";
    my($y, $s) = ($1, $2);
    return ($s < 5) ? $y : $y+1;
}

__PACKAGE__->meta->make_immutable;
1;
__END__

=pod

=head1 NAME

MIRO::Term - Academic term object

=head1 SYNOPSIS

    use MIRO::Term;
    my $e = new MIRO::Term(code => "200910");   # entry/catalog term
    my $g = new MIRO::Term(code => "201042");   # graduation term

    printf "TTD is %d days\n", MIRO::Term->get_delta_days($e, $g);  # 684
    printf "Entry sem_yr is %s\n", $e->convert_to_sem_yr;           # 2008-8
    printf "Grad fiscal year is %s\n", $g->convert_to_fiscal_yr;    # 2011

=head1 DESCRIPTION

All-purpose class for mangling Banner terms and ODS-kine SEM_YR_IRO values.
Code argument can be either type. For a run-down of many of the relationships
and assumptions that are built into this class, hopefully you can still find
the following document on the miro$ share drive: MIRO Office > date convert

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
