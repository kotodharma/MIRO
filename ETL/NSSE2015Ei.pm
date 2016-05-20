package MIRO::ETL::NSSE2015Ei;

##
##!
##! Dude, I think this needs to be revisited, and converted to use the Answers class as
##! its parent. Why doesn't it insert into the ed_lvl column? Ayasiki gozaru! -dji
##!
##

use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

extends 'MIRO::ETL';

my @ei_scores = qw(HO RI QR LS CL DD SF ET SE QI);

my @scale4 = qw(
    HOapply
    HOanalyze
    HOevaluate
    HOform
    RIintegrate
    RIsocietal
    RIdiverse
    RIownview
    RIperspect
    RInewview
    RIconnect
    QRconclude
    QRproblem
    QRevaluate
    LSreading
    LSnotes
    LSsummary
    CLaskhelp
    CLexplain
    CLstudy
    CLproject
    DDrace
    DDeconomic
    DDreligion
    DDpolitical
    SFcareer
    SFotherwork
    SFdiscuss
    SFperform
    ETgoals
    ETorganize
    ETexample
    ETdraftfb
    ETfeedback
    SEacademic
    SElearnsup
    SEdiverse
    SEsocial
    SEwellness
    SEnonacad
    SEactivities
    SEevents
);

my @scale7 = qw(
    QIstudent
    QIadvisor
    QIfaculty
    QIstaff
    QIadmin
);

###############################################################
##
###############################################################
sub default_table {
    "nsse_2015_answers"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'id_number',
      ['*', '_ei_score', @ei_scores],
      ['*', '_four_scale', @scale4],
      ['*', '_seven_scale', @scale7],
    ];
}

###############################################################
## Validate various numeric values, or empty
###############################################################
sub _ei_score {
    my $self = shift;
    my($col, $val) = @_;
    croak "Bad value for column $col" unless $val =~ /^([0-9.]+|)$/;
    $val = 'null' if $val eq '';
    return ($col, $val);
}

###############################################################
## Validate integer responses in range 1-4, or empty
###############################################################
sub _four_scale {
    my $self = shift;
    my($col, $val) = @_;
    croak "Bad value for column $col" unless $val =~ /^([1-4]|)$/;
    $val = 'null' if $val eq '';
    return ($col, $val);
}

###############################################################
## Validate integer responses in range 1-7, 9 (n/a), or empty
###############################################################
sub _seven_scale {
    my $self = shift;
    my($col, $val) = @_;
    croak "Bad value for column $col" unless $val =~ /^([1-79]|)$/;
    $val = 'null' if $val eq '';
    return ($col, $val);
}

###############################################################
## Override parent method. More single "record" than single row per se
###############################################################
sub single_row_insert {
    my $self = shift;
    my($cols, $vals, $add) = @_;
    my %Rec = map { $_ => shift(@{$vals}) } @{$cols};
    my $idn = delete $Rec{id_number};  ## delete so it is not treated as a question
    my $table = $self->table;
    my $out;

    foreach my $col (sort keys %Rec) {
        my $ei = uc(substr $col, 0, 2);
        my $v = $Rec{$col};
        if ($v ne 'null') {
            $v =~ s/'/''/g;    ## Escape single quotes
            $v = "'$v'";
        }
        substr($col, 0, 2) = $ei;  ## return first two chars to uppercase
        $out .= sprintf(" insert $table (id_number,category,question,answer)".
                        " values (%s,'%s','%s',%s);\n", $idn, $ei, $col, $v);
    }
    return $out;
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
      ## When adapting this class for later NSSE surveys, don't forget to
      ## change the hardcoded semester code below! -dji
    print <<EOT;
        update $table n
           join miro_base b
             on b.id_number = n.id_number
            and b.sem_yr_iro = '2015-1'
        set n.base_fk = b.skid,
            n.ed_lvl = case when b.ed_lvl_iro in ('FR','SO') then 'FR' else 'SR' end
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::NSSE2015Ei - ETL subclass for NSSE engagement indicator data for 2015

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::NSSE2015Ei; MIRO::ETL::NSSE2015Ei->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::NSSE2015Ei;\
               MIRO::ETL::NSSE2015Ei->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
