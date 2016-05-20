package MIRO::ETL::NSSE2015;

use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

extends 'MIRO::ETL';

###############################################################
##
###############################################################
sub default_table {
    "nsse_2015"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'id_number',
      ['*', '_ei_score', qw(HO RI QR LS CL DD SF ET SE QI)],
      ['*', '_four_scale', qw(
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
      )],
      ['*', '_seven_scale', qw(
        QIstudent
        QIadvisor
        QIfaculty
        QIstaff
        QIadmin
      )],
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
            n.ed_lvl = case when b.edlvl_iro in ('FR','SO') then 'FR' else 'SR' end
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::NSSE2015 - ETL subclass for NSSE survey results data for 2015

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::NSSE2015; MIRO::ETL::NSSE2015->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::NSSE2015;\
               MIRO::ETL::NSSE2015->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
