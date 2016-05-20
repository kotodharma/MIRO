package MIRO::ETL::NSSE2015Sat;

use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

###
### NOTE: This class extends another ETL subclass!
###
extends 'MIRO::ETL::NSSE2015Answers';

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [ ## Most of the following are columns in the source, but are converted
      ## into row data by the single_row_insert() method below! -dji
      'id_number',
      ['&', '_get_category'],
      ['&', '_get_ed_lvl_irclass', qw(irclass)],
      'evalexp',
      'sameinst',
    ];
}

###############################################################
##
###############################################################
sub _get_category {
    my $self = shift;
    return ('category', 'Satisfac');
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::NSSE2015Sat - ETL subclass for NSSE Overall Satisfaction data for 2015

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::NSSE2015Sat; MIRO::ETL::NSSE2015Sat->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::NSSE2015Sat;\
               MIRO::ETL::NSSE2015Sat->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
