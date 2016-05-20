package MIRO::ETL::Courses;

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
    "miro_course"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'sem_yr_iro',
      'semester',
      'subject',
      'subject_desc',
      'crs_lvl',
      'course_number',
      'section_number',
      'cls_size_ind_iro',
      'regs_adj_iro',
      'crs_orgstr1_iro',
      'crs_orgstr2_iro',
      'crs_orgstr3_iro',
      'crs_orgstr4_iro',
      'department',
      'department_desc',
      'division',
      'division_desc',
    ];
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub cls_size_ind_iro {
    my $self = shift;
    return $self->boolean(@_);
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::Courses - ETL subclass for course data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::Courses; MIRO::ETL::Courses->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table,
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Courses;\
               MIRO::ETL::Courses->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
