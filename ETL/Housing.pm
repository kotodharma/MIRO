package MIRO::ETL::Housing;

use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

require MIRO::ETL::Common;

extends 'MIRO::ETL';

###############################################################
##
###############################################################
sub default_table {
    "miro_housing"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'id_number',
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'building',
      ## 'building_desc'
    ];
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table h
           join miro_base b
             on b.id_number = h.id_number
            and b.sem_yr_iro = h.sem_yr_iro
        set h.base_fk = b.skid,
            h.person_uid = b.person_uid;
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::Housing - ETL subclass for undergraduate on-campus housing

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::Housing; MIRO::ETL::Housing->run();' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Housing;\
               MIRO::ETL::Housing->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
