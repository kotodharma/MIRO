package MIRO::ETL::BaseMajor;

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
    "miro_base_major"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'person_uid',
      'sem_yr_iro',
      'academic_period',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'curriculum_priority',
      'primary_program_ind',
      'acad_lvl_iro',
      'degree',
      'degree_award_category',
      'degree_award_category_desc',
      'major',            ## name changed from major_iro at pull stage
      'major_desc',       ## name changed from major_desc_iro at pull stage
      'program',
      'program_desc',
      ['&', '_get_m_major', qw(major major_desc)],
      ## Computed fields deliberately put before the columns major_orgstr* & department that
      ## they make use of, because we don't want to see those fields set to 'null'. -dji
      ['&', '_get_m_college', qw(m_major major major_orgstr1_iro major_orgstr2_iro class_lvl_iro)],
      ['&', '_get_m_dept', qw(m_major department major_orgstr2_iro sem_yr_iro)],
      'major_orgstr1_iro',
      'major_orgstr2_iro',
      'major_orgstr3_iro',
      'department',
      'department_desc',
    ];
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub acad_lvl_iro {
    my $self = shift;
    my($val) = @_;
    ## I dunno whose hair-brained idea it was that law and medicine
    ## are not examples of graduate study. IRO_BASE has them as GR!
    ## We fixin it. -dji
    return ($val eq 'LW' || $val eq 'MD') ? 'GR' : $val;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major {
    my $self = shift;
    my($val) = @_;
    ## Convert crazy new code PNRU back to PNUR
    return ($val eq 'PRNU') ? 'PNUR' : $val;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub primary_program_ind {
    my $self = shift;
    return $self->boolean(@_);
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table t
           join miro_base b
             on b.person_uid = t.person_uid
            and b.sem_yr_iro = t.sem_yr_iro
        set base_fk = b.skid
        where base_fk is null ;
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::BaseMajor - ETL subclass for all declared majors

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::BaseMajor; MIRO::ETL::BaseMajor->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::BaseMajor;\
               MIRO::ETL::BaseMajor->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
