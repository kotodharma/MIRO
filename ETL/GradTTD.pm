package MIRO::ETL::GradTTD;

use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

use MIRO::Term;
require MIRO::ETL::Common;

extends 'MIRO::ETL';

###############################################################
##
###############################################################
sub default_table {
    "miro_grad_ttd"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'coh_year',
      'cat_sem_yr_iro',
      'grad_sem_yr_iro',
      ['&', '_get_grad_sem_parts', qw(grad_sem_yr_iro)],  ## NB: this is a wrapper unique to this class!
      ['&', '_get_fiscal_year', qw(grad_sem_yr_iro)],
      'id_number',
      'gender',
      'age',
      'leave_ind',
      'tot_cred',
      'man_cred',
      'trans_cred',
      'residency',
      ['&', '_get_res_miro', qw(residency)],
      ['&', '_get_cit_type', qw(citizenship_type)],
      ['&', '_get_geographic', qw(residency citizenship_type)],
      ['&', '_get_race_ethn_subset1', qw(ethnicity citizenship_type)],
      'fed_race',
      'college',
      'major',
      'major_desc',
      'outcome',
      'outcome_type',
      # 'outcome_seq_num',
      'regfss',
      'regfs',
      'totfs',
      ['&', '_get_enrolled_rate', qw(regfs totfs)],
      'ttd'
    ];
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub cat_sem_yr_iro {
    my $self = shift;
    ## What's coming in is really a banner term code, so convert
    MIRO::Term->new(code => shift)->convert_to_sem_yr;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub grad_sem_yr_iro {
    my $self = shift;
    ## What's coming in is really a banner term code, so convert
    MIRO::Term->new(code => shift)->convert_to_sem_yr;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub outcome_type {
    my $self = shift;
    my $type = shift;
    $type eq 'Master' ? 'Masters' : $type;
}

###############################################################
##
###############################################################
sub _get_grad_sem_parts {
    my $self = shift;
    my %Data = @_;
    my $grad = $Data{grad_sem_yr_iro};
    ## By this point, grad_sem_yr_iro has already been converted
    return $self->_get_sem_yr_parts(sem_yr_iro => $grad);
}

###############################################################
##
###############################################################
sub _get_fiscal_year {
    my $self = shift;
    my %Data = @_;
    my $grad = $Data{grad_sem_yr_iro};
    ## By this point, grad_sem_yr_iro has already been converted
    return ('fiscal_yr',
           MIRO::Term->new(code => $grad)->convert_to_fiscal_yr);
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub leave_ind {
    my $self = shift;
    $self->boolean(shift);
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub trans_cred {
    my $self = shift;
    int(shift);  ## make sure it's an integer
}

###############################################################
##
###############################################################
sub _get_enrolled_rate {
    my $self = shift;
    my %Data = @_;
    my $rate = int($Data{totfs}) == 0 ? 0 : int($Data{regfs}) / int($Data{totfs});
    return ('enroll_rate', sprintf "%.2f", $rate);
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table g
           join miro_base b
             on g.id_number = b.id_number
            and g.cat_sem_yr_iro = b.sem_yr_iro
        set g.coh_base_fk = b.skid,
            g.person_uid = b.person_uid ;

        update $table g
           join miro_degree d
             on g.person_uid = d.person_uid
            and g.grad_sem_yr_iro = d.sem_yr_iro
            and g.outcome = d.outcome
            and g.major = d.major
        set g.degree_fk = d.skid ;
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::GradTTD - ETL subclass for Graduate (OGE) time-to-degree data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::GradTTD; MIRO::ETL::GradTTD->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::GradTTD;\
               MIRO::ETL::GradTTD->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
