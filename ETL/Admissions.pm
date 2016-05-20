package MIRO::ETL::Admissions;

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
    "miro_admissions"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'person_uid',
      'id_number',
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'admitted',
      'enrolled',
      'age',
      'gender',
      'styp_adm',
      'acad_lvl_iro',
      'class_lvl_iro',
      ['&', '_get_ed_lvl_iro', qw(edlvl_iro)],
      ['&', '_get_cohort_type', qw(styp_adm edlvl_iro)],
      'major_orgstr1_iro',
      'major_orgstr2_iro',
      'major_orgstr3_iro',
      'major',
      'major_desc',
      'program_desc',
      'department',
      'degree',
      'residency',
      'residency_desc',
      'paddr_state',
      'act_c',
      'act_e',
      'act_m',
      'act_w',
      'sat_c',
      'sat_m',
      'sat_r',
      'sat_w',
      'hs_gpa',     ### Renamed from HIGH_SCH_GPR at pull stage
      ['&', '_get_hs_type', qw(high_sch_type_iro)],
      ['&', '_get_m_major', qw(major major_desc)],
      ['&', '_get_cit_type', qw(citizenship_type)],
      'nation_of_citizenship_desc',
      ['&', '_get_uhm_race_ethn',
        qw(hawaiian_iro
           ethnicity
           ethnicity_desc
           ipeds_race_category
           ipeds_race_category_desc
           citizenship_type)],
      ['&', '_get_res_miro', qw(residency residency_desc)],
      ['&', '_get_geographic', qw(citizenship_type residency)],
      ['&', '_get_m_college', qw(m_major major_orgstr1_iro major_orgstr2_iro class_lvl_iro)],
      ['&', '_get_m_dept', qw(m_major department major_orgstr2_iro sem_yr_iro)],
      ['&', '_get_conv_sat_scores', qw(act_c act_e act_m act_w)],
    ];
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
sub paddr_state {
    my $self = shift;
    shift || 'XN';   ## just replace nulls with 'XN'

}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub hs_gpa {
    my $self = shift;
    my $gpa = shift;
    ($gpa < 1 || $gpa > 5) ? 'null' : $gpa;
}

###############################################################
##
###############################################################
sub _get_cohort_type {
    my $self = shift;
    my %Data = @_;
    my $styp = $Data{styp_adm};
    my $edl = $MIRO::ETL::State{ed_lvl_iro};  ## Already cleaned
    my %Edl = qw(FR Freshman SO Sophomore JR Junior SR Senior);
    my $type;

    if ($edl eq 'FP') {
        $type = 'First Professional';
    }
    elsif ($edl eq 'M' || $edl eq 'D' || $edl eq 'GS') {
        $type = 'Graduate';
    }
    elsif ($edl eq 'PU' || $edl eq 'UU') {
        $type = 'Unclassified';
    }
    elsif ($styp eq 'F') {
        $type = 'First-time Freshmen';
    }
    elsif ($styp eq 'T' && $Edl{$edl}) {
        $type = 'Transfer-'.$Edl{$edl};
    }
    else {
        $type = 'Other';
    }
    return ('m_cohort_type', $self->wrap_quotes($type));
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::Admissions - ETL subclass for admissions data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::Admissions; MIRO::ETL::Admissions->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Admissions;\
               MIRO::ETL::Admissions->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
