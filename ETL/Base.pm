package MIRO::ETL::Base;

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
    "miro_base"
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
      ['&', '_get_acyr_fiscal', qw(sem_yr_iro)],
      'age',
      'gender',
      'pell_ind',
      'acad_lvl_iro',
      'class_lvl_iro',
      ['&', '_get_ed_lvl_iro', qw(edlvl_iro)],
      'ft_pt_stat',
      ['&', '_get_styp_miro', qw(styp_reg_iro)],
      'first_time_fr_iro',
      'cur_attempted_sh',
      'major_orgstr1_iro',
      'major_orgstr2_iro',
      'major_orgstr3_iro',
      'major',            ## name changed from major_iro at pull stage
      'major_desc',       ## name changed from major_desc_iro at pull stage
      'program_desc',
      'department',
      'degree',
      'residency',
      'residency_desc',
      'sat_c_s06',
      'sat_m_s02',
      'sat_r_satr',
      'sat_w_satw',
      ['&', '_get_hs_type', qw(high_sch_type_iro)],
      ['&', '_get_m_major', qw(major major_desc)],
      ['&', '_get_m_dept', qw(m_major department major_orgstr2_iro sem_yr_iro)],
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
      ['&', '_get_conv_sat_scores', qw(act_c_a05 act_w_wtg)],
    ];
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub acad_lvl_iro {
    my $self = shift;
    my($val) = @_;
    unless ($val) {
        ## Default to undergrad for no data (null). This also
        ## means that the student is home-based at a campus
        ## other than Manoa.
        $val = 'UG';
        $MIRO::ETL::State{non_MAN_based}++;
    }
    return $val;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub class_lvl_iro {
    my $self = shift;
    my($val) = @_;
    if ($MIRO::ETL::State{non_MAN_based} || !$val) {
        $val = 'UNCLS';
    }
    return $val;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    # and, convert crazy new code PNRU back to PNUR
    my $maj = $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
    return ($maj eq 'PRNU') ? 'PNUR' : $maj;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major_desc {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub department {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub degree {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub program_desc {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major_orgstr1_iro {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major_orgstr2_iro {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major_orgstr3_iro {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub pell_ind {
    my $self = shift;
    return $self->boolean(@_);
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub first_time_fr_iro {
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
          join departments d on t.department = d.dept_code
        set department_desc = d.dept_desc
        where department_desc is null ;

        /* Change the m_dept column from being a code to being a desc! */
        update $table t
          join departments d on t.m_dept = d.dept_code
        set m_dept = d.dept_desc ;
EOT
}


__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::Base - ETL subclass for enrollment base data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::Base; MIRO::ETL::Base->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Base; MIRO::ETL::Base->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
