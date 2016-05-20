package MIRO::ETL::Degree;

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
    "miro_degree"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'person_uid',
      'age',
      'gender',
      'all_completed_sh',
      ['&', '_get_man_completed_sh', qw(all_completed_sh cum_trans_total)],
      'cum_gpr',
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'acyr',
      'fiscal_yr_iro',
      'hawaiian_iro',
      'outcome',
      'outcome_honor_desc',
      'outcome_number',
      'department',
      'program_desc',
      'major',
      'major_desc',
      'major_orgstr1_iro',
      'major_orgstr2_iro',
      'major_orgstr3_iro',
      ['&', '_get_acad_lvl', qw(acad_lvl_iro outcome)],
      ['&', '_get_cit_type', qw(citizenship_type)],
      'nation_of_citizenship_desc',
      ['&', '_get_outcome_type', qw(outcome)],
      ['&', '_get_uhm_race_ethn', qw(hawaiian_iro
                                     ethnicity
                                     ethnicity_desc
                                     ipeds_race_category
                                     ipeds_race_category_desc
                                     citizenship_type)],
      ['&', '_get_res_miro', qw(residency residency_desc)],
      ['&', '_get_geographic', qw(citizenship_type residency)],
      ['&', '_get_m_college', qw(major major_orgstr1_iro major_orgstr2_iro)],
      ['&', '_get_m_dept', qw(major department major_orgstr2_iro sem_yr_iro)],
    ];
}

###############################################################
##
###############################################################
sub _get_acad_lvl {
    my $self = shift;
    my %Data = @_;
    my $acad = $Data{outcome} eq 'ARCHD' ? 'GR' : $Data{acad_lvl_iro};
    return ('acad_lvl_iro', $self->wrap_quotes($acad));
}

###############################################################
##
###############################################################
sub _get_outcome_type {
    my $self = shift;
    my %Data = @_;

    ## The content of these categories can (and will) change over time;
    ## if this subroutine hasn't been changed in quite a while, it would be
    ## good to consult IRAO's Master List of Curricula for updates. -dji
    ##
    my %Bachelors = map { $_ => 1 } qw();  # only the ones that are not /^B/, currently none
    my %Masters = map { $_ => 1 } qw(LLM); # only the ones that are not /^M/
    my %RDoc = map { $_ => 1 } qw(PHD DRPH);
    my %PDoc = map { $_ => 1 } qw(MD JD EDD DNP DARCH ARCHD);
    my %Cert = map { $_ => 1 } qw(PCERT);

    my $out = uc $Data{outcome};

    if ($Cert{$out}) {
        $out = 'Certificate';
    }
    elsif ($RDoc{$out}) {
        $out = 'Research Doctorate';
    }
    elsif ($PDoc{$out}) {
        $out = 'Professional Doctorate';
    }
    elsif ($Masters{$out}) {
        $out = 'Masters';
    }
    elsif ($Bachelors{$out}) {
        $out = 'Bachelors';
    }
    elsif ($out =~ /^M/) {
        $out = 'Masters';
    }
    elsif ($out =~ /^B/) {
        $out = 'Bachelors';
    }
    else {
        $out = 'Unknown';
    }
    return ('m_outcome_type', $self->wrap_quotes($out));
}

###############################################################
##
###############################################################
sub _get_man_completed_sh {
    my $self = shift;
    my %Data = @_;
    my $man_sh = $Data{all_completed_sh} - $Data{cum_trans_total};
    return ('m_man_completed_sh', $man_sh);
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub major {
    my $self = shift;
    # Null out if student is home-based elsewhere than Manoa
    return $MIRO::ETL::State{non_MAN_based} ? 'null' : shift;
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

MIRO::ETL::Degree - ETL subclass for degree (outcome) data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::Degree; MIRO::ETL::Degree->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Degree;\
               MIRO::ETL::Degree->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
