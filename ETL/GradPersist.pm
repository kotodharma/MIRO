package MIRO::ETL::GradPersist;

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
    "miro_grad_persist"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'coh_year',
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'id_number',
      'gender',
      'styp',
      'ft_pt',
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
      'outcome_seq_num',
      'grad_sem_yr_iro',
      ['*', '_each_bit', qw(
          pgm_grad_1
          pgm_grad_2
          pgm_grad_3
          pgm_grad_4
          pgm_grad_5
          pgm_grad_6
          pgm_grad_7
          pgm_grad_8
          pgm_grad_9
          pgm_grad_10
          oge_grad_1
          oge_grad_2
          oge_grad_3
          oge_grad_4
          oge_grad_5
          oge_grad_6
          oge_grad_7
          oge_grad_8
          oge_grad_9
          oge_grad_10
          pgm_ret_1
          pgm_ret_2
          pgm_ret_3
          pgm_ret_4
          pgm_ret_5
          pgm_ret_6
          pgm_ret_7
          pgm_ret_8
          pgm_ret_9
          pgm_ret_10
          oge_ret_1
          oge_ret_2
          oge_ret_3
          oge_ret_4
          oge_ret_5
          oge_ret_6
          oge_ret_7
          oge_ret_8
          oge_ret_9
          oge_ret_10
      )]
    ];
}

###############################################################
##
###############################################################
sub _each_bit {
    my $self = shift;
    my($col, $val) = @_;
    if (my $cohyr = $MIRO::ETL::State{coh_year}) {
        my($t, $n);
        if ($col =~ /^(pgm|oge)_(grad|ret)_(\d+)$/) {
            $t = $2;
            $n = $3;
        }
        else {
            croak "Bad column name |$col|";
        }
        ## Do something with this coh_yr?
        ## Damn... we'll need to know the current/latest semester :=\
    }
    return ($col, $self->boolean($val));
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub sem_yr_iro {
    my $self = shift;
    ## What's coming in is really a banner term code, so convert
    MIRO::Term->new(code => shift)->convert_to_sem_yr;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub grad_sem_yr_iro {
    my $self = shift;
    my $bcode = shift;
    return 'null' if not $bcode;
    ## What's coming in is really a banner term code, so convert
    MIRO::Term->new(code => $bcode)->convert_to_sem_yr;
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub ft_pt {
    my $self = shift;
    shift eq 'Y' ? 'FT' : 'PT';
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table gp
           join miro_base b
             on b.id_number = gp.id_number
            and b.sem_yr_iro = gp.sem_yr_iro
        set gp.coh_base_fk = b.skid,
            gp.person_uid = b.person_uid

        update $table gp
           join miro_degree d
             on gp.person_uid = d.person_uid
            and gp.grad_sem_yr_iro = d.sem_yr_iro
            and gp.outcome = d.outcome
            and gp.major = d.major
        set gp.degree_fk = d.skid

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null
        where coh_year = 2004;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null
        where coh_year = 2005;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null
        where coh_year = 2006;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null
        where coh_year = 2007;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null,
        pgm_ret_6 = null, oge_ret_6 = null
        where coh_year = 2008;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null,
        pgm_ret_6 = null, oge_ret_6 = null,
        pgm_ret_5 = null, oge_ret_5 = null
        where coh_year = 2009;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null,
        pgm_ret_6 = null, oge_ret_6 = null,
        pgm_ret_5 = null, oge_ret_5 = null,
        pgm_ret_4 = null, oge_ret_4 = null
        where coh_year = 2010;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null,
        pgm_ret_6 = null, oge_ret_6 = null,
        pgm_ret_5 = null, oge_ret_5 = null,
        pgm_ret_4 = null, oge_ret_4 = null,
        pgm_ret_3 = null, oge_ret_3 = null
        where coh_year = 2011;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null,
        pgm_ret_6 = null, oge_ret_6 = null,
        pgm_ret_5 = null, oge_ret_5 = null,
        pgm_ret_4 = null, oge_ret_4 = null,
        pgm_ret_3 = null, oge_ret_3 = null,
        pgm_ret_2 = null, oge_ret_2 = null
        where coh_year = 2012;

        update $table set
        pgm_ret_10 = null, oge_ret_10 = null,
        pgm_ret_9 = null, oge_ret_9 = null,
        pgm_ret_8 = null, oge_ret_8 = null,
        pgm_ret_7 = null, oge_ret_7 = null,
        pgm_ret_6 = null, oge_ret_6 = null,
        pgm_ret_5 = null, oge_ret_5 = null,
        pgm_ret_4 = null, oge_ret_4 = null,
        pgm_ret_3 = null, oge_ret_3 = null,
        pgm_ret_2 = null, oge_ret_2 = null,
        pgm_ret_1 = null, oge_ret_1 = null
        where coh_year = 2013;

        delete from $table where coh_year = 2014;
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::GradPersist - ETL subclass for Graduate (OGE) retention and graduation data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::GradPersist; MIRO::ETL::GradPersist->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::GradPersist;\
               MIRO::ETL::GradPersist->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
