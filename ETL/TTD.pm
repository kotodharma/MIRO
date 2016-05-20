package MIRO::ETL::TTD;

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
    "miro_ttd"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'person_uid',
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'fiscal_yr_iro',
      'outcome_number',
      'entry_term',      ## name changed from first_term_acad_hist_camp at pull stage
      'acad_lvl_entry',
      ['&', '_get_ed_lvl_entry', qw(edlvl_entry)],
      'ft_fr_entry',
      ['&', '_get_cohort_type', qw(ft_fr_entry edlvl_entry)],
      'ft_pt_entry',
      'major_entry',
      'program_entry',
      'trans_cr_entry',
      'ttd',
      'lag_yr',
      'lag_last_enr_yr',
      'sec_ba',           ## actually this is always null/false, determined by the pull query
      'more_than_1_deg'   ## actually this is always null/false, determined by the pull query
    ];
}

###############################################################
## Mostly just a way to change the name to include the underscore
###############################################################
sub _get_ed_lvl_entry {
    my $self = shift;
    my %Data = @_;
    my $edlvl = $Data{edlvl_entry} || 'XN';
    return ('ed_lvl_entry', $self->wrap_quotes($edlvl));
}

###############################################################
##
###############################################################
sub _get_cohort_type {
    my $self = shift;
    my %Data = @_;
    my %Edl = qw(FR Freshman SO Sophomore JR Junior SR Senior);

    ## Note: $ftf is a bool value, bec. it's already been converted
    my $ftf = $Data{ft_fr_entry};
    my $ele = $Data{edlvl_entry};
    my $mct;

    if ($ftf) {
        $mct = ($ele eq 'FR') ? 'First-time Freshmen' : 'Other';
    }
    else {
        my $edl = $Edl{$ele} || "$ele??";
        $mct = "Transfer-$edl";
    }
    return ('m_cohort_type', $self->wrap_quotes($mct));
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub entry_term {
    my $self = shift;
    MIRO::Term->new(code => shift)->convert_to_sem_yr;
}

###############################################################
## At present, need to handle null entries. Maybe not forever.
###############################################################
sub ft_pt_entry {
    my $self = shift;
    my $ftpt = shift;
    ($ftpt || 'XN');
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub ft_fr_entry {
    my $self = shift;
    $self->boolean(shift);
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub sec_ba {
    my $self = shift;
    $self->boolean(shift, 'null is false');
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub more_than_1_deg {
    my $self = shift;
    $self->boolean(shift, 'null is false');
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table t
          join miro_degree d
               on t.person_uid = d.person_uid
              and t.sem_yr_iro = d.sem_yr_iro
              and t.outcome_number = d.outcome_number
        set t.degree_fk = d.skid
        where t.degree_fk is null
        ;

        update $table t
          join miro_base b
               on b.person_uid = t.person_uid
              and b.sem_yr_iro = t.entry_term
        set t.base_entry_fk = b.skid
        where t.base_entry_fk is null
        ;

        -- if no enrollment for t.entry_term, look for the next term after
        update $table t
          join miro_base b
               on b.person_uid = t.person_uid
              and b.sem_yr_iro = incr_term(t.entry_term)
        set t.base_entry_fk = b.skid
        where t.base_entry_fk is null
        and t.entry_term >= '2003-8'
        ;

        -- if still no enrollment, look for two terms after
        update $table t
          join miro_base b
               on b.person_uid = t.person_uid
              and b.sem_yr_iro = incr_term(incr_term(t.entry_term))
        set t.base_entry_fk = b.skid
        where t.base_entry_fk is null
        and t.entry_term >= '2003-8'
        ;

        -- if still no enrollment, look for three terms after
        update $table t
          join miro_base b
               on b.person_uid = t.person_uid
              and b.sem_yr_iro = incr_term(incr_term(incr_term(t.entry_term)))
        set t.base_entry_fk = b.skid
        where t.base_entry_fk is null
        and t.entry_term >= '2003-8'
        ;

        call update_ttd_fields();
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::TTD - ETL subclass for undergraduate time-to-degree data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::TTD; MIRO::ETL::TTD->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::TTD;\
               MIRO::ETL::TTD->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
