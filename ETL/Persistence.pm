package MIRO::ETL::Persistence;

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
    "miro_persistence"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'person_uid',
      ['&', '_get_cohort_type', qw(cohort_id cohort_edlvl)],
      'trk_outcome',
      'trk_outcome_semyr',
      'trk_outcome_number',
      'trk_outcome_major_desc',
      'ret_1f',
      'ret_1yr',
      'ret_2f',
      'ret_2yr',
      'ret_3f',
      'ret_3yr',
      'ret_4f',
      'ret_4yr',
      'ret_5f',
      'ret_5yr',
      'ret_6f',
      'ret_6yr',
      'ret_7f',
      'ret_7yr',
      'ret_8f',
      'ret_8yr',
      'grad_1f',
      'grad_1yr',
      'grad_2f',
      'grad_2yr',
      'grad_3f',
      'grad_3yr',
      'grad_4f',
      'grad_4yr',
      'grad_5f',
      'grad_5yr',
      'grad_6f',
      'grad_6yr',
      'grad_7f',
      'grad_7yr',
      'grad_8f',
      'grad_8yr',
    ];
}

###############################################################
##
###############################################################
sub _get_cohort_type {
    my $self = shift;
    my %Data = @_;
    my %Edl = qw(FR Freshman SO Sophomore JR Junior SR Senior);

    my $cid = $Data{cohort_id};
    my $cel = $Data{cohort_edlvl};
    my $ct = 'Unknown';  ## Default is hopefully never seen...

    if ($cid eq 'FTF') {
        $ct = 'First-time Freshmen';
    }
    elsif ($cid eq 'TFS') {
        my $edl = $Edl{$cel} || "$cel??";
        $ct = "Transfer-$edl";
    }
    return ('m_cohort_type', $self->wrap_quotes($ct));
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table p
           join miro_base b
             on b.person_uid = p.person_uid
            and b.sem_yr_iro = p.sem_yr_iro
        set p.base_fk = b.skid ;

        update $table p
           join miro_xfer_detail x
             on x.person_uid = p.person_uid
            and x.sem_yr_iro = p.sem_yr_iro
        set p.xfer_detail_fk = x.skid ;

        update $table p
           join miro_degree d
             on p.person_uid = d.person_uid
            and p.trk_outcome = d.outcome
            and p.trk_outcome_semyr = d.sem_yr_iro
            and p.trk_outcome_number = d.outcome_number
        set p.degree_fk = d.skid ;
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::Persistence - ETL subclass for retention and graduation data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::Persistence; MIRO::ETL::Persistence->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table,
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Persistence;\
               MIRO::ETL::Persistence->new("table"=>"test.foo")->run;' <csv tab-delim input file>

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
