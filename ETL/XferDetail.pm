package MIRO::ETL::XferDetail;

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
    "miro_xfer_detail"
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
      'vpdi',
      'acad_lvl_iro',
      'class_lvl_iro',
      'four_yr_history',
      'associates_awarded_type',
      'certificate_awarded_type',
      'final_trans_inst',
      ['&', '_get_trans_type', qw(final_trans_type final_trans_inst)],
      'institution_name',
    ];
}

###############################################################
##
###############################################################
sub associates_awarded_type {
    my $self = shift;
    shift || 'None';   ## just replace nulls with 'None'
}

###############################################################
##
###############################################################
sub certificate_awarded_type {
    my $self = shift;
    shift || 'None';   ## just replace nulls with 'None'
}

###############################################################
##
###############################################################
sub four_yr_history {
    my $self = shift;
    shift || 'None';   ## just replace nulls with 'None'
}

###############################################################
##
###############################################################
sub _get_trans_type {
    my $self = shift;
    my %Data = @_;
    my $type = $Data{final_trans_type};
    my $inst = uc $Data{final_trans_inst};
    my %is_a_CC = map { $_ => 1 } qw(HAW HON KAP KAU LEE MAU WIN);

    if ($type eq 'UH Campus') {
        $type = $is_a_CC{$inst} ? 'UH CC campus' : 'UH 4Y campus';
    }
    elsif ($type =~ /^U\.?S[. ]/) {
        $type = 'Other USA & related';
    }
    return ('final_trans_type', $self->wrap_quotes($type),
            'man_trans',        $self->boolean(int($inst eq 'MAN')));
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table x
           join miro_base b
             on b.person_uid = x.person_uid
            and b.sem_yr_iro = x.sem_yr_iro
        set x.base_fk = b.skid
        where x.base_fk = 0
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::XferDetail - ETL subclass for undergrad transfer details

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::XferDetail; MIRO::ETL::XferDetail->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::XferDetail;\
               MIRO::ETL::XferDetail->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
