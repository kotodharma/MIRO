package MIRO::ETL::Major;

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
    "miro_major"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'base_fk',
      'person_uid',
      'sem_yr_iro',
      ['&', '_get_sem_yr_parts', qw(sem_yr_iro)],
      'm_major',
      'm_major_desc',
      ['&', '_get_major_seq'],
      ['&', '_get_major_college', qw(department m_major major_orgstr1_iro major_orgstr2_iro)],
      ['&', '_get_major_dept', qw(department m_major major_orgstr2_iro sem_yr_iro)],
      ['&', '_get_changes'],
      ['&', '_get_prev_stuff']
    ];
}

###############################################################
## Pull the major sequence number out of the %State hash
###############################################################
sub _get_major_seq {
    my $self = shift;
    return ('m_majseq', $MIRO::ETL::State{cur_majseq});
}

###############################################################
## Local wrapper for _get_m_college() that allows side effect of storing State
###############################################################
sub _get_major_college {
    my $self = shift;
    my @arr = $self->_get_m_college(@_);
    $self->set_state('cur_college', $arr[1]); ## strips quotes
    return (@arr);
}

###############################################################
## Local wrapper for _get_m_dept() that allows side effect of storing State
###############################################################
sub _get_major_dept {
    my $self = shift;
    my @arr = $self->_get_m_dept(@_);
    $self->set_state('cur_dept', $arr[1]); ## strips quotes
    return (@arr);
}

###############################################################
## Local wrapper for _get_m_dept() that allows side effect of storing State
###############################################################
sub _get_changes {
    my $self = shift;
    my $old = $MIRO::ETL::State{prev_major};
    my $new = $MIRO::ETL::State{m_major};
    ## Note that the value expression below is a MySQL function call
    return ('hard_change', ($old ? "not hard_same_major('$old','$new')" : 'null'),
            'soft_change', ($old ? "not soft_same_major('$old','$new')" : 'null'));
}

###############################################################
## Return 'previous' field values as columns, out of the State
###############################################################
sub _get_prev_stuff {
    my $self = shift;
    my $maj = $MIRO::ETL::State{prev_major};
    my $maj_desc = $MIRO::ETL::State{prev_major_desc};
    my $college = $MIRO::ETL::State{prev_college};
    my $dept = $MIRO::ETL::State{prev_dept};

    return (
      'prev_major', $maj ? $self->wrap_quotes($maj) : 'null',
      'prev_major_desc', $maj_desc ? $self->wrap_quotes($maj_desc) : 'null',
      'prev_college', $college ? $self->wrap_quotes($college) : 'null',
      'prev_dept', $dept ? $self->wrap_quotes($dept) : 'null'
    );
}

###############################################################
## Wrapper for the SQL generator:
## Reduce major history to just changes, unduplicated
###############################################################
sub distinct_majors {
    my $self = shift;
    my $save = 0;

    my @headers = @{ $self->headers };
    ## Convert row data to hash, indexed by headers
    my %Row = map { shift(@headers) => $_ } @_;

    ## Do nothing with this data row if the major is exploratory. Because there are
    ## students who undeclare a major and go back to exploratory in mid-career, this table
    ## will get messy if we try to account for the exploratories. If, in the future, it is
    ## decided to account for these undeclared periods, you'll need to tweak this
    ## whole function carefully. -dji
    return if $Row{m_major} =~ /^EX/;  ## Let's hope this pattern works into the future...

    ### Here MIRO::ETL::State carries state ACROSS (i.e. between) data rows
    if ($Row{person_uid} eq $MIRO::ETL::State{cur_person}) {
        if ($Row{m_major} ne $MIRO::ETL::State{cur_major}) {
            $self->set_state('prev_major', $MIRO::ETL::State{cur_major});
            $self->set_state('prev_major_desc', $MIRO::ETL::State{cur_major_desc});
            $self->set_state('prev_college', $MIRO::ETL::State{cur_college});
            $self->set_state('prev_dept', $MIRO::ETL::State{cur_dept});

            $self->set_state('cur_major', $Row{m_major});
            $self->set_state('cur_major_desc', $Row{m_major_desc});
            $MIRO::ETL::State{cur_majseq}++;
            $save = 1;
        }
    }
    else {
        $self->clear_state();  ## New person: reset the %MIRO::ETL::State hash
        $self->set_state('prev_major', undef);
        $self->set_state('prev_major_desc', undef);
        $self->set_state('prev_college', undef);
        $self->set_state('prev_dept', undef);

        $self->set_state('cur_person', $Row{person_uid});
        $self->set_state('cur_major', $Row{m_major});
        $self->set_state('cur_major_desc', $Row{m_major_desc});
        $self->set_state('cur_majseq', 1);
        $save = 1;
    }

    if ($save) {
        ## Now, call back to the default SQL generator routine
        $self->generate_sql(@_);
    }
}

###############################################################
## This method overrides the one in MIRO::ETL. It only exists
## so that the SQL generator method (gen) can be redefined.
###############################################################
sub run {
    my $self = shift;
    if (not ref($self)) {
        $self = new $self();  ## Need an instantiated object
    }
    $self->clear_state_every_row(0);  ## Carry state across data rows
    $self->SUPER::run(gen => "distinct_majors");
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::Major - ETL subclass for undergraduate major tracking

=head1 SYNOPSIS

N.B.! This class has a run() method that overrides the usual one in MIRO::ETL.

Basic usage is:

  perl -e 'use MIRO::ETL::Major; MIRO::ETL::Major->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::Major;\
               MIRO::ETL::Major->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
