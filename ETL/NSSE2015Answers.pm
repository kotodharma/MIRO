package MIRO::ETL::NSSE2015Answers;

use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

extends 'MIRO::ETL';

###############################################################
##
###############################################################
sub default_table {
    "nsse_2015_answers"
}

###############################################################
##
###############################################################
sub _get_ed_lvl_irclass {
    my $self = shift;
    my %Data = @_;
    my $irc = $Data{irclass};
    croak "IRclass not 1 or 4" unless ($irc == 1 || $irc == 4);
    return ('ed_lvl', $self->wrap_quotes($irc == 1 ? 'FR' : 'SR'));
}

###############################################################
## Override parent method. More single "record" than single row per se
###############################################################
sub single_row_insert {
    my $self = shift;
    my($cols, $vals, $add) = @_;
    my %Rec = map { $_ => shift(@{$vals}) } @{$cols};  ## Hashify cols and their vals
    my $idn = delete $Rec{id_number};
    my $edl = delete $Rec{ed_lvl};    ## delete these so they are not treated as questions
    my $cat = delete $Rec{category};
    my $table = $self->table;
    my $out;

    foreach my $col (sort keys %Rec) {
        $out .= sprintf(" insert $table (id_number,ed_lvl,category,question,answer)".
                        " values (%s,%s,'%s','%s',%s);\n", $idn, $edl, $cat, $col, $Rec{$col});
    }
    return $out;
}

###############################################################
##
###############################################################
sub post_proc {
    my $self = shift;

    my $table = $self->table;
      ## When adapting this class for later NSSE surveys, don't forget to
      ## change the hardcoded semester code below! -dji
    print <<EOT;
        update $table n
           join miro_base b
             on b.id_number = n.id_number
            and b.sem_yr_iro = '2015-1'
        set n.base_fk = b.skid
        where n.base_fk is null ;
EOT
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::NSSE2015Answers - ETL subclass for NSSE 2015 data going into the answers table

=head1 SYNOPSIS

Should serve only as parent class for the other NSSE 2015 subclasses that are converting
columnar variables in the NSSE spreadsheet into row data in the nsse_2015_answers table.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
