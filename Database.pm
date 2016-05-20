package MIRO::Database;

use strict;  ## remove this line under penalty of death :=|
use DBI;
use Carp;

our $errstr;
our %Creds;

require dbcreds;

#############################################################################
##
#############################################################################
sub connect {
    my $self = shift;
    my($service, @other) = @_;
    my @creds = @{ $Creds{$service} } or croak "No such service_name ($service)";
    if (@creds < 2) {
        print STDERR "User: ";
        my $u = <STDIN>;
        chomp $u;
        push(@creds, $u);
    }
    if (@creds < 3) {
        print STDERR "Password: ";
        system "stty -echo";
        my $p = <STDIN>;
        system "stty echo";
        print STDERR "\n";
        chomp $p;
        push(@creds, $p);
    }
    my $h = DBI->connect(@creds, @other) or croak $DBI::errstr;
    $MIRO::Database::errstr = $DBI::errstr;
    return $h;
}

1;
__END__
=pod

=head1 NAME

MIRO::Database - Local wrapper for database authentication; encapsulation of db info

=head1 SYNOPSIS

  MIRO::Database->connect(<service_name>, [other params])

=head1 DESCRIPTION

This method returns a DBI database connection handle, which then is used like any other DBI handle.
"Other params" are those to be passed directly through to DBI->connect()

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
