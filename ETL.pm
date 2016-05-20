package MIRO::ETL;

use 5.8.0;   ## this is a minimum req, not an exact one
use strict;  ## remove this line upon penalty of death :=|
use Moose;
use namespace::autoclean;
use Carp;
no warnings qw(numeric uninitialized);

has 'fields' => (
    is  => 'ro',
    lazy => 1,
    builder => '_build_fields',
    init_arg => undef
);

has 'headers' => (
    is => 'rw',
    lazy => 1,
    default => sub { [] },  ## an empty array
    isa => 'ArrayRef',
    trigger => \&standardize_headers,
    init_arg => undef
);

has 'table' => (
    is  => 'ro',
    default => sub { my $s = shift; $s->default_table(); }
);

## This is a flag controlling whether the %State hash, a kind of "clipboard"
## for the ETL process, is to be cleared/reset on each row of data. True by
## default, but individual modules may turn it off and clear the hash by hand.
has 'clear_state_every_row' => (
    is  => 'rw',
    default => 1
);

###############################################################
##
###############################################################
sub standardize_headers {
    my($obj, $new, $old) = @_;
    ## Remove all non-"word" chars (as defined by regex) and
    ##   convert to lower-case
    my @xformed = map { (s/[\W]//g, lc($_))[1] } @{$new};
    if (not $old) {    ## this prevents infinite recursion!
        $obj->headers([@xformed]);
    }
}

###############################################################
##
###############################################################
sub basic_clean {
    my $self = shift;
    my($s) = @_;
    $s =~ s/[<>"]//g;  ## these chars never allowed in data
    $s =~ s/^\s*//;    ## trim left side of whitespace
    $s =~ s/\s*$//;    ## trim right side of whitespace
    return $s;
}

###############################################################
##
###############################################################
sub xform {
    my $self = shift;
    my($field, $cell) = @_;

    ## First, most basic level of cleaning
    $cell = (not(defined $cell) || $cell =~ /#NULL/i) ?  '' : $self->basic_clean($cell);

    ## Attempt to further transform cell data as appropriate to this job
    ## by looking for a method with same name as column
    eval {
        $cell = $self->$field($cell);
    };
    if ($@) {
        unless ($@ =~ /can't locate object method/i) {
            croak "Failure to transform cell data for $field: $@";
            ## NOT REACHED
        }
    }
    return $cell;
}

###############################################################
##
###############################################################
sub boolean {
    my $self = shift;
    my($s, $null_is_false) = @_;
    croak "Unexpected character ($1) in boolean column" if $s =~ /([^01YN\s])/i;
    unless ($null_is_false) {
        return 'null' if (!defined($s)) || $s !~ /^\s*[01YN]\s*$/;
    }
    return ($s =~ /[Y1]/i) ? 1 : 0;
}

###############################################################
##
###############################################################
sub wrap_quotes {
    my $self = shift;
    my($s) = @_;
    if ($s =~ /^(['"]).*\1$/) {
        ## If it already looks quoted, don't redo it
        carp "Trying to re-quote an already quoted string: $s OK?";
        return $s;
    }
    return 'null' if (!defined($s)) || $s =~ /^\s*$/;
    $s =~ s/'/''/g;
    carp "Storing string 'null'. Is this what you intended?" if $s eq 'null';
    return "'$s'";
}

###############################################################
##
###############################################################
sub single_row_insert {
    my $self = shift;
    my($cols, $vals, $add) = @_;

    my $table = $self->table;
    sprintf(" insert $table (%s,load_date) values (%s,now());\n",
        join(',', @{$cols}), join(',', @{$vals})
    ) . $add;
}

our %State;  ## A place to store information (current state) temporarily

###############################################################
## Add processed data to the store of the current state
###############################################################
sub add_state {
    my($c, $v) = @_;
    ## Not only quotes we've added, but sometimes raw string data comes in to this
    ## class already quoted, so we need to remove them to get the original data.
    $v =~ s/^(['"])(.*)\1$/$2/;
    $State{$c} = $v;
}
sub set_state { my $self = shift; add_state(@_); }  ## just alias for above; rename some day
sub get_state { my $self = shift; my($k) = @_; return $State{$k}; }
sub clear_state { my $self = shift; %State = (); }

###############################################################
## Process data and generate SQL code for a single row
###############################################################
sub generate_sql {
    my $self = shift;

    my @headers = @{ $self->headers };
    ## Convert row data to hash, indexed by headers
    my %Row = map { lc(shift(@headers)) => $_ } @_;

    my(@sql_cols, @sql_vals, $addenda);
    my @fields = @{ $self->fields };   ## this array comes from the subclass _build_fields() method

    foreach my $f (@fields) {

        ## It's a simple 1-to-1 match of input column to db column
        if (not ref($f)) {
            $f = lc $f;   ## case-insensitize
            croak "No column='$f' found in input" if not exists $Row{$f};
            push(@sql_cols, $f);
            my $cell = $self->xform($f, $Row{$f});
            unless ($cell eq 'null' || $cell =~ /^[\d.]+$/) {
                ## Unless the datum is null or entirely numeric, attempt to wrap in quotes
                $cell = $self->wrap_quotes($cell);
            }
            push(@sql_vals, $cell);
            add_state($f, $cell);
            next;
        }

        ## Otherwise it's an array - special biz logic applies
        my @fod = @{$f};
        my $op = shift @fod;
        my $sub = shift @fod;
        @fod = map { lc } @fod;   ## case-insensitize

        if ($op eq '&' || $op eq '=') {
            ## "Collective" application of code callback to columns (Reduce?)
            ##  i.e. sub(c1, c2, c3, c4...)
            my @params = map {
              croak "No column='$_' found in input" unless exists($Row{$_}) || exists($State{$_});
              ($_, exists($State{$_}) ? $State{$_} : $self->xform($_, $Row{$_}))
            } @fod;
            ## Note wrt above: this is the one place where variables previously cleaned and
            ## processed may be passed into a callback for a later variable. If we find the
            ## variable already in the current row state ($State), we use this value (it might
            ## even have been drastically changed) rather than the one passed in originally
            ## with the row data.

            my @out;
            eval {
                @out = $self->$sub(@params);
            };
            if ($@) {
                croak "Callback ($sub) failed for '&' or '=' field spec: $@";
                ## NOT REACHED
            }
            if ($op eq '&') {
                while (@out) {
                    croak "Expected column name undefined (sub=$sub)" if not defined $out[0];
                    my $col = shift @out;
                    push(@sql_cols, $col);

                    croak "Expected column value undefined (sub=$sub)" if not defined $out[0];
                    my $val = shift @out;
                    push(@sql_vals, $val);

                    add_state($col, $val);
                }
            }
            elsif ($op eq '=') {
                $addenda .= $out[0];
            }
        }
        elsif ($op eq '*') {
            ## "Distributive" application of code callback to columns (Map)
            ##  i.e. sub(c1), sub(c2), sub(c3), sub(c4)...
            foreach my $col (@fod) {
                croak "No column='$col' found in input" if not exists $Row{$col};
                my $value = $self->xform($col, $Row{$col}); ## clean it
                eval {
                    my @out = $self->$sub($col, $value);
                    while (@out) {
                        croak "Expected column name undefined (sub=$sub)" if not defined $out[0];
                        my $col = shift @out;
                        push(@sql_cols, $col);

                        croak "Expected column value undefined (sub=$sub)" if not defined $out[0];
                        my $val = shift @out;
                        push(@sql_vals, $val);

                        add_state($col, $val);
                    }
                };
                if ($@) {
                    croak "Callback ($sub) failed for '*' field spec: $@";
                    ## NOT REACHED
                }
            }
        }
        else {
            croak "Unknown op in fields spec: $op";
            ## NOT REACHED
        }
    }

    print $self->single_row_insert(\@sql_cols, \@sql_vals, $addenda);
}

###############################################################
## Read tab-delim data from STDIN and process; takes args hash
###############################################################
sub run {
    my $self = shift;
    my %args = ref($_[0]) eq 'HASH' ? %{$_[0]} : @_;
    my $gen = $args{gen} || 'generate_sql';
    local %State = (); ## Localize variable, to ensure being cleared between calls

    unless (ref($self)) {
        $self = new $self();  ## create object if it doesn't exist yet
    }

    my $line = 0;
    eval { $self->pre_proc(); };   ## eval because method might not exist

    while (<>) {
        if ($line == 0 && /\015$/) {
            ## Try to dynamically detect input newline situation
            $/ = "\015\012";  ## change to DOS-like (CRLF endings)
        }
        chomp;
        $self->clear_state() if $self->clear_state_every_row;

        my @data = split /[\t]/, $_, -1;  ## file must be tab-delimited!
        if ($line == 0) {
            next if $args{skip_headers};
            $self->headers( [@data] );
        }
        else {
            $self->$gen(@data); ## real work happens here
        }
        $line++;
    }
    eval { $self->post_proc(); };
    return 1;
}

__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL - Superclass for ETL operations

=head1 SYNOPSIS

See subclasses for usage

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut

