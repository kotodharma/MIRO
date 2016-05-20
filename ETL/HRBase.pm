package MIRO::ETL::HRBase;

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
    "miro_hrbase"
}

###############################################################
##
###############################################################
sub _build_fields {
    my $self = shift;

    [
      'id_number',
      'age',
      'sex',
      'extract_year',
      'extract_month',
      'extract_day',
      ['&', '_get_qtr', qw(qtr_year)],
      'eac',
      'eac_dept',
      'eac_division',
      'eac_division_desc',
      'eac_branch',
      'eac_branch_desc',
      'eac_section',
      'eac_section_desc',
      'eac_unit',
      'eac_unit_desc',
      'empl_status',
      'empl_status_desc',
      'employee_type_code',
      'citizenship_status',
      'job_code',
      ['&', '_get_citizenship', qw(citizenship_status)],
      ['&', '_get_mhr_div_college', qw(eac_division eac_branch)],
      ['&', '_get_job_code_fields', qw(job_code employee_type_code)],
      ['&', '_get_race_ethn_subset1', qw(ethnicity citizenship_status)],
      'job_family',
      'job_function',
      'union_code',
      'union_code_desc',
      'grade',
      'ipeds',
      'eeo_uh',
      'uh_iro',
      'service_years',
      'fte_person',
      'fte_total',
      ['&', '_get_m_ftpt', qw(fte_total)],
      'tenure_code',
      'tenure_effective_date',
      ['&', '_get_tenure_status', qw(tenure_code)],
      'highest_degree',
      ['&', '_get_degree_stuff', qw(highest_degree)],
      'univ_appt_date',
      'appt_period_from',
      'appt_period_to',
    ];
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub sex {
    my $self = shift;
    shift || 'N';  ## Replace nulls with "N" for no data
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub ipeds {
    my $self = shift;
    shift || '6';  ## Replace nulls with "6" for Unknown
}

###############################################################
## Field-specific cleaning/transformation, called from xform
###############################################################
sub uh_iro {
    my $self = shift;
    shift || 'NO';  ## Replace nulls with "NO" for Unknown
}

###############################################################
##
###############################################################
sub _get_qtr {
    my $self = shift;
    my %Data = @_;
    return ('qtr', $self->wrap_quotes(substr($Data{qtr_year},0,1)));
}

###############################################################
##
###############################################################
sub _get_mhr_div_college {
    my $self = shift;
    my %Data = @_;
    my %NotAcadColl = (
        10 => 'UHM Chancellor',
        15 => 'College of A&S Deans',
        26 => 'Library Services',
        28 => 'Outreach College',
        38 => 'Research & Grad Division',
        45 => 'Student Affairs',
        46 => 'Intercollegiate Athletics',
        50 => 'Administration',
    );
    my %AcadColl = (
        11 => 'Arts & Humanities',
        12 => 'Lang, Ling & Lit',
        13 => 'Natural Sciences',
        14 => 'Social Sciences',
        16 => 'Tropical Ag & Human Res',
        17 => 'School of Architecture',
        18 => 'Business Administration',
        19 => 'Education',
        20 => 'Engineering',
        21 => 'School of Law',
        2311 => 'School of Medicine',
        2321 => 'Nursing & Dental Hygiene',
        2331 => 'School of Medicine',  ## orig code is Public Health
        2341 => 'School of Social Work',
        27 => 'Travel Industry Management',
        30 => 'Pacific & Asian Studies',
        31 => 'Hawaiinuiakea',
        3808 => 'Ocean & Earth Sci & Tech',
    );
    my $div = $Data{eac_division};
    my $br = $Data{eac_branch};
    my $college;

    if ($college = $AcadColl{$div.$br}) {
        $div = 'Academic Colleges';
    }
    else {
       my $origdiv = $div;
       $div = $NotAcadColl{$div} || 'Academic Colleges';
       $college = ($div eq 'Academic Colleges') ? $AcadColl{$origdiv} : undef;
    }
    return ('mhr_division', $self->wrap_quotes($div),
            'mhr_college', $self->wrap_quotes($college));
}

###############################################################
##
###############################################################
sub _get_job_code_fields {
    my $self = shift;
    my %Data = @_;
    my $jc = $Data{job_code};
    my $et = $Data{employee_type_code};
    my $jc1 = substr($jc,0,1);
    my($jc2, $jc3) = (undef, undef);

    my %ETypes = (
       A => 'APT',
       C => 'Civil Service',
       E => 'Executive/Managerial',
       F => 'Faculty',
       G => 'Graduate Assistant',
       L => 'Faculty'  ## Lecturer (considered Faculty)
    );
    my @FacTypes = (
        {},  ## a dummy, to force indexes of others to be positive integers (>0)
        { lvl2 => 'Extension Agent' },
        { lvl2 => 'Librarian' },
        { lvl2 => 'Instructional',
          2 => 'Instructor',          3 => 'Assistant Professor',
          4 => 'Associate Professor', 5 => 'Professor' },
        { lvl2 => 'Researcher',
          2 => 'Junior Researcher',    3 => 'Assistant Researcher',
          4 => 'Associate Researcher', 5 => 'Researcher' },
        { lvl2 => 'Specialist',
          2 => 'Junior Specialist',    3 => 'Assistant Specialist',
          4 => 'Associate Specialist', 5 => 'Specialist' },
    );
    my $jlvl1 = $ETypes{$et} || 'UNKNOWN_TYPE';
    my($jlvl2, $jlvl3) = (undef, undef);
    my $idx;

    if ($et eq 'L') {
        $jlvl2 = 'Lecturer';
    }
    elsif ($et eq 'G') {
        ## Graduate Assistant
        $jc3 = substr($jc,2,2); ## add appt months from job code
        my %GATypes = ('09' => 'Teaching Assistant', '11' => 'Research Assistant');
        $jlvl2 = $GATypes{$jc3} || undef;
    }
    elsif ($et eq 'F' && ($idx = {A=>1,B=>2,I=>3,M=>3,J=>3,R=>4,S=>5}->{$jc1})) {
        ## Typical 5-char faculty-type job code
        $jc2 = substr($jc,1,1); ## 2nd character: numeric
        $jc3 = substr($jc,3,2); ## Last two characters: 09 or 11 or 12

        $jlvl2 = $FacTypes[$idx]->{lvl2};
        if ($idx > 2) {
            $jlvl3 = $FacTypes[$idx]->{$jc2};
        }
    }

    return ('job_code1', $self->wrap_quotes($jc1),
            'job_code2', $self->wrap_quotes($jc2),
            'job_code3', $self->wrap_quotes($jc3),
            'job_lvl_1', $self->wrap_quotes($jlvl1),
            'job_lvl_2', $self->wrap_quotes($jlvl2),
            'job_lvl_3', $self->wrap_quotes($jlvl3));
}

###############################################################
##
###############################################################
sub _get_m_ftpt {
    my $self = shift;
    my %Data = @_;
    return ('m_ftpt', $self->wrap_quotes($Data{fte_total} >= 1.0 ? 'FT' : 'PT'));
}

###############################################################
##
###############################################################
sub _get_tenure_status {
    my $self = shift;
    my %Data = @_;
    my $tc = $Data{tenure_code};
    my %Stati = (
        1 => 'Tenure-track (Probationary)',
        2 => 'Tenured',
        3 => 'Not eligible for tenure',
        0 => 'Faculty tenure n/a',
    );
    my $idx = int( {FPP=>1,FPR=>1,
                    FTN=>2,FTP=>2,
                    FCN=>3,FNR=>3,FNT=>3,FPI=>3,FTD=>3,FVF=>3}->{$tc} );
    my $ts = $Stati{$idx};
    return ('tenure_status', $self->wrap_quotes($ts));
}

###############################################################
##
###############################################################
sub _get_degree_stuff {
    my $self = shift;
    my %Data = @_;
    my $hd = $Data{highest_degree};
    my %DegreeDesc = (
      A => 'Associates (less than Bachelors)',
      B => 'Bachelors',
      D => 'Doctoral',
      F => 'Post Bachelor-5th year',
      M => 'Masters (not terminal)',
      P => 'Professional (Law, Medicine)',
      T => 'Terminal degree (usually Masters)',
      U => 'Unknown'
    );
    my $desc = $DegreeDesc{$hd};
    return ('highest_degree_desc', $hd ? $self->wrap_quotes($desc) : 'null',
            'terminal_degree', $hd ? $self->boolean(int({D=>1,P=>1,T=>1}->{$hd})) : 'null');
}

###############################################################
##
###############################################################
sub _get_citizenship {
    my $self = shift;
    my %Data = @_;
    my $cs = int( $Data{citizenship_status} );  ## merge "N" and null into "Not indicated"
    my %Codes = (1=>'US Citizen',    2=>'US National',    3=>'Permanent Resident',
                 4=>'International', 0=>'Not indicated');
    return('citizenship', $self->wrap_quotes($Codes{$cs} || 'Bad citizenship code') );
}

###############################################################
##
###############################################################
sub XXpost_proc {
    my $self = shift;

    my $table = $self->table;
    print <<EOT;
        update $table t
        set
        where
EOT
}


__PACKAGE__->meta->make_immutable;
1;
__END__
=pod

=head1 NAME

MIRO::ETL::HRBase - ETL subclass for human resources base data

=head1 SYNOPSIS

Basic usage is:

  perl -e 'use MIRO::ETL::HRBase; MIRO::ETL::HRBase->run;' <csv tab-delim input file>

This will cause SQL INSERT statements to be generated which write into the default table
(see method default_table). To cause data to be written elsewhere, say into table 'foo' in
schema 'test':

  perl -e 'use MIRO::ETL::HRBase;\
               MIRO::ETL::HRBase->new(table=>"test.foo")->run;' <csv tab-delim input file>

=head1 DESCRIPTION

Need to have dependencies like Moose in path, of course, as well as this class and its
parent, and... whatever.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut
