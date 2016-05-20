
use strict;  ## remove this line upon penalty of death :=|
use Carp;

###############################################################
##
###############################################################
sub _get_sem_yr_parts {
    my $self = shift;
    my %Data = @_;
    my %sem_names = qw(1 Spring 5 Summer 8 Fall);
    $Data{sem_yr_iro} =~ /^([12]\d\d\d)-(\d)$/;
    my $yr = $1;
    my $sem = $sem_names{$2};
    return ('year', $yr,
            'semester', $self->wrap_quotes($sem));
}

###############################################################
##
###############################################################
sub _get_acyr_fiscal {
    my $self = shift;
    my %Data = @_;
    my %sem_names = qw(1 Spring 5 Summer 8 Fall);
    unless ($Data{sem_yr_iro} =~ /^([12]\d\d\d)-([158])$/) {
        croak "Bad sem_yr_iro value = |".$Data{sem_yr_iro}."|";
    }
    my $yr = $1;
    my $sem = $sem_names{$2};
    return ('acyr',          ($sem eq 'Fall' ? $yr+1 : $yr),
            'fiscal_yr_iro', ($sem eq 'Spring' ? $yr : $yr+1));
}

###############################################################
## Simplified residency, with exemptions and rare types neutralized
###############################################################
sub _get_res_miro {
    my $self = shift;
    my %Data = @_;

    my $res = $Data{residency};
    my $desc = $Data{residency_desc};
    my %NRExempt = map { $_ => $_ } qw(E F G H M P S T);

    ## Codes for new simplified types are digraphs (two letters),
    ## because IRAO may make use of any free alphabet letters in
    ## the future, but I'm gambling they won't hit these combos. -dji
    if ($res eq 'R' || $res eq 'C') {
        $res = 'R';
        $desc = 'Resident';
    }
    elsif ($res eq 'J' || $res eq 'I') {
        $res = 'XJ';
        $desc = 'Pac. Islander Exempt';
    }
    elsif ($res eq 'W') {
        $res = 'W';
        $desc = 'WUE Exempt';
    }
    elsif ($res eq 'N' || $res eq 'O' || $res eq 'A' || $res eq '0') {
        $res = 'N';
        $desc = 'Non-Resident';
    }
    elsif ($NRExempt{$res} || $desc =~ /exe?mpt/i) {
        ## there's one example of spelling "exmpt"
        $res = 'XE';
        $desc = 'Non-Resident Exempt';
    }
    elsif ($res) {
        $res = 'XO';
        $desc = 'Other Residency Type';
    }
    else {
        $res = 'XN';
        $desc = 'No Data';
    }

    return ('m_residency', $self->wrap_quotes($res),
            'm_residency_desc', $self->wrap_quotes($desc));
}

###############################################################
## Mainly handle the unspecified cases: null and 'no data'
###############################################################
sub _get_cit_type {
    my $self = shift;
    my %Data = @_;

    my $ct = $Data{citizenship_type};
    $ct = 'D' if $ct !~ /\w/;  ## replace nulls with 'D' (Data not available)
    return ('citizenship_type', $self->wrap_quotes($ct));
}

###############################################################
## Convert ethnicity and citizenship info to race/ethn categories
###############################################################
sub _get_uhm_race_ethn {
    my $self = shift;
    my %Data = @_;

    my %Asians = map { $_ => 1 }    qw(JP CH FI KO TH VI LA IN ME OA MA);
    my %Islanders = map { $_ => 1 } qw(HW GC MC SA TO OP MP PI);
    my %Latinos = map { $_ => 1 }   qw(HS PR MH);
    my %Whites = map { $_ => 1 }    qw(CA PO);
    my %RLabels = (
        ## Hmm, is this a clever structure, or stupid? Time will tell. -dji
         1 => 'NR',
        NR => 'International',
         2 => 'HS',
        HS => 'Hispanic/Latino',
         3 => 'AA',
        AA => 'Black or African American',
         4 => 'CA',
        CA => 'White',
         5 => 'AI',
        AI => 'American Indian or Alaska Native',
         6 => 'AS',
        AS => 'Asian',
         7 => 'HW',
        HW => 'Native Hawaiian or Other Pacific Islander',
         8 => 'MX',
        MX => 'Multiracial',
         9 => 'NO',
        NO => 'Race and ethnicity unknown'
    );

    my $eth = $Data{ethnicity};
    my $eth_desc = $Data{ethnicity_desc};
    my $citzship = $Data{citizenship_type};

    ### Conversion for HR data, converts to IRO_BASE-compatible codes
    if (exists $Data{citizenship_status}) {
        my $cs = $Data{citizenship_status};
        $citzship = {1=>'Y', 2=>'U', 3=>'R', 4=>'N', N=>'D'}->{$cs} || $cs;
    }

    my $uhmrace = $eth;
    my $uhmracedesc = $eth_desc;
    my $uhmethn = $eth;
    my $uhmethndesc = $eth_desc;

    if ($Asians{$eth}) {
        $uhmrace = 'AS';
        $uhmracedesc = 'Asian';
    }
    elsif ($Islanders{$eth}) {
        $uhmrace = 'HP';
        $uhmracedesc = 'Native Hawaiian or Pacific Islander';
    }
    elsif ($Latinos{$eth}) {
        $uhmrace = 'HS';
        $uhmracedesc = 'Hispanic';
    }
    elsif ($Whites{$eth}) {
        $uhmrace = 'CA';
        $uhmracedesc = 'Caucasian or White';
    }
    elsif ($uhmrace eq 'MX') {
        $uhmracedesc = 'Multiracial';
    }

    ## UH Hawaiian trumping rule
    my $hawn = ($Data{hawaiian_iro} =~ /[Y]/i);
    if ($hawn) {
        $uhmrace = 'HP';
        $uhmracedesc = 'Native Hawaiian or Pacific Islander';
        $uhmethn = 'HW';
        $uhmethndesc = 'Native Hawaiian or Part-Hawn';
    }

    ## The "newer" versions of m_race and m_ethnicity ala Yang & Kelly
    ##    N.B. that this comes after Hwn trumping has happened above
    my($m_race, $m_ethnicity, $m_ethnicity_d) = _get_m_values($uhmrace, $uhmethn, $citzship);

    ## Then, IPEDS values
    my $uhm_ipeds_race = $Data{ipeds_race_category};
    my $uhm_ipeds_race_desc = $Data{ipeds_race_category_desc};

    if ($uhm_ipeds_race xor $uhm_ipeds_race_desc) {
        ## One is specified, the other not
        croak "IPEDS race data inconsistent";
    }
    elsif (not $uhm_ipeds_race) {
        ## Attempt to backfill using m_race, if not already in db
        $uhm_ipeds_race = $RLabels{$m_race};
        $uhm_ipeds_race_desc = $RLabels{$uhm_ipeds_race};
    }

    ## Variables with "_x" are Dave's original... "purer"? computations for
    ## these values, as manifest in the current function. -dji
    return (
      'm_race', $m_race,
      'm_race_desc', ($RLabels{$m_race} ? $self->wrap_quotes( $RLabels{ $RLabels{$m_race} } ) : 'null'),
      'm_race_x', $self->wrap_quotes( $uhmrace ),
      'm_race_x_desc', $self->wrap_quotes( $uhmracedesc ),
      'm_ethnicity', $m_ethnicity,
      'm_ethnicity_desc', ($m_ethnicity_d eq 'null' ? 'null' : $self->wrap_quotes( $m_ethnicity_d )),
      'm_ethnicity_x', $self->wrap_quotes( $uhmethn ),
      'm_ethnicity_x_desc', $self->wrap_quotes( $uhmethndesc ),
      'm_ipeds_race', $self->wrap_quotes( $uhm_ipeds_race ),
      'm_ipeds_race_desc', $self->wrap_quotes( $uhm_ipeds_race_desc )
    );
}

###############################################################
## Return a particular subset of variables computed by the above
## _get_uhm_race_ethn(). Different specialized subsets should be
## generated by creating subset_2, etc methods like this one.
###############################################################
sub _get_race_ethn_subset1 {
    my $self = shift;
    my %All = $self->_get_uhm_race_ethn(@_);
    return ('m_race', $All{m_race},
            'm_race_desc', $All{m_race_desc},
            'm_ethnicity', $All{m_ethnicity},
            'm_ethnicity_desc', $All{m_ethnicity_desc}
    );
}

###############################################################
## Compute Yang & Kelly's m_* values
###############################################################
sub _get_m_values {
    my($race, $eth, $cittype) = @_;
    my $m_race = 'null';

    ## First, race... international trumps all
    if ($cittype eq 'N')  { $m_race = 1; }
    elsif ($race eq 'HS') { $m_race = 2; }
    elsif ($race eq 'AA') { $m_race = 3; }
    elsif ($race eq 'CA') { $m_race = 4; }
    elsif ($race eq 'AI') { $m_race = 5; }
    elsif ($race eq 'AS') { $m_race = 6; }
    elsif ($race eq 'HP') { $m_race = 7; }
    elsif ($race eq 'MX') { $m_race = 8; }
    elsif ($race eq 'NO') { $m_race = 9; }

    ## Then, ethnicity. Codes provided by Kelly:
    my %Ethcodes = (
        JP => [1, "Japanese"],
        CH => [2, "Chinese"],
        FI => [3, "Filipino"],
        KO => [4, "Korean"],
        TH => [5, "Thai"],
        VI => [6, "Vietnamese"],
        LA => [7, "Laotian"],
        IN => [8, "Asian Indian"],
        OA => [9, "Other Asian"],
        MA => [10, "Mixed Asian"],
        HW => [11, "Native Hawaiian or Part-Hawn"],
        GC => [12, "Guamanian or Chamorro"],
        MC => [13, "Micronesian"],
        SA => [14, "Samoan"],
        TO => [15, "Tongan"],
        OP => [16, "Other Pacific Islander"],
        MP => [17, "Mixed Pacific Islander"]
    );

    ## Add in a few that exist in older data, but not accounted for in the above
    $Ethcodes{ME} = $Ethcodes{OA}; ## Middle Easterner -> Other Asian
    $Ethcodes{PI} = $Ethcodes{OP}; ## Pacific Islander -> Other Pacific Islander

    my $m_eth = 'null';
    my $m_eth_d = 'null';
    if ($cittype ne 'N') {
        if (int($m_race) == 6 || int($m_race) == 7) {
            $m_eth = $Ethcodes{$eth}[0] || 'null';
            $m_eth_d = $Ethcodes{$eth}[1] || 'null';
        }
    }
    return ($m_race, $m_eth, $m_eth_d);
}


## Real Pre-prof majors: Psych, Nursing, and Engineering.
## Each one maps to the name of its college. -dji
my %Real_Pre_Major = (
    'PPSY' => { col => 'Social Sciences', dept => 'PSY' },
    'PNUR' => { col => 'Nursing & Dental Hygiene', dept => 'NURS' },
    'PRNU' => { col => 'Nursing & Dental Hygiene', dept => 'NURS' },
    'PMT' => { col => 'School of Medicine', dept => 'MEDT' },
    'PREN' => { col => 'Engineering', dept => 'ENGR' },
);

my %Old_Pre_Major = (
    'PBUS' => { major => 'EXB', desc => 'Exploratory Business' },
    'PRED' => { major => 'EXED', desc => 'Exploratory Education' },
    'PRDE' => { major => 'EXHS', desc => 'Exploratory Health Sciences' },
    'PSW' => { major => 'EXSW', desc => 'Exploratory Social Work' },
);

###############################################################
##
###############################################################
sub _get_m_college {
    my $self = shift;
    my %Data = @_;
    croak 'No major_orgstr1_iro passed in to _get_m_dept' unless exists $Data{major_orgstr1_iro};
    croak 'No major_orgstr2_iro passed in to _get_m_dept' unless exists $Data{major_orgstr2_iro};

    my $maj = $Data{m_major} || $Data{major} || $Data{major_iro};
       $maj = undef if $maj eq 'null';   ## counteract any earlier conversion
    my $org2 = $Data{major_orgstr2_iro};
       $org2 = undef if $org2 eq 'null';   ## counteract any earlier conversion
    my $college = $org2 || 'Unknown';

    if ($Data{major_orgstr1_iro} =~ /Hawa.*nuiakea/i) {
        ## Needed because orgstr2 has some department names in it
        $college = "Hawaiinuiakea";
    }
    elsif ($Real_Pre_Major{$maj}) {
        $college = $Real_Pre_Major{$maj}{col};
        ## This should take care of the weird Pre-Engineering thing that happened recently.
    }
    elsif ($Data{major_orgstr1_iro} eq 'Unclassified' || $Data{class_lvl_iro} eq 'UNCLS') {
        $college = 'Unclassified';
    }
    elsif ($org2 eq 'General Arts & Sciences') {
        $college = 'General Studies';
    }
    return ('m_college', $self->wrap_quotes($college));
}

###############################################################
##
###############################################################
sub _get_m_dept {
    my $self = shift;
    my %Data = @_;
    croak 'No major_orgstr2_iro passed in to _get_m_dept' unless exists $Data{major_orgstr2_iro};

    my $dept = $Data{department};
       $dept = undef if $dept eq 'null'; ## counteract any earlier conversion
    my $maj = $Data{m_major} || $Data{major} || $Data{major_iro};
       $maj = undef if $maj eq 'null';   ## counteract any earlier conversion
    my $sem = $Data{sem_yr_iro} || croak 'No sem_yr_iro passed in to _get_m_dept';
       $sem =~ s/[-]//g; ## delete the hyphen, else we can't do numeric comparison. -dji

    if ($Real_Pre_Major{$maj}) {
        $dept = $Real_Pre_Major{$maj}{dept};
    }
    elsif ($dept eq 'CAS' && $maj ne 'IS' && $self->table ne 'miro_degree') {
        ## CAS > MAC for enrollment-type data but NOT for Interdiscip. Studies major -dji
        $dept = 'MAC';
    }
    elsif ($maj eq 'LIS') {
        ## To fix three stinkin' past degree records that have dept missing :=( -dji
        $dept = 'ICS';
    }
    elsif ($maj eq 'HAW' && $sem >= '20075') {
        ## Summer of '07, HAW major no longer belongs to HIPL dept, moves to HAWN dept. -dji
        $dept = 'HAWN';
    }
    elsif ($maj eq 'PHLL' && $sem >= '20081') {
        ## Spring of '08, HIPL dept disappeared, PHLL major moved to IPLL dept.
        $dept = 'IPLL';
    }
    elsif ($maj eq 'UNCL' || $Data{major_orgstr2_iro} eq 'Unclassified') {
        $dept = 'UNC';
    }
    elsif (!$dept) {
        $dept = 'UNC' if not $self->table eq 'miro_degree';
        ### Left as null in the case of miro_degree
    }
    return ('m_dept', $self->wrap_quotes($dept));
}

###############################################################
##
###############################################################
sub _get_m_major {
    my $self = shift;
    my %Data = @_;
    my $maj = $Data{major} || $Data{major_iro};
       $maj = undef if $maj eq 'null';   ## counteract any earlier conversion
    my $desc = $Data{major_desc} || $Data{major_desc_iro} || 'Unknown';
    my $null = 0;

    my $is_Explor = ($desc =~ /pre-/i && not $Real_Pre_Major{$maj});

    if ($MIRO::ETL::State{non_MAN_based} || !$maj) {
        $null = 'null';
    }
    elsif ($maj eq 'PRNU') {
        $maj = 'PNUR';  ## Fix this big mistake.
    }
    elsif ($maj eq 'SLS1') {
        $maj = 'SLS';   ## Fix a silly legacy thing.
    }
    elsif (my $x = $Old_Pre_Major{$maj}) {
        $maj = $x->{major};
        $desc = $x->{desc};
    }
    elsif ($Real_Pre_Major{$maj}) {
        if ($desc =~ /Gen(?:eral)?\s*\((.+)\)/) {
            $desc = $1;
        }
    }
    elsif ($maj eq 'GEAS' || $is_Explor) {
        $maj = 'EX';
        if ($is_Explor) {
            ### my $cat = ($desc =~ /Gen(?:eral)?\s*\((.+)\)/) ? $1 : $desc;
            croak "ABORT: Extinct major $maj/$desc found. ETL does not handle it.";
        }
        else {
            $desc = 'Exploratory';
        }
    }
    return ('m_major',      $null || $self->wrap_quotes($maj),
            'm_major_desc', $null || $self->wrap_quotes($desc));
}

##############################################################}
## Clean up nation_of_citizenship_desc data
###############################################################
sub nation_of_citizenship_desc {
    my $self = shift;
    my($in) = @_;

    if ($in =~ /\*|invalid/i) {
        ## Handles "**VPD MIGRATION INVALID CODE**" and maybe some other weird ones
        "null";
    }
    elsif ($in =~ /south korea|korea, republic/i) {
        "South Korea (ROK)";
    }
    elsif ($in =~ /north korea|korea, demo|dprk/i) {
        "North Korea (DPRK)";
    }
    elsif ($in =~ /france/i) {
        "France";
    }
    elsif ($in =~ /germany/i) {
        "Germany";
    }
    elsif ($in =~ /serbia.*mont/i) {
        "Serbia and Montenegro";
    }
    elsif ($in =~ /taiwan/i) {
        "Taiwan";
    }
    elsif ($in =~ /slo[vw]ak/i) {
        "Slovak Republic";
    }
    elsif ($in =~ /bo[sz]nia/i) {
        "Bosnia and Herzegovina";
    }
    elsif ($in =~ /fsm|micronesia/i) {
        "Micronesia, Federated States";
    }
    else {
        $in;
    }
}

###############################################################
## Simplified geographic origin data + nation of citizenship
###############################################################
sub _get_geographic {
    my $self = shift;
    my %Data = @_;

    my($origin, $nocd);
    if ($Data{citizenship_type} eq 'N') {
        $origin = 'International';
    }
    elsif ($Data{citizenship_type} eq 'U') {
        $origin = 'US National/CFAS';
    }
    elsif ($Data{residency} eq 'R' || $Data{residency} eq 'C') {
        $origin = 'Hawaii';
    }
    else {
        $origin = 'US Mainland';
    }
    return ('m_geog_origin', $self->wrap_quotes($origin));
}

###############################################################
## Mostly a way to change the name to include the underscore
###############################################################
sub _get_ed_lvl_iro {
    my $self = shift;
    my %Data = @_;
    my $edlvl = $Data{edlvl_iro} || 'XN';
    $edlvl = 'UU' if $edlvl eq '*';  ## convert archaic * to UU
    return ('ed_lvl_iro', $self->wrap_quotes($edlvl));
}

###############################################################
##
###############################################################
sub _get_styp_miro {
    my $self = shift;
    my %Data = @_;

    my $styp;
    my $sri = $Data{styp_reg_iro} || $Data{styp_adm};
    if ($sri =~ /^([FM])$/i) {
        $styp = 'XF';
    }
    elsif ($sri =~ /^([TCR])$/i) {
        $styp = uc($1);
    }
    elsif ($sri =~ /\w/) {
        $styp = 'XO';
    }
    else {
        $styp = 'XN';
    }
    return ('styp_miro', $self->wrap_quotes($styp));
}

###############################################################
##
###############################################################
sub _get_hs_type {
    my $self = shift;
    my %Data = @_;
    my $hstype = $Data{high_sch_type_iro};

    ## Define the mapping
    my %Map = ('HIPU' => 'Hawaii Public', 'HIPR' => 'Hawaii Private', 'FOR' => 'Foreign');
    map { $Map{$_} = 'US National/CFAS' } qw(ASA CAZ CFA COM GUA POS PUE USJ VIS);
    map { $Map{$_} = 'US Mainland' }
        qw(AK AL AR AZ CA CO CT DC DE FL GA IA ID IL IN KS KY
           LA MA MD ME MI MN MO MS MT NC ND NE NH NJ NM NV NY
           OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY);

    ## Now compute the column value using it
    $hstype = $Map{$hstype} || ($hstype ? 'Other' : 'No Data');
    return ('m_hs_type', $self->wrap_quotes($hstype));
}

###############################################################
##
###############################################################
sub _get_conv_sat_scores {
    my $self = shift;
    my %Data = @_;
    return (); ## Do nothing just now
}

###############################################################
##
###############################################################
sub gender {
    my $self = shift;
    shift || 'N';  ## replace nulls with N (No data)
}

###############################################################
## Validation: integer
###############################################################
sub valid_integer {
    my($dat) = @_;
    croak "Invalid integer data = |$dat|" if $dat =~ /\D/;
    return $dat;
}

###############################################################
## Validation: alphanumeric identifier
###############################################################
sub valid_alphaid {
    my($dat) = @_;
    croak "Invalid alphanumeric identifier = |$dat|"
        unless $dat =~ /^_*[a-z]\w+$/i;
    return $dat;
}

1;
__END__
=pod

=head1 NAME

MIRO::ETL::Common - a collection of functions that are used by many modules, and should be defined in one place

=head1 SYNOPSIS

require MIRO::ETL::Common;

=head1 DESCRIPTION

This is not a class, or really even a named module per se, just a way to isolate shared
functions in their own file.

=head1 AUTHOR

dji <David.Iannucci@hawaii.edu>

=cut

