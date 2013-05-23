#!/usr/bin/perl

use warnings;
use strict;

my $VERSION;
$VERSION = sprintf "%d.%02d", q$Revision: 1.00 $ =~ /(\d+)/g;

use Getopt::Long qw(:config bundling_override);
use Pod::Usage;

my %opt = (
           help => 0,
           man => 0,
           version => 0,
           database => "Oracle",
     sid => $ENV{ORACLE_SID},
	   user => $ENV{DB_USER},
	   pass => $ENV{DB_PWD},
	   rows => 0,
	   log => $ENV{SQL_LOG} || "/dev/null",
           trace => 0,
	   desc => 0,
);

GetOptions (\%opt,
            'help|?',
	    'man',
	    'version',
            'database|d=s',
	    'file|f=s',
	    'sid|s=s',
	    'user|u=s',
	    'pass|p=s',
	    'rows|r=i',
	    'log|l=s',
            'trace|t=i',
            'desc|?',
	   ) or pod2usage(1);
pod2usage(1) if $opt{help};
pod2usage(-exitstatus => 0, -verbose => 2) if $opt{man};
print "$VERSION\n" and exit(0) if $opt{version};

my $sql;

if (exists $opt{file}){
  open FILE, $opt{file} or die "can't open $opt{file}: $!";
  $sql = join '', <FILE>;
  close FILE or die "close failed on $opt{file}: $!";
} else {
  $sql = shift @ARGV;
}

use DBI;

open LOG, ">> $opt{log}" or die "can't open $opt{log}: $!";

my $h = DBI->connect("dbi:$opt{database}:$opt{sid}",
		     $opt{user}, $opt{pass},
		     {AutoCommit => 0,
		      RaiseError => 1,
		      PrintError => 0});

$h->trace($opt{trace});

#print "$sql\n";

while ($sql =~ /^\s*@(\S+)(.*)$/m){
  my $script = $1;
  print LOG scalar localtime(), "\n$script;\n"
    or die "can't append to $opt{log}: $!";

  open SCRIPT, $script or die "can't open $script: $!";

  local $/; # slurp mode
  my $script_sql = <SCRIPT>;

  my @args;
  if (length($2)){
    @args = split (" ", $2);
  };

  my $i = 0;
  for (@args){
    $i++;
    #print "$i => $_\n";
    $script_sql =~ s/&$i/$_/g;
  };

  $sql =~ s/^\s*\@$script.*$/$script_sql/;
  close SCRIPT or die "can't close $script: $!";
}

$sql =~ s/^\s*(![^;]*)\s*$/$1;/mg;

print LOG scalar localtime(), "\n$sql;\n"
  or die "can't append to $opt{log}: $!";

for (split(m';', $sql)){
  if (s/\!(.*)//m){
    print `$1`;
  }

  next if /^\s*$/;

  next if /^\s*--/;

  print LOG scalar localtime(), "\n$_;\n"
    or die "can't append to $opt{log}: $!";

  my $s;
  if (/^\s*desc (.*)/){
    $s = $h->prepare("select * from $1");
    $opt{desc} = 1;
  } else {
    $s = $h->prepare($_);
  }


  for my $i (1..$s->{NUM_OF_PARAMS}){
    my $param = shift @ARGV;
    $s->bind_param($i, $param);
    print LOG "Bind param $i using $param.\n"
      or die "can't append to $opt{log}: $!";
  }

  $s->execute();

  my $fields = $s->{NUM_OF_FIELDS} || 0;
#  $fields = 0 unless defined $fields;

  if ($fields > 0){
    my @row;
    my $row = 0;

    if ($opt{desc}) {
      print "Column Name                     Type  Precision  Scale  Nullable?\n";
      print "------------------------------  ----  ---------  -----  ---------\n";

      for (my $i = 0; $i < $fields; $i++){
        printf "%-30s %5d       %4d   %4d  %s\n",
                $s->{NAME}->[$i],
                $s->{TYPE}->[$i],
                $s->{PRECISION}->[$i],
                $s->{SCALE}->[$i],
                ("No", "Yes", "Unknown")[ $s->{NULLABLE}->[$i] ];
      }
    } else {
      while (@row = $s->fetchrow_array){
        last if ($opt{rows} > 0 and ++$row > $opt{rows});
        print "@row\n";
      }
    }
  }

  $s->finish();
}

$h->commit;

close LOG or die "close failed on $opt{log}: $!";



__END__

=head1 NAME

exec_sql.pl - Run generic SQL statements with DBI.

=head1 SYNOPSIS

=over

=item B<exec_sql.pl> [I<options>] I<statements> I<binds...>

=back

=head1 DESCRIPTION

=head2 Transactions

=head1 OPTIONS

=over

=item B<-f> I<file>, B<--file> I<file>

=item B<-s> I<sid>, B<--sid> I<sid>

=item B<-u> I<user>, B<--user> I<user>

=item B<-p> I<pass>, B<--pass> I<pass>

=item B<-r> I<rows>, B<--rows> I<rows>

=item B<-l> I<logfile>, B<--log> I<logfile>

=item B<-desc>

Prints meta information about the selected columns instead of printing the 
results of each query.

=item B<-?>, B<--help>

Prints the B<SYNOPSIS> and B<OPTIONS> sections.

=item B<--man>

Prints the exec_sql.pl(1) manual.

=item B<--version>

Prints the current version number of exec_sql.pl and exits.

=back

=head1 ENVIRONMENT

=over

=item B<SQL_LOG>

=item B<DB_USER>

=item B<DB_PWD>

=item B<ORACLE_SID>

=back

=head1 EXAMPLES

Here's a quick and dirty way to get the current date using Oracle:

  $ exec_sql.pl "select sysdate from dual"
  17-OCT-06

And here's a contrived example to show using a bind variable:

  $ exec_sql.pl "select sysdate from dual where dummy = ?" X
  17-OCT-06

And an even more contrived example to show two seperate statements
with one bind variable:

  $ exec_sql.pl "select sysdate from dual where dummy = ?;
                 select sysdate + 1 from dual;" X
  17-OCT-06
  18-OCT-06


=head1 TODO

=over

=item *

Make connection string variable.

=back

=head1 BUGS

=over

=item *



=back

=head1 NOTES



=head1 HISTORY


=head1 SEE ALSO

perl(1)

=head1 AUTHOR

Jon Ericson I<jericson@cpan.org>

=head1 COPYRIGHT

  Copyright 2006 by Jon Ericson.

  This program is free software; you can redistribute it and/or modify
  it under the same terms as Perl.

=begin CPAN

=head1 README


=head1 SCRIPT CATEGORIES


=end CPAN

=cut
