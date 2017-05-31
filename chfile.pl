#!/usr/local/bin/perl
# /*********************************************************************/

use Switch;

MAIN:
$fn = "$ARGV[0]";
$output_dir	= "$ENV{'AREA_ROOT_DIR'}/output";
$area_name 	= uc("$ENV{'AREA_NAME'}");
$src="";

$que1 = "$ARGV[0]";

# 
#open(OUTALL, ">${output_dir}/F50F_report_$cur_date.csv") || die "Can't open ${output_dir}/\n";
open(INALL, "<$fn") || die "Can't open '$fn' /\n";
open(OUTALL, ">$fn.out") || die "Could not open file '$fn' $!\n";
flock(OUTALL, 2) || die;
	
while (my $line = <INALL>)  {
	chomp($line);
	if ($line eq "-}") {
	  print OUTALL "$line\n";
	}
	else {
  	print OUTALL "$line\r\n";
	}
}
flock(OUTALL, 8) || die;
close(OUTALL);
close(INALL);

exit  (1);

CNF-FAX|BOTH|EXECREQ:Y|MANY|M:MTS$CDT_ACCOUNT|FLDREQ:N|TYPE:ACCOUNT|||,-
CNF-FAX|BOTH|EXECREQ:Y|MANY|M:MTS$CDT_ACCOUNT|FLDREQ:N|TYPE:ACCOUNT|||,-
CNF-FAX|BOTH|EXECREQ:Y|MANY|E:MTS$QUEUE|FLDREQ:N|TYPE:STR|||THIS_E:ONE|,-
