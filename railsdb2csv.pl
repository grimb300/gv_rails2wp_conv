#!/usr/bin/perl

# Open input and output files (hard coded for now)
my $output_dir = "table_csvs";
unless (-e $output_dir and -d $output_dir) {
  mkdir($output_dir) || die "Can't create directory: $output_dir";
}
open(my $input_fh, "<", "rails_db.sql")
  || die "Can't open < rails_db.sql: $!";
open(my $output_fh, ">", "$output_dir/rails_db.csv")
  || die "Can't open > rails_db.csv: $!";
close($output_fh);
open(my $debug_fh, ">", "$output_dir/rails_db.dbg")
  || die "Can't open > rails_db.dbg: $!";

# Stuff for GeoDirectory

# Open the example file to get the headings
open(my $geo_dir_input_fh, "<", "GeoDirectory-example.csv")
  || die "Can't open < GeoDirectory-example.csv: $!";
chomp(my $geo_dir_header_line = readline $geo_dir_input_fh);
my @geo_dir_fields = split(/,/, $geo_dir_header_line);
close($geo_dir_input_fh);

# Define the conversion from rails to GeoDirectory
my %rails2geodirectory = {
  ID => "",
  post_title => "name",
  post_author => 1,
  post_content => "description",
  post_category => "",
  post_tags => "",
  post_type => "gd_place",
  post_status => "publish",
  featured => 1,
  video => "",
  street => "address",
  city => "",
  region => "",
  country => "",
  zip => "",
  latitude => "latitude",
  longitude => "longitude",
  business_hours => "hours",
  phone => "",
  email => "",
  website => "url",
  twitter => "",
  facebook => "",
  post_images => ""
};

# Open the GeoDirectory output file for writing
open(my $geo_dir_output_fh, ">", "GeoDirectory-output.csv")
  || die "Can't open < GeoDirectory-output.csv: $!";
print $geo_dir_output_fh join(",", @geo_dir_fields)."\n";

########### End stuff for GeoDirectory

# Bad idea again, global variable to tell me which table
my $curr_table = "";

# Main input loop, go until the end of the file
while(! eof($input_fh)) {
  # Grab the next line of the input file
  my $input_line = get_next_line($input_fh);

  # If it is a copy command, start parsing
  if ($input_line =~ "COPY") {
    print $output_fh parse_copy_cmd($input_line);

    # Parse all rows associated with this table
    for (my $row=get_next_line($input_fh); $row ne '\.'; $row=get_next_line($input_fh)) {
      print $output_fh parse_table_row($row);
    }

    # Again, really bad idea to close the fh here since it was opened in a subroutine
    close($output_fh);
  }
}

# Gets the next line from the provided file handle
sub get_next_line {
  my $fh = pop;
  chomp(my $ret_line = readline $fh);
  return $ret_line;
}

# Gets the table name and field names from the copy command
sub parse_copy_cmd {
  my $line = pop;
  my $ret_string;
  if ($line =~ /^COPY (\w+) \((.*)\) FROM stdin;$/) {
    # Love global variable
    $curr_table = $1;
    # this is a really bad idea, but open the output file here and name it after the table
    open($output_fh, ">", "$output_dir/${curr_table}_table.csv")
      || die "Can't open > ${curr_table}_table.csv: $!";
    # $ret_string = "Saw table,${curr_table}\n";
    $ret_string = "\"".join("\",\"", split(/, /, $2))."\"\n";
  }
  return $ret_string;
}

# Gets the table field values from the provided row
sub parse_table_row {
  my $line = pop;

  # Yet another bad idea
  # Print the GeoDirectory csv here if it is the businesses table
  if($curr_table eq "businesses") {
    my @fields = split(/\t/, $line);
    print $geo_dir_output_fh ",$fields[3],1,$fields[8],,,gd_place,publish,1,,$fields[7],,,,,$fields[1],$fields[2],$fields[9],,,$fields[10],,,\n";
  }

  return "\"".join("\",\"", split(/\t/, $line))."\"\n";
}