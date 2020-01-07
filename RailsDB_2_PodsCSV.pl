#!/usr/bin/perl

# Open input file (hard coded for now)
open(my $input_fh, "<", "rails_db.sql")
  || die "Can't open < rails_db.sql: $!";

# Data structure holding the Rails DB (only the parts we need)
my %RailsDB;


# Main input loop, go unti the end of the file
while(! eof($input_fh)) {
  # Grab the next line of the input file
  my $line = get_next_line($input_fh);

  # If it is a COPY command, start parsing
  if ($line =~ "COPY") {
    my ($table_name, $table_header) = parse_copy_cmd($line);

    # Do different things depending on what table is being parsed
    if(($table_name eq "businesses") or
       ($table_name eq "business_types") or
       ($table_name eq "business_types_businesses") or
       ($table_name eq "phone_numbers") or
       ($table_name eq "locations") or
       ($table_name eq "locationships") or
       ($table_name eq "volunteer_opportunities") or
       ($table_name eq "volunteer_types") or
       ($table_name eq "volunteer_opportunities_volunteer_types")) {
      # Parse the rows
      my $table_rows = parse_table_rows($table_name, $input_fh);
      # Print the table csv
      print_table_csv($table_name, $table_header, $table_rows);
      # Add the table to the Rails DB data structure
      store_table_in_RailsDB($table_name, $table_header, $table_rows);
    }
  }
}

# Debug message to see the structure of %RailsDB
# print "RailsDB has ".scalar(keys(%RailsDB))." tables\n";
foreach $t_name (keys(%RailsDB)) {
  # print "\tTable $t_name has ".scalar(keys(%{$RailsDB{$t_name}}))." rows\n";
  my @t_ids = keys(%{$RailsDB{$t_name}});
  my $t_id = $t_ids[0];
  # print "\t\tRow with id $t_id has headers: ".join(", ", keys(%{$RailsDB{$t_name}{$t_id}}))."\n";
}

# Close the input file
close($input_fh);

# Print the final import file
print_Pods_import();

exit;

# Gets the next line from the provided file handle
sub get_next_line {
  my $fh = pop;
  chomp(my $ret_line = readline $fh);
  return $ret_line;
}

# Gets the table name and field names from the copy command
sub parse_copy_cmd {
  my $line = pop;
  my ($ret_table_name, $ret_table_header);
  if ($line =~ /^COPY (\w+) \((.*)\) FROM stdin;$/) {
    $ret_table_name = $1;
    @$ret_table_header = split(/, /, $2);
  }
  return ($ret_table_name, $ret_table_header);
}

# Gets the table field values from the provided row
sub parse_table_rows {
  my ($table_name, $fh) = @_;
  my $ret_rows;
  my $debug_max_rows = 0; # 0 means parse all of them
  # if ($table_name eq "volunteer_opportunities") {
  #   $debug_max_rows = 1;
  # }
  my $debug_rows_parsed = 0;

  printf("Parsing %s rows for table $table_name\n", $debug_max_rows ? $debug_max_rows : "all");

  # Parse the file handle until the end condition is met (in this case "\.")
  for (my $line=get_next_line($fh); $line ne '\.'; $line=get_next_line($fh)) {
    my @values = split(/\t/, $line);
    # Sanitize the text
    #  If the value contains a comma(,) or double quote(")
    #  the entire value must be quoted after escaping the double quote(\")
    foreach $val (@values) {
      # if (($table_name eq "volunteer_opportunities") && ($debug_rows_parsed < $debug_max_rows)) {
      #   print "Before sanitizing:\n";
      #   print "\t$val\n";
      # }
      $val = sanitize_text($val);
      # if (($table_name eq "volunteer_opportunities") && ($debug_rows_parsed < $debug_max_rows)) {
      #   print "After:\n";
      #   print "\t$val\n";
      # }
      # if ($val =~ /[,"]/) {
      #   $val =~ s/"/\\"/g;
      #   $val = "\"$val\"";
      # }
    }
    # for (my $i=0; $i<=$#values; $i++) {
    #   my $modval = $values[$i];
    #   if($modval =~ /[,"]/) {
    #     $modval =~ s/"/\\"/g;
    #     $values[$i] = "\"$modval\"";
    #   }
    # }

    # For debug purposes, only parse some of the rows
    if (!$debug_max_rows || ($debug_rows_parsed < $debug_max_rows)) {
      $debug_rows_parsed += 1;
      push @$ret_rows, \@values;
    }
  }

  return $ret_rows;
}

# The text coming from the rails DB needs to be sanitized to be legal for a CSV file
# sanitize_text(<dirty_text>)
#   dirty_text: text that isn't clean
sub sanitize_text {
  my ($dirty_text) = @_;
  my $clean_text = $dirty_text;

  # Pet peve... Strip off any errant spaces or return/newlines ("\r\n") from the beginning/end of the field text
  my $no_leading_trailing_whitespace_text = $clean_text;
  $no_leading_trailing_whitespace_text =~ s/^(\s|(\\r\\n))+//; # leading whitespace
  $no_leading_trailing_whitespace_text =~ s/(\s|(\\r\\n))+$//; # trailing whitespace
  $clean_text = $no_leading_trailing_whitespace_text;

  # Strip out the return/newline ("\r\n") and convert them to a newline (\n)
  my $no_cr_nl_text = $clean_text;
  $no_cr_nl_text =~ s/\\r\\n/\n/g;
  $clean_text = $no_cr_nl_text;


  # Any double-quotes must be escaped
  my $esc_double_quotes_text = $clean_text;
  $esc_double_quotes_text =~ s/"/\\"/g;
  $clean_text = $esc_double_quotes_text;

  # If there are commas, escaped double-quotes, or newlines, the entire string needs to be quoted
  if ($clean_text =~ /(,|(\\")|\n)/) {
    $clean_text = "\"$clean_text\"";
  }

  # Return the clean text
  return $clean_text;
}


# Print the table csv
sub print_table_csv {
  my ($name, $header, $rows) = @_;

  # Open the csv
  my $output_dir = "table_csvs";
  unless (-e $output_dir and -d $output_dir) {
    mkdir($output_dir) || die "Can't create directory: $output_dir";
  }
  open(my $csv_fh, ">", "$output_dir/${name}_table.csv")
    || die  "Can't open > ${name}_table.csv: $!";
  # Print the header
  print $csv_fh join(",", @$header)."\n";
  # Print the rows
  foreach $row (@$rows) {
    print $csv_fh join(",", @$row)."\n";
  }
  # Close the csv
  close($csv_fh);
}

# Add the table to the Rails DB data structure
sub store_table_in_RailsDB {
  my ($name, $header, $rows) = @_;

  # Iterate accross the rows
  my $id = 0;
  foreach $row (@$rows) {
    # Populate the data structure (got the hash mapping from https://www.perlmonks.org/?node_id=4402)
    my %hashed_row;
    @hashed_row{@$header} = @$row;
    # I think this is only needed for the business_types_businesses and
    # volunteer_opportunities_volunteer_types tables
    # If there is no "id" field, add one
    if(defined $hashed_row{id}) {
      $RailsDB{$name}{$hashed_row{id}} = \%hashed_row;
    } else {
      $RailsDB{$name}{$id} = \%hashed_row;
    }
    $id++;
  }
};

# Print the final import file
sub print_Pods_import {
  # Open the csv files for writing
  # open(my $PODSimport_fh, ">", "Pods_import.csv")
  #   || die "Can't open > Pods_import.csv: $!";
  open(my $PODS_businesses_fh, ">", "Pods_Businesses_import.csv")
    || die "Can't open > Pods_Businesses_import.csv: $!";
  open(my $PODS_vol_opps_fh, ">", "Pods_VolunteerOpportunities.csv")
    || die "Can't open > Pods_VolunteerOpportunities.csv: $!";
  open(my $PODS_phone_nums_fh, ">", "Pods_PhoneNumbers.csv")
    || die "Can't open > Pods_PhoneNumbers.csv: $!";

  # CSV structure for Pods busineses import file
  my @Pods_businesses_header = qw(company_name locations address
                                  phone_1 phone_type_1
                                  phone_2 phone_type_2
                                  phone_3 phone_type_3
                                  web categories description latitude longitude
                                  hours short_location);

  # Print the businesses header
  print $PODS_businesses_fh join(",", @Pods_businesses_header)."\n";

  # Iterate across the entries in the "businesses" table and print them to the output csv
  # Sort them so they always print out in the same order
  foreach $business_id (sort(keys(%{$RailsDB{businesses}}))) {
    my @output_row = (
      get_field("businesses", $business_id, "name"),              # company_name
      get_field("businesses", $business_id, "location"),          # locations
      get_field("businesses", $business_id, "address"),     # address
      get_field("businesses", $business_id, "phone_numbers"),     # phone_1/2/3 and phone_type_1/2/3 (returns all three)
      get_field("businesses", $business_id, "url"),               # web
      get_field("businesses", $business_id, "business_types"),    # categories
      get_field("businesses", $business_id, "description"), # description
      get_field("businesses", $business_id, "latitude"),          # latitude
      get_field("businesses", $business_id, "longitude"),         # longitude
      get_field("businesses", $business_id, "hours"),       # hours
      get_field("businesses", $business_id, "short_location")     # short_location
    );
    print $PODS_businesses_fh join(",", @output_row)."\n";
  }

  # CSV structure for Pods volunteer opportunities import file
  my @Pods_vol_opps_header = qw(vol_opp_name locations short_location
                                organization_url volunteer_url
                                facebook_url twitter_username
                                volunteer_types description
                                min_duration max_duration duration_notes
                                cost_suggestion fees_notes
                                other_ways_to_help contact_info);

  # Print the volunteer opportunities header
  print $PODS_vol_opps_fh join(",", @Pods_vol_opps_header)."\n";

  # Iterate across the entries in the "volunteer_opportunities" table and print them to the output csv
  # Sort them so they always print out in the same order
  foreach $vol_opp_id (sort(keys(%{$RailsDB{volunteer_opportunities}}))) {
    my @output_row = (
      get_field("volunteer_opportunities", $vol_opp_id, "name"),               # vol_opp_name
      get_field("volunteer_opportunities", $vol_opp_id, "location"),           # locations
      get_field("volunteer_opportunities", $vol_opp_id, "short_location"),     # short_location
      get_field("volunteer_opportunities", $vol_opp_id, "organization_url"),   # organization_url
      get_field("volunteer_opportunities", $vol_opp_id, "volunteer_url"),      # volunteer_url
      get_field("volunteer_opportunities", $vol_opp_id, "facebook_url"),       # facebook_url
      get_field("volunteer_opportunities", $vol_opp_id, "twitter_username"),   # twitter_username
      get_field("volunteer_opportunities", $vol_opp_id, "volunteer_types"),    # volunteer_types
      get_field("volunteer_opportunities", $vol_opp_id, "description"),        # description
      get_field("volunteer_opportunities", $vol_opp_id, "min_duration"),       # min_duration
      get_field("volunteer_opportunities", $vol_opp_id, "max_duration"),       # max_duration
      get_field("volunteer_opportunities", $vol_opp_id, "duration_notes"),     # duration_notes
      get_field("volunteer_opportunities", $vol_opp_id, "cost_suggestion"),    # cost_suggestion
      get_field("volunteer_opportunities", $vol_opp_id, "fees_notes"),         # fees_notes
      get_field("volunteer_opportunities", $vol_opp_id, "other_ways_to_help"), # other_ways_to_help
      get_field("volunteer_opportunities", $vol_opp_id, "contact_info")  # contact_info
    );
    print $PODS_vol_opps_fh join(",", @output_row)."\n";
  }

  # CSV structure for Pods phone numbers import file
  my @Pods_phone_nums_header = qw(phone_number phone_number_type associated_business);

  # Print the volunteer opportunities header
  print $PODS_phone_nums_fh join(",", @Pods_phone_nums_header)."\n";

  # Iterate across the entries in the "phone_numbers" table and print them to the output csv
  # Sort them so they always print out in the same order
  foreach $phone_num_id (sort(keys(%{$RailsDB{phone_numbers}}))) {
    my @output_row = (
      get_field("phone_numbers", $phone_num_id, "number"),               # phone_number
      get_field("phone_numbers", $phone_num_id, "description"),          # phone_number_type
      get_field("phone_numbers", $phone_num_id, "associated_business")   # associated_business
    );
    print $PODS_phone_nums_fh join(",", @output_row)."\n";
  }

  # Close the csv files
  close($PODS_businesses_fh);
  close($PODS_vol_opps_fh);
  close($PODS_phone_nums_fh);
}

###############################################
# Making the getter functions much more generic
###############################################

# get_record(<table>, <record_id>)
#   table:     name of table (ex: businesses, volunteer_opportunities)
#   record_id: record id within <table>
sub get_record {
  my ($table, $record_id) = @_;
  return \%{$RailsDB{$table}{$record_id}};
}

# get_field(<table>, <record_id>, <field>)
#   table:     name of table (ex: businesses, volunteer_opportunities)
#   record_id: record id within <table>
#   field:     field within <record_id>
#
sub get_field {
  my ($table, $record_id, $field) = @_;

  ###############
  # Special cases
  ###############

  # The "location" field requires parsing the locationships table
  # to find which records in the locations table to use to build the full location
  if ($field eq "location") {
    # The locationships table keys off of an id and locatable type ("Business" or "VolunteerOpportunity")
    my $locatable_type = ($table eq "businesses")              ? "Business" :
                         ($table eq "volunteer_opportunities") ? "VolunteerOpportunity" :
                         die "Table \"$table\" is not defined for field \"location\"\n";
    return build_location($locatable_type, $record_id);
  }

  # The "phone_numbers" field requires parsing the phone_numbers table
  if ($field eq "phone_numbers") {
    # Only used by businesses, no need to pass the $table into the function
    return get_phone_numbers($record_id);
  }

  # The "business_types" field requires parsing the business_types_businesses table
  # to return matching records in the business_types table
  if ($field eq "business_types") {
    # Only used by businesses, no need to pass the $table into the function
    return get_business_types($record_id);
  }

  # The "volunteer_types" field requires parsing the volunteer_opportunities_volunteer_types table
  # to return matching records in the volunteer_types table
  if ($field eq "volunteer_types") {
    # Only used by volunteer_opportunities, no need to pass the $table into the function
    return get_volunteer_types($record_id);
  }

  # Map the cost suggestion from a number of $'s (0-3) into the appropriate string (FREE, $, $$, or $$$)
  if ($field eq "num_dollar_signs") {
    # Only used by volunteer_opportunities, no need to pass the $table into the function
    return decode_cost_suggestion($record_id);
  }

  # The "associated_business" field used in the phone_numbers table associates a business name with the phone number
  if ($field eq "associated_business") {
    # Only used by phone_numbers, no need to pass the $table into the function
    return associate_business_to_phone_number($record_id);
  }

  ###################
  # End special cases
  ################### 

  # Fallback case, return the field directly out of the $RailsDB hash
  return $RailsDB{$table}{$record_id}{$field};
}

# Build the location string by following the breadcrumbs in the locations table
# build_locataion(<locatable_type>, <locatable_id>)
#   locatable_type: record type (allowed "Business" or "VolunteerOpportunity")
#   locatable_id:   record id
sub build_location {
  my ($locatable_type, $locatable_id) = @_;
  my @ret_locations;

  # Iterate over the locationships table
  foreach $locationships_id (sort(keys(%{$RailsDB{locationships}}))) {
    my $locationships_record = get_record("locationships", $locationships_id);

    # Does this record match what we're looking for?
    if (($$locationships_record{locatable_id} == $locatable_id) and
    ($$locationships_record{locatable_type} eq $locatable_type)) {

      # Get the associated record from the locations table
      # print "Building location with locations[".$$locationships_record{location_id}."]\n";
      my $locations_record = get_record("locations", $$locationships_record{location_id});

      # Build the full location tree from this endpoint
      if ($$locations_record{ancestry_depth} == 0) {
        # This is a root level location, nothing special to do here
        push @ret_locations, $$locations_record{name};
      } else {
        # The ancestry field provides the rest of the tree
        my @location_parts;
        foreach $ancestor_id (split("/", $$locations_record{ancestry})) {
          push @location_parts, get_field("locations", $ancestor_id, "name");
        }
        push @location_parts, $$locations_record{name};
        my $full_location = join(" > ", @location_parts);
        push @ret_locations, $full_location;
      }
    }
  }

  # The current list of locations includes all intermediate locations
  # For example: "asia", "asia > thailand", and "asia > thailand > chiang mai"
  # Reduce this so there are only unique trees
  my @reduced_locations;
  for ($loc_id=0;$loc_id<scalar(@ret_locations);$loc_id+=1) {
    my $unique_loc = 1; # Assume uniqueness unless proven otherwise
    for ($cmp_id=0;$cmp_id<scalar(@ret_locations);$cmp_id+=1) {
      # Dont compare a location against itself
      if ($loc_id == $cmp_id) {
        next;
      }

      # See if $loc_id is a substring of $cmp_id starting at index 0
      my $str_index = index($ret_locations[$cmp_id], $ret_locations[$loc_id]);
      if ($str_index == 0) {
        # Location at $loc_id is a substring of the one at $cmp_id, not unique
        $unique_loc = 0;
        # No need to keep looking
        last;
      }
    }
    # If still unique, add to the reduced array
    if ($unique_loc) {
      push @reduced_locations, $ret_locations[$loc_id];
    }
  }

  return join(";", sort(@reduced_locations));
}

# get_phone_numbers(<business_id>)
#   business_id: Record id in the businesses table
sub get_phone_numbers {
  my ($business_id) = @_;
  my @ret_phone_numbers;

  # Iterate over the phone_numbers table and associate a business with its phone number(s) and phone number description(s)
  foreach $phone_numbers_id (sort(keys(%{$RailsDB{phone_numbers}}))) {
    my $phone_numbers_record = get_record("phone_numbers", $phone_numbers_id);

    if(($$phone_numbers_record{phone_numberable_id} == $business_id) and
    ($$phone_numbers_record{phone_numberable_type} eq "Business")) {
      push @ret_phone_numbers, ($$phone_numbers_record{number}, $$phone_numbers_record{description});
    }
  }
  
  # Return a string of three sorted comma separated values
  # Make sure to fill in the array so there are three values
  my $fill_elms = 6 - scalar(@ret_phone_numbers);
  return join(",", (@ret_phone_numbers, ("")x$fill_elms));
}

# get_business_types(<business_id>)
#   business_id: Record id in the businesses table
sub get_business_types {
  my ($business_id) = @_;
  my @ret_types;

  # Iterate over the business_types_businesses table and associate a business with its business_type(s)
  foreach $b_t_b_id (keys(%{$RailsDB{business_types_businesses}})) {
    my $b_t_b_record = get_record("business_types_businesses", $b_t_b_id);
    if($$b_t_b_record{business_id} == $business_id) {
      push @ret_types, (get_field("business_types", $$b_t_b_record{business_type_id}, "name"));
    }
  }

  # Return a string of sorted semi-colon separated values
  return join(";", sort(@ret_types));
}

# get_volunteer_types(<vol_opp_id>)
#   vol_opp_id: Record id in the volunteer_opportunities table
sub get_volunteer_types {
  my ($vol_opp_id) = @_;
  my @ret_types;

  # Iterate over the volunteer_opportunities_volunteer_types table
  # and associate a volunteer_opportunity with its volunteer_type(s)
  foreach $vol_opp_vol_type_id (keys(%{$RailsDB{volunteer_opportunities_volunteer_types}})) {
    my $vol_opp_vol_type_record = get_record("volunteer_opportunities_volunteer_types", $vol_opp_vol_type_id);
    if($$vol_opp_vol_type_record{volunteer_opportunity_id} == $vol_opp_id) {
      push @ret_types, (get_field("volunteer_types", $$vol_opp_vol_type_record{volunteer_type_id}, "name"));
    }
  }

  # Return a string of sorted semi-colon separated values
  return join(";", sort(@ret_types));
}

# decode_cost_suggestion(<vol_opp_id>)
#   vol_opp_id: Record id in the volunteer_opportunities table
sub decode_cost_suggestion {
  my ($vol_opp_id) = @_;

  # The cost_suggestion field in the volunteer_opportunities table represents the number of $'s to print
  my $num_dollar_signs = get_field("volunteer_opportunities", $vol_opp_id, "cost_suggestion");
  
  # Special case, 0 dollar signs returns "FREE"
  if ($num_dollar_signs == 0) {
    return "FREE";
  }
  # Else, return a string of $'s
  return "\$" x $num_dollar_signs;
}

# associate_business_to_phone_number(<phone_num_id>)
#   phone_num_id: Record id in the phone_numbers table
sub associate_business_to_phone_number {
  my ($phone_num_id) = @_;

  # Sanity check, make sure the phone number is associated with a business
  if (get_field("phone_numbers", $phone_num_id, "phone_numberable_type") ne "Business") {
    die "Phone number id $phone_num_id is not associated with a Business\n";
  }

  # Return the name of the business assoicated with this phone number
  my $associated_business_id = get_field("phone_numbers", $phone_num_id, "phone_numberable_id");
  return get_field("businesses", $associated_business_id, "name");
}