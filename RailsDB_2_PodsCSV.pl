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
      my $table_rows = parse_table_rows($input_fh);
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
  my $fh = pop;
  my $ret_rows;

  # Parse the file handle until the end condition is met (in this case "\.")
  for (my $line=get_next_line($fh); $line ne '\.'; $line=get_next_line($fh)) {
    my @values = split(/\t/, $line);
    # Sanitize the text
    #  If the value contains a comma(,) or double quote(")
    #  the entire value must be quoted after escaping the double quote(\")
    foreach $val (@values) {
      $val = sanitize_text($val);
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
    push @$ret_rows, \@values;
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
  if ($clean_text =~ /"/) {
    print "Field has a double quote in it:\n";
    print "$clean_text\n";
    print "Modified text is:\n$esc_double_quotes_text\n";
  }
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

  # CSV structure for Pods busineses import file
  my @Pods_businesses_header = qw(company_name locations address
                                  phone_1 phone_2 phone_3
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
      get_field("businesses", $business_id, "phone_numbers"),     # phone_1/2/3 (returns all three)
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
                                fee_category fee_notes
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
      get_field("volunteer_opportunities", $vol_opp_id, "description"),  # description
      get_field("volunteer_opportunities", $vol_opp_id, "min_duration"),       # min_duration
      get_field("volunteer_opportunities", $vol_opp_id, "max_duration"),       # max_duration
      get_field("volunteer_opportunities", $vol_opp_id, "duration_notes"),     # duration_notes
      get_field("volunteer_opportunities", $vol_opp_id, "num_dollar_signs"),   # cost_suggestion
      get_field("volunteer_opportunities", $vol_opp_id, "fees_notes"),         # fees_notes
      get_field("volunteer_opportunities", $vol_opp_id, "other_ways_to_help"), # other_ways_to_help
      get_field("volunteer_opportunities", $vol_opp_id, "contact_info")  # contact_info
    );
    print $PODS_vol_opps_fh join(",", @output_row)."\n";
  }

  # Close the csv files
  close($PODS_businesses_fh);
  close($PODS_vol_opps_fh);
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

  # Clean the description, address, hours, contact_info
  my $field_to_clean = $field;
  if ($field_to_clean =~ s/^clean_//) {
    return clean_text(get_field($table, $record_id, $field_to_clean));
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

  # Iterate over the phone_numbers table and associate a business with its phone number(s)
  foreach $phone_numbers_id (keys(%{$RailsDB{phone_numbers}})) {
    my $phone_numbers_record = get_record("phone_numbers", $phone_numbers_id);

    if(($$phone_numbers_record{phone_numberable_id} == $business_id) and
    ($$phone_numbers_record{phone_numberable_type} eq "Business")) {
      # TODO: Decide how to handle the (optional) description in Pods, not used right now
      push @ret_phone_numbers, ($$phone_numbers_record{number});
    }
  }
  
  # Return a string of three sorted comma separated values
  # Make sure to fill in the array so there are three values
  my $fill_elms = 3 - scalar(@ret_phone_numbers);
  return join(",", (sort(@ret_phone_numbers), ("")x$fill_elms));
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

# The text coming from the rails DB uses things that don't work so well for WordPress
#   "\r\n" => an actual newline (\n)
# clean_text(<dirty_text>)
#   dirty_text: text that isn't clean
sub clean_text {
  my ($dirty_text) = @_;
  my $clean_text = $dirty_text;

  # Pet peve... Strip off any errant spaces or return/newlines ("\r\n") from the beginning/end of the field text
  my $no_leading_trailing_whitespace_text = $clean_text;
  $no_leading_trailing_whitespace_text =~ s/^\s+//;       # leading whitespace
  $no_leading_trailing_whitespace_text =~ s/\s+$//;       # trailing whitespace
  $no_leading_trailing_whitespace_text =~ s/(\\r\\n)+$//; # trailing retun/newline
  $clean_text = $no_leading_trailing_whitespace_text;


  # If there are any return/newlines ("\r\n"), need to make sure there are quotes around the field text
  my $add_quotes_text = $clean_text;
  if ($add_quotes_text =~ /\\r\\n/) {
    if ($add_quotes_text !~ /^".*"$/) {
      $add_quotes_text = "\"$add_quotes_text\"";
    }
  }
  $clean_text = $add_quotes_text;

  # Strip out the return/newline ("\r\n") from the Rails DB and convert them to a newline (\n)
  my $no_cr_nl_text = $clean_text;
  $no_cr_nl_text =~ s/\\r\\n/\n/g;
  $clean_text = $no_cr_nl_text;

  # Return the clean text
  return $clean_text;
}

##########################################
# Old getter functions

# sub get_volunteer_types {
#   my $vol_opp_id = pop;
#   my @ret_types;
#   # Iterate over the volunteer_opportunities_volunteer_types table and associate a volunteer_opportunity with its volunteer_type(s)
#   foreach $table_id (keys(%{$RailsDB{volunteer_opportunities_volunteer_types}})) {
#     if($RailsDB{volunteer_opportunities_volunteer_types}{$table_id}{volunteer_opportunity_id} == $vol_opp_id) {
#       my $vol_opp = $RailsDB{volunteer_opportunities}{$vol_opp_id}{name};
#       my $volunteer_type_id = $RailsDB{volunteer_opportunities_volunteer_types}{$table_id}{volunteer_type_id};
#       my $volunteer_type = $RailsDB{volunteer_types}{$volunteer_type_id}{name};
#       # print "In volunteer_opportunities_volunteer_types{$table_id} found volunteer_opportunity{$vol_opp_id} ($vol_opp) matching volunteer_types{$volunteer_type_id} ($volunteer_type)\n";
#       push @ret_types, ($volunteer_type);
#     }
#   }

#   # If there are multiple types associated with this business, make sure it has the correct separator (;)
#   if(scalar(@ret_types) > 1) {
#     return join(";", @ret_types);
#   } else {
#     return $ret_types[0];
#   }
# }


sub get_address {
  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{businesses}{$business_id}{address};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a comma and a space (, )
  # $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;
}

# Convert the Rails/SQL formatted description into something usable by WordPress
sub get_description {
  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{businesses}{$business_id}{description};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If a description has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a single newline (\n\n)
  # $ret_string =~ s/\\r\\n/\n/g;

  # Remove the Rails more tag
  #$ret_string =~ s/<!--more-->//g;

  # TODO: Add more formatting conversions:
  #       => Bold -- Text surrounded by asterisks (*Some of the services on offer:*)
  #       => Unordered list -- Single newlines with space-asterisk for each item
  #                            ( * Thai Massage\r\n * Foot Massage\r\n * Traditional Thai Herbal Body Scrub\r\n * Herbal Facial  Scrub\r\n * Pedicure & Manicure)

  # Return the modified string
  return $ret_string;
}

# This could be a tricky one since it has very specific formatting according to the import page:
#   Opening hours content field import format: "Mon 01:00 AM - 12:30 PM", only 30 minutes range allowed.
#   Days of week: Mon, Tue, Wed, Thu, Fri, Sat, Sun. Separate opening hours of each day of week by comma.
#   Missing days of week will be set as "closed".
#
# It looks like blank is ok (nothing will show in the listing)
sub get_hours {
  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{businesses}{$business_id}{hours};

  # Return the modified string
  return $ret_string;
}

# Volunteer opportunity fields that are potentially multi-line
#   Right now, each subroutine just returns the string.
#   Eventually, they should fix the fomatting (convert "\r\n" to "\n", etc)
sub get_duration_notes {
  # Volunteer Opportunity ID
  my $vol_opp_id = pop;

  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{volunteer_opportunities}{$vol_opp_id}{duration_notes};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a comma and a space (, )
  # $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;
}

sub get_fee_category {
  # Volunteer Opportunity ID
  my $vol_opp_id = pop;

  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{volunteer_opportunities}{$vol_opp_id}{cost_suggestion};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a comma and a space (, )
  # $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;
}

sub get_fee_notes {
  # Volunteer Opportunity ID
  my $vol_opp_id = pop;

  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{volunteer_opportunities}{$vol_opp_id}{fees_notes};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a comma and a space (, )
  # $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;  
}

sub get_other_ways_to_help {
  # Volunteer Opportunity ID
  my $vol_opp_id = pop;

  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{volunteer_opportunities}{$vol_opp_id}{other_ways_to_help};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a comma and a space (, )
  # $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;  
}

sub get_contact_info {
  # Volunteer Opportunity ID
  my $vol_opp_id = pop;

  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{volunteer_opportunities}{$vol_opp_id}{contact_info};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  # if($ret_string =~ /\\r\\n/) {
  #   if($ret_string !~ /^".*"$/) {
  #     # print "Had to do the double-quote diving save for business_id $business_id\n";
  #     $ret_string = "\"".$ret_string."\"";
  #   }
  # }

  # Replace the Rails/SQL return-newline (\r\n) with a comma and a space (, )
  # $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;  
}