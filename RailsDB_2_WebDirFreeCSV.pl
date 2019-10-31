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
       ($table_name eq "locationships")) {
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
print "RailsDB has ".scalar(keys(%RailsDB))." tables\n";
foreach $t_name (keys(%RailsDB)) {
  print "\tTable $t_name has ".scalar(keys(%{$RailsDB{$t_name}}))." rows\n";
  my @t_ids = keys(%{$RailsDB{$t_name}});
  my $t_id = $t_ids[0];
  print "\t\tRow with id $t_id has headers: ".join(", ", keys(%{$RailsDB{$t_name}{$t_id}}))."\n";
}

# Close the input file
close($input_fh);

# Print the final import file
print_WebDirFree_import();

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
    # Sanitize the data
    #  If the value contains a comma(,) or double quote(")
    #  the entire value must be quoted after escaping the double quote(\")
    for (my $i=0; $i<=$#values; $i++) {
      my $modval = $values[$i];
      if($modval =~ /[,"]/) {
        $modval =~ s/"/\\"/g;
        $values[$i] = "\"$modval\"";
      }
    }
    push @$ret_rows, \@values;
  }

  return $ret_rows;
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
    # Trying to debug a problem, only store the businesses entry for "Lila Thai Massage"
    # if(($name eq "businesses") and ($$row[3] ne "Lila Thai Massage")) {
    #   next
    # }

    # Populate the data structure (got the hash mapping from https://www.perlmonks.org/?node_id=4402)
    my %hashed_row;
    @hashed_row{@$header} = @$row;
    # I think this is only needed for the business_types_businesses table
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
sub print_WebDirFree_import {
  # Open the csv file for writing
  open(my $WDFimport_fh, ">", "WebDirFree_import.csv")
    || die "Can't open > WebDirFree_import.csv: $!";

  # CSV structure for Web Directory Free import file
  # Removing the following fields: contact_email, expiration_date, price, hours, summary, youtube, email
  # Adding the following fields: latitude longitude alt_phone alt2_phone
  # Renaming: company_name to company_name_title
  #           address to address_1
  # my @WDF_header = qw(company_name level_ID contact_email
  #                     expiration_date locations address
  #                     zip phone email web price categories
  #                     summary description youtube hours);
  my @WDF_header = qw(company_name_title level_ID
                      locations address_1
                      zip phone alt_phone alt2_phone web categories
                      description
                      latitude longitude);

  # Existing fields during the collate phase of the import (and the associated csv column)
  #  Title              => company_name_title (required)
  #  Level ID           => level_id           (required)
  #  Directory ID       =>                    (keeping blank to create a new listing, necessary to update existing)
  #  Author             =>                    (keeping blank, adding via dropdown)
  #  Status             =>                    (active, expired, or unpaid?)
  #  Categories         => categories         (will add new category(ies) if doesn't already exist)
  #  Tags               =>                    (will add new tag(s) if doesn't already exist)
  #  Description        => description
  #  Summary            => summary            (content field type Excerpt)
  #  Locations          => locations          (will add new location(s) if doesn't already exist)
  #  Address line 1     => address_1      
  #  Address line 2     =>
  #  Zip code           => zip
  #  Latitude           => latitude
  #  Longitude          => longitude
  #  Map icon file      =>
  #  Images files       =>
  #  YouTube or ...     => youtube
  #  Listing expiration =>
  #  Listing contact    =>
  #  Make claimable     =>
  #  Phone              => phone
  #  Alt Phone          => alt_phone
  #  Alt2 Phone         => alt2_phone
  #  Website            => web
  #  Email              => email


  # Print the header
  print $WDFimport_fh join(",", @WDF_header)."\n";

  # Iterate across the entries in the "businesses" table and print them to the output csv
  foreach $business_id (keys(%{$RailsDB{businesses}})) {
    my @output_row = (
      $RailsDB{businesses}{$business_id}{name},        # company_name_title
      1,                                               # level_ID        (HARDCODED)
      #"",                                              # contact_email   (EMPTY: Can't be empty)
      #"",                                              # expiration_date (EMPTY: Can't be empty)
      get_locations($business_id),                     # locations
      get_address($business_id),                       # address_1       (TODO: Parse into other fields)
      "",                                              # zip             (TODO: Parse from other fields)
      get_phone_numbers($business_id),                 # phone, alt_phone, alt2_phone (returns all three)
      #"",                                              # email           (EMPTY?)
      $RailsDB{businesses}{$business_id}{url},         # web
      #"",                                              # price           (EMPTY: Can remove)
      get_business_types($business_id),                # categories
      #"",                                              # summary         (TODO: Do I need this?)
      get_description($business_id),                   # description     (TODO: Parse into summary if needed)
      #"",                                              # youtube         (EMPTY)
      #$RailsDB{businesses}{$business_id}{hours},       # hours          (TODO: Need to add to fields, I think)
      $RailsDB{businesses}{$business_id}{latitude},    # latitude
      $RailsDB{businesses}{$business_id}{longitude}    # longitude
    );
    print $WDFimport_fh join(",", @output_row)."\n";
  }

  # Close the csv file
  close($WDFimport_fh);
}

sub get_business_types {
  my $business_id = pop;
  my @ret_types;
  # Iterate over the business_types_businesses table and associate a business with its business_type(s)
  foreach $table_id (keys(%{$RailsDB{business_types_businesses}})) {
    if($RailsDB{business_types_businesses}{$table_id}{business_id} == $business_id) {
      my $business = $RailsDB{businesses}{$business_id}{name};
      my $business_type_id = $RailsDB{business_types_businesses}{$table_id}{business_type_id};
      my $business_type = $RailsDB{business_types}{$business_type_id}{name};
      # print "In business_types_businesses{$table_id} found business{$business_id} ($business) matching business_types{$business_type_id} ($business_type)\n";
      push @ret_types, ($business_type);
    }
  }

  # If there are multiple types associated with this business, make sure it has the correct separator (;)
  if(scalar(@ret_types) > 1) {
    return join(";", @ret_types);
  } else {
    return $ret_types[0];
  }
}

sub get_phone_numbers {
  my $business_id = pop;
  my @ret_phone_numbers;
  # Iterate over the phone_numbers table and associate a business with its phone number(s)
  foreach $table_id (keys(%{$RailsDB{phone_numbers}})) {
    if(($RailsDB{phone_numbers}{$table_id}{phone_numberable_id} == $business_id) and
       ($RailsDB{phone_numbers}{$table_id}{phone_numberable_type} eq "Business")) {
      my $business = $RailsDB{businesses}{$business_id}{name};
      my $phone_number = $RailsDB{phone_numbers}{$table_id}{number};
      # print "In phone_numbers{$table_id} found business{$business_id} ($business) with phone number ($phone_number)\n";
      push @ret_phone_numbers, ($phone_number);
    }
  }

  # The phone number field can't handle multiple phone numbers, so I added an alt/alt2_phone field
  # This subroutine returns both fields, so if there is only one number then it returns a blank field
  # The separator is a comma (,) as opposed to the semicolon used for other multiple value fields
  # Start with a sanity check to make sure there aren't more than three numbers
  if(scalar(@ret_phone_numbers) > 3) {
    die "There are more than three phone numbers for business_id $business_id\n";
  } elsif(scalar(@ret_phone_numbers == 3)) {
    return join(",", @ret_phone_numbers);
  } elsif(scalar(@ret_phone_numbers == 2)) {
    return join(",", @ret_phone_numbers).",";
  } else {
    return $ret_phone_numbers[0].",,";
  }
}

sub get_locations {
  my $business_id = pop;
  # Web Directory Free really only wants one location (the deepest one in the locations hierachy)
  # my @ret_locations;
  my $ret_location;
  my $deepest_location = -1; # Must be -1 to make the loop work
  # Iterate over the locationships table and associate a business with its locations(s)
  foreach $table_id (keys(%{$RailsDB{locationships}})) {
    if(($RailsDB{locationships}{$table_id}{locatable_id} == $business_id) and
       ($RailsDB{locationships}{$table_id}{locatable_type} eq "Business")) {
      my $business = $RailsDB{businesses}{$business_id}{name};
      my $location_id = $RailsDB{locationships}{$table_id}{location_id};
      my $location = $RailsDB{locations}{$location_id}{name};
      my $ancestry_depth = $RailsDB{locations}{$location_id}{ancestry_depth};
      my $ancestry = $RailsDB{locations}{$location_id}{ancestry};
      # parse the depth and ancestry to create the location string
      my $location_string;
      if ($ancestry_depth == 0) {
        $location_string = $location;
      } else {
        foreach $ancestor_id (split(/\//, $ancestry)) {
          $location_string .= $RailsDB{locations}{$ancestor_id}{name}." > ";
        }
        $location_string .= $location;
      }
      # print "In locationships{$table_id} found business{$business_id} ($business) with location{$location_id} ($location_string)\n";
      # Only update $ret_location if this is a deeper ancestor
      # push @ret_locations, ($location_string);
      # print "deepest location ($deepest_location) current depth ($ancestry_depth) ";
      if($ancestry_depth > $deepest_location) {
        $ret_location = $location_string;
        $deepest_location = $ancestry_depth;
        # print "Update the location\n";
      } else {
        # print "Not the deepest ancestor\n";
      }
    }
  }

  # If there are multiple locations associated with this business, make sure it has the correct separator (;)
  # Only print out one location (unless there is a need in the future)
  # if(scalar(@ret_locations) > 1) {
  #   return join(";", @ret_locations);
  # } else {
  #   return $ret_locations[0];
  # }
  return $ret_location;
}

sub get_address {
  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{businesses}{$business_id}{address};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If an address has a carriage return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  if($ret_string =~ /\\r\\n/) {
    if($ret_string !~ /^".*"$/) {
      # print "Had to do the double-quote diving save for business_id $business_id\n";
      $ret_string = "\"".$ret_string."\"";
    }
  }

  # Replace the Rails/SQL carriage return-newline (\r\n) with a comma and a space (, )
  $ret_string =~ s/\\r\\n/, /g;

  # Return the modified string
  return $ret_string;
}

# Convert the Rails/SQL formatted description into something usable by WordPress
sub get_description {
  # Start with the raw string from the rails sql file
  my $ret_string = $RailsDB{businesses}{$business_id}{description};

  # Not sure where else to do this right now cuz doing it in the sanitize section above broke other things
  # If a description has a carriage return and newline (\r\n), but doesn't already have quotes around it ("..."), add it now
  if($ret_string =~ /\\r\\n/) {
    if($ret_string !~ /^".*"$/) {
      # print "Had to do the double-quote diving save for business_id $business_id\n";
      $ret_string = "\"".$ret_string."\"";
    }
  }

  # Replace the Rails/SQL carriage return-newline (\r\n) with a single newline (\n\n)
  $ret_string =~ s/\\r\\n/\n/g;

  # Remove the Rails more tag
  $ret_string =~ s/<!--more-->//g;

  # TODO: Add more formatting conversions:
  #       => Bold -- Text surrounded by asterisks (*Some of the services on offer:*)
  #       => Unordered list -- Single newlines with space-asterisk for each item
  #                            ( * Thai Massage\r\n * Foot Massage\r\n * Traditional Thai Herbal Body Scrub\r\n * Herbal Facial  Scrub\r\n * Pedicure & Manicure)

  # Return the modified string
  return $ret_string;
}