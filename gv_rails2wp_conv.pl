#!/usr/bin/perl

# Open input and output files (hard coded for now)
open(my $input_fh, "<", "rails_db.sql")
  || die "Can't open < rails_db.sql: $!";
open(my $output_fh, ">", "new_wordpress_db.sql")
  || die "Can't open > new_wordpress_db.sql: $!";
open(my $debug_fh, ">", "new_wordpress_db.dbg")
  || die "Can't open > new_wordpress_db.dbg: $!";

# Data structures defining how the input file gets converted into the output file

# Rails data structure
#   Table name
%RAILS_DATA_STRUCTURE = (
  # Categories for the businesses
  "business_types" => {},
  # Links businesses with their types
  "business_types_businesses" => {},
  # Businesses
  "businesses" => {},
  # Blog categories (not needed)
  # "category_labels" => {},
  # Comments on businesses and volunteer opportunities
  "comments" => {
    "FILTER" => {
      "AND_NOT" => {
        # Filter out the comments marked as spam
        "status" => "spam"
      }
    }
  },
  # Looks like a generic website comment db (not needed)
  # "contact_messages" => {},
  # Labels used by the volunteer opportunities (I think)
  "cost_labels" => {},
  # Durations used by volunteer opportunities
  "durations" => {},
  # Links a volunteer opportunity with its duration
  "durations_volunteer_opportunities" => {},
  # Categories for the images (not needed)
  # "image_categories" => {},
  # Crops for the images (not needed)
  # "image_crops" => {},
  # Caption and filename for the images (not needed)
  # "images" => {},
  # Comments created by the ambassadors, both are by userid 1 (not needed)
  # "internal_comments" => {},
  # Used to build the location tree for searching
  "locations" => {},
  # Links a location with a business or volunteer opportunity
  "locationships" => {},
  # Links a business and volunteer opportunity together
  "organization_pairings" => {},
  # Links a phone number with a business or volunteer opportunity
  "phone_numbers" => {},
  # A rails only construct (not needed)
  # "schema_migrations" => {},
  # Creates a slideshow (not needed)
  # "slideshow_images" => {},
  # Creates a slideshow (not needed)
  # "slideshows" => {},
  # Users (not needed, but should make sure all are in WP)
  # "users" => {},
  # Volunteer opportunities
  "volunteer_opportunities" => {},
  # Links volunteer opportunities with their types
  "volunteer_opportunities_volunteer_types" => {},
  # Types of volunteer opportunities
  "volunteer_types" => {},
  # Links rails part with wp part (not needed)
  # "wordpress_data" => {}
);

# Data structure holding the entire input rails database
my %rails_tables;
my @rails_table_names;

# Main input loop, go until the end of the file
while(! eof($input_fh)) {
  # Grab the next line of the input file
  my $input_line = get_next_line($input_fh);

  # If it is a copy command, start parsing
  if ($input_line =~ "COPY") {
    my $curr_table = parse_copy_cmd($input_line);
    # Parse all rows associated with this table
    # But only if it is one of the tables we care about (in %RAILS_DATA_STRUCTURE)
    if (defined $RAILS_DATA_STRUCTURE{$curr_table->{"table_name"}}) {
      for (my $row=get_next_line($input_fh); $row ne '\.'; $row=get_next_line($input_fh)) {
        parse_table_row($curr_table, $row);
      }
    }

    # Add this table to the master rails data structure
    push @rails_table_names, $curr_table->{"table_name"};
    $rails_tables{$curr_table->{"table_name"}}{"field_names"} = $curr_table->{"field_names"};
    $rails_tables{$curr_table->{"table_name"}}{"rows"} = $curr_table->{"rows"};
  }
}

print_debug();

# Print out the data structure
sub print_debug {
  foreach (@rails_table_names) {
    my $table_name = $_;
    print $debug_fh "Saw rails table $table_name with rows:\n";
    my $row_num = 0;
    foreach (@{$rails_tables{$table_name}{"rows"}}) {
      my $row = $_;
      print $debug_fh "  Row $row_num:\n"; $row_num++;
      foreach (@{$rails_tables{$table_name}{"field_names"}}) {
        print $debug_fh "    $_ => ${$row}{$_}\n";
      }
    }
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
  my %ret_table;
  if ($line =~ /^COPY (\w+) \((.*)\) FROM stdin;$/) {
    $ret_table{"table_name"} = $1;
    @{$ret_table{"field_names"}} = split /, /, $2;
  }
  return \%ret_table;
}

# Gets the table field values from the provided row
sub parse_table_row {
  my $line = pop;
  my $ret_table = pop;
  my @row_values = split /\t/, $line;
  my %ret_fields;
  for ($i=0; $i<@{$ret_table->{"field_names"}}; $i++) {
    # Sanitize the field values to escape any single quotes
    $ret_fields{${$ret_table->{"field_names"}}[$i]} = $row_values[$i] =~ s/'/\\'/r;
  }
  # Push the row onto the data structure and return
  # But first check to see if there is a filter on this table
  push(@{$ret_table->{"rows"}}, \%ret_fields) if passed_filter($ret_table->{"table_name"}, \%ret_fields);
  return $ret_table;
}

# Returns true if the row passes the filter criteria
# FIXME: Only implimented to handle the AND_NOT filter so far
sub passed_filter {
  my $fields = pop;
  my $table_name = pop;

  # If there is a filter
  if (defined $RAILS_DATA_STRUCTURE{$table_name}{"FILTER"}) {
    # If any of the AND_NOT criteria are present, return false
    foreach (keys %{$RAILS_DATA_STRUCTURE{$table_name}{"FILTER"}{"AND_NOT"}}) {
      if ($fields->{$_} eq $RAILS_DATA_STRUCTURE{$table_name}{"FILTER"}{"AND_NOT"}{$_}) {
        return 0; # FALSE
      }
    }
  }

  # If we make it this far, return true
  return 1; # TRUE
}