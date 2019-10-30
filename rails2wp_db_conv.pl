#!/usr/bin/perl

# Open the files for reading/writing
open(my $input_fh, "<", "rails_db.sql")
  || die "Can't open < rails_db.sql: $!";
open(my $output_fh, ">", "wordpress_db.sql")
  || die "Can't open > wordpress_db.sql: $!";

# Describe the various data structures
# posts table
$WP_POSTS_TABLE_NAME = "gv_wp_1_0_posts";
# Field names without the id (it will be auto generated)
@WP_POSTS_FIELD_NAMES = qw( post_author post_date post_date_gmt post_content post_title
                            post_excerpt post_status comment_status ping_status post_password
                            post_name to_ping pinged post_modified post_modified_gmt post_content_filtered
                            post_parent guid menu_order post_type post_mime_type comment_count );
%WP_POSTS_DEFAULTS = (
  "post_author"           => 1,
  "post_date"             => "0000-00-00 00:00:00",
  "post_date_gmt"         => "0000-00-00 00:00:00",
  "post_content"          => "This was generated from a Rails $rails_table_name record",
  "post_title"            => 'Hello world!',
  "post_excerpt"          => '',
  "post_status"           => 'publish',
  "comment_status"        => 'open',
  "ping_status"           => 'open',
  "post_password"         => '',
  "post_name"             => 'hello-world',
  "to_ping"               => '',
  "pinged"                => '',
  "post_modified"         => "0000-00-00 00:00:00",
  "post_modified_gmt"     => "0000-00-00 00:00:00",
  "post_content_filtered" => '',
  "post_parent"           => 0,
  "guid"                  => '',
  "menu_order"            => 0,
  "post_type"             => 'post',
  "post_mime_type"        => '',
  "comment_count"         => 0
);
# Create the insert commands to be used later
$WP_POSTS_INSERT_HEADER = "INSERT INTO `$WP_POSTS_TABLE_NAME` (`".join("`, `", @WP_POSTS_FIELD_NAMES)."`) VALUES\n";
# postmeta table
$WP_POSTMETA_TABLE_NAME = "gv_wp_1_0_postmeta";
# Field names without the id (it will be auto generated)
@WP_POSTMETA_FIELD_NAMES = qw( post_id meta_key meta_value );
# Create the insert commands to be used later
$WP_POSTMETA_INSERT_HEADER = "INSERT INTO `$WP_POSTMETA_TABLE_NAME` (`".join("`, `", @WP_POSTMETA_FIELD_NAMES)."`) VALUES\n";

# Structure describing the rails to wordpress conversion
%RAILS_CONVERTER = (
  "businesses" => {
    "copy" => {
      "created_at"  => ["post_date", "post_date_gmt"],
      "name"        => ["post_title"],
      "slug"        => ["post_name"],
      "updated_at"  => ["post_modified", "post_modified_gmt"]
    },
  },
);

# Loop to read the rails file
while (! eof($input_fh)) {
  defined( $_ = readline $input_fh ) or die "readline failed: $!";
  chomp; # Avoid newline

  # The COPY command sets everything in motion
  if (/^COPY (\w+) \((.*)\) FROM stdin;$/) {
    my $table_name = $1;
    my @field_names = split /, /, $2;
    # print "Table $table_name has fields @field_names\n";
    my %rails_table = (
      table_name => $1,
      field_names => [split(/, /, $2)]
    );
    handle_rails_table(\%rails_table);
  }
}

sub handle_rails_table {
  my $rails_table = pop;

  # Handle the tables we're interested in
  if ($rails_table->{"table_name"} eq "businesses") {
    convert_rails_to_wp($rails_table);
  } else {
    print $output_fh "-- Dont know how to handle table ".$rails_table->{"table_name"}." yet\n\n";
  }
}

sub convert_rails_to_wp {
  my $rails_table = pop;
  my %wp_fields = %WP_POSTS_DEFAULTS;

  print $output_fh "-- Converting rails table ".$rails_table->{"table_name"}." into wordpress posts\n";

  # Loop until we run out of rows to insert (ending condition: a row equal to '\.')
  for (chomp(my $rails_row = readline $input_fh); $rails_row ne '\.'; chomp($rails_row = readline $input_fh)) {
    my %rails_fields;
    my @rails_field_values = split /\t/, $rails_row;
    for ($i=0; $i<@{$rails_table->{"field_names"}}; $i++) {
      # Sanitize the field values to escape any single quotes
      $rails_fields{${$rails_table->{"field_names"}}[$i]} = $rails_field_values[$i] =~ s/'/\\'/r;
    }

    # Iterate across the rails_fields
    foreach (sort keys %rails_fields) {
      # Some rails fields map directly into wordpress fields
      if ($_ eq "created_at") {
        $wp_fields{"post_date"}         = $rails_fields{"created_at"};
        $wp_fields{"post_date_gmt"}     = $rails_fields{"created_at"};
      } elsif ($_ eq "name") {
        $wp_fields{"post_title"}        = $rails_fields{"name"};
      } elsif ($_ eq "slug") {
        $wp_fields{"post_name"}         = $rails_fields{"slug"};
      } elsif ($_ eq "updated_at") {
        $wp_fields{"post_modified"}     = $rails_fields{"updated_at"};
        $wp_fields{"post_modified_gmt"} = $rails_fields{"updated_at"};
      } else {
        # Everything else gets dumped into postmeta records
        # Using the LAST_INSERT_ID() function to link it to the posts record being created
        push @postmeta_rows, "(LAST_INSERT_ID(), \'$_\', \'$rails_fields{$_}\')";
      }

      # Fix the broken post_content default
      $wp_fields{"post_content"} = "This was generated from a Rails ".$rails_table->{"table_name"}." record";

    }

    # New way of doing it by inserting one row at a time (so we can get the postmeta at the same time)
    print $output_fh $WP_POSTS_INSERT_HEADER."(\'".join("\', \'", @wp_fields{@WP_POSTS_FIELD_NAMES})."\');\n";

    # Add the postmeta inserts immediately after the posts insert
    print $output_fh $WP_POSTMETA_INSERT_HEADER.join(",\n", @postmeta_rows).";\n";
  }
}

