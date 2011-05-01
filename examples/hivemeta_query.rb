#!/usr/bin/env ruby

require 'hivemeta'
require 'getoptlong'

db_user    = 'hive'
db_pass    = 'hivepasshere'
db_host    = 'localhost'
db_name    = 'hivemeta'

def usage
  puts <<-EOF
usage: #$0 [options] table_name|hdfs_path
  -h, --help
  -c, --comments       # display comments along with field detail (default)
  -C, --no-comments    # do not display comments with the field detail
  -l, --list-tables    # list matching tables but no detail
  -f, --list-file-path # list the table HDFS file locations
  -w, --fit-width      # fit the text to the width of the screen (default)
  -W, --no-fit-width   # do not fit the text to the width of the screen
  -u, --db-user=arg    # hive metastore db user (requires read access)
  -p, --db-pass=arg    # hive metastore db password
  -H, --db-host=arg    # host running the hive meta db (default: localhost)
  -d, --db-name=arg    # hive meta db name (default: hivemeta)
EOF
end   

# main

opts = GetoptLong.new(
  [ '--comments', '-c', GetoptLong::NO_ARGUMENT ],
  [ '--no-comments', '-C', GetoptLong::NO_ARGUMENT ],
  [ '--list-tables', '-l', GetoptLong::NO_ARGUMENT ],
  [ '--list-file-path', '-f', GetoptLong::NO_ARGUMENT ],
  [ '--fit-width', '-w', GetoptLong::NO_ARGUMENT ],
  [ '--no-fit-width', '-W', GetoptLong::NO_ARGUMENT ],
  [ '--db-user', '-u', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--db-pass', '-p', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--db-name', '-d', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--db-host', '-H', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--help', '-h', GetoptLong::NO_ARGUMENT ]
)

show_comments = true
list_tables   = false
list_paths    = false
fit_width     = true
opts.each do |opt, arg|
  case opt
    when '--comments' 
      show_comments = true
    when '--no-comments'
      show_comments = false
    when '--list-tables'
      list_tables = true
    when '--list-file-path'
      list_paths = true
    when '--fit-width'
      fit_width = true
    when '--no-fit-width'
      fit_width = false
    when '--db-host'
      db_host = arg
    when '--db-user'
      db_user = arg
    when '--db-pass'
      db_pass = arg
    when '--db-name'
      db_name = arg
    when '--help'
      usage
      exit
  end end

dbi_string = "DBI:Mysql:#{db_name}:#{db_host}"
h = HiveMeta::Connection.new(dbi_string, db_user, db_pass)

tables = []
max_col_width = 8

ARGV.each do |arg|
  if arg =~ %r|/|
    h.tables(filter_path: arg).each {|t| tables << t}
  else
    h.tables(filter_name: arg).each {|t| tables << t}
  end
end

tables.uniq.sort.each do |table|
  table.each_col do |col_name|
    max_col_width = col_name.size if col_name.size > max_col_width
  end
end

first_table = true
tables.each do |table|
  puts if not first_table and not list_tables
  puts table
  first_table = false
  next if list_tables
  puts table.path
  next if list_paths
  tput_cols = `tput cols`.chomp.to_i rescue tput_cols = 0

  table.each_with_index do |col_name, i|
    print "%-3d %-#{max_col_width}s" % [i, col_name]
    if show_comments and table.comments[i]
      if fit_width and tput_cols > 0
        width = tput_cols - 3 - 1 - max_col_width - 1
        width = 0 if width < 0
        print "%-#{width}.#{width}s" % " \# #{table.comments[i]}"
      else
        print " \# #{table.comments[i]}"
      end
    end
    puts
  end
end
