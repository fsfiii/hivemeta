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
  -u, --db-user=arg    # hive metastore db user (requires read access)
  -p, --db-pass=arg    # hive metastore db password
  -H, --db-host=arg    # host running the hive meta db (default: localhost)
  -d, --db-name=arg    # hive meta db name (default: hivemeta)
EOF
end   

# main

opts = GetoptLong.new(
  [ '--db-user', '-u', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--db-pass', '-p', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--db-name', '-d', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--db-host', '-H', GetoptLong::REQUIRED_ARGUMENT ],
  [ '--help', '-h', GetoptLong::NO_ARGUMENT ]
)

opts.each do |opt, arg|
  case opt
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

h = HiveMeta::Connection.new(db_name, db_host, db_user, db_pass)

# test table has the following schema
# i   col_name
# 0   foo     
# 1   bar

test_table_name = 'testhive'

test_table = h.table test_table_name

begin
  test_data = "data0\tdata1"
  row = test_table.process_row test_data
  p row
  puts "access via method (best): #{row.foo} | #{row.bar}"
  puts "access via symbol lookup: #{row[:foo]} | #{row[:bar]}"
  puts "access via string lookup: #{row['foo']} | #{row['bar']}"

  # this will bomb
  test_data = "data0\tdata1\tdata2"
  row = test_table.process_row test_data
  p row
rescue HiveMeta::FieldCountError => e
  puts e
  puts "bad data: #{test_data}"
end
