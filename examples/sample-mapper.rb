# a sample streaming mapper
#   - reads a fictitous sample_inventory table that has a number of
#     fields, one of which is item_id and another is inv_cnt
#   - outputs the inventory count for all items that have 1000 or more

require 'hivemeta'

db_user    = 'hive'
db_pass    = 'hivepasshere'
db_host    = 'localhost'
db_name    = 'hivemeta'

dbi_string = "DBI:Mysql:#{db_name}:#{db_host}"
h = HiveMeta::Connection.new(dbi_string, db_user, db_pass)

inventory = h.table 'sample_inventory'

STDIN.each_line do |line|
  begin
    row = inv_table.process_row line
  rescue HiveMeta::FieldCountError
    STDERR.puts "reporter:counter:bad_data:row_size,1"
    next
  end
  item_id = row.item_id # can access by method or [:sym] or ['str']
  count   = row.inv_cnt.to_i
  puts "#{item_id}\t#{count}" if count >= 1000
end
