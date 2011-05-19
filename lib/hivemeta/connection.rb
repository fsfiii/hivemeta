require 'dbi'
require 'hivemeta/table'
require 'hivemeta/record'

module HiveMeta

  class Connection
    def initialize(dbi_string = nil, db_user = nil, db_pass = nil)
      @dbi_string = dbi_string
      @db_user    = db_user
      @db_pass    = db_pass

      begin
        @dbh = DBI.connect(dbi_string, db_user, db_pass)
      rescue DBI::DatabaseError => e
        STDERR.puts "cannot connect to metastore %s:\n  error (%s) %s" %
          [dbi_string, e.err, e.errstr]
        raise
      end
    end

    def query(sql, *args)
      results = nil

#puts "sql: #{sql}"
#puts "args: #{args}"
      sth = @dbh.prepare(sql)
      sth.execute(*args)
      if block_given?
        sth.fetch {|row| yield row}
      else
        results = []
        sth.fetch {|row| results << row.dup}
      end
      sth.finish

      results # returns nil if a block is given
    end

    def tables(opts = {})
      args = nil
      if opts[:filter_path]
        sql = "select t.TBL_NAME from TBLS t, SDS s
          where t.SD_ID = s.SD_ID
          and s.LOCATION like ?"
        args = "%#{opts[:filter_path]}%"
      elsif opts[:filter_name]
        sql = "select TBL_NAME from TBLS
          where TBL_NAME like ?"
        args = opts[:filter_name]
      else
        sql = "select TBL_NAME from TBLS"
      end

      results = query sql, *args
      table_names = results.map {|result| result[0]}
      
#puts "TABLE_NAMES:"
#p table_names

      tables = []
      table_names.each do |name|
#puts "NAME: "
#p name
        table = Table.new(name)

        sql = "select c.INTEGER_IDX, c.column_name, c.COMMENT,
          s.LOCATION, s.SD_ID
          from TBLS t, COLUMNS c, SDS s
          where t.SD_ID = c.SD_ID and t.SD_ID = s.SD_ID
          and t.TBL_NAME = ?"
        query sql, name do |rec|
#puts "REC:"
#p rec
          col_idx  = rec[0].to_i
          col_name = rec[1]
          col_cmt  = rec[2]
          tbl_loc  = rec[3]
          sd_id    = rec[4]
          table.columns[col_idx]         = col_name
          table.indexes[col_name.to_sym] = col_idx
          table.comments[col_idx]        = col_cmt
          table.path      = tbl_loc
        end

        sql = "select sp.PARAM_VALUE
          from SERDE_PARAMS sp, TBLS t
          where t.SD_ID = sp.SERDE_ID
          and PARAM_KEY = 'field.delim'
        and t.TBL_NAME = ?"
        results = query sql, name
        if results and results[0] and results[0][0]
          table.delimiter = results[0][0]
        end
#puts "#{name}: found delim '#{table.delimiter}'" if results[0]
#puts "#{name}: no delim" if not results[0]

        tables << table
      end
      tables
    end

    def table(name)
      t = tables(:filter_name => name) # appeasing the old skool 1.8 users
      t[0] # if it comes back with multiple tables, return the first
    end
  end

end

# fix for broken row dup in 1.9
# http://rubyforge.org/tracker/index.php?func=detail&aid=28624&group_id=234&atid=967
module DBI
  class Row
    if RUBY_VERSION =~ /^1\.9/
      def dup
        row = super
        row.instance_variable_set :@arr, @arr.dup
        row
      end
    end
  end
end
