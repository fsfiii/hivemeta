require 'dbi'
require 'hivemeta/table'
require 'hivemeta/record'

module HiveMeta

  class Connection
    def initialize(dbi_string = nil, db_user = nil, db_pass = nil)
      db_name = ENV['hivemeta_db_name']
      db_host = ENV['hivemeta_db_host']
      dbi_string ||= "DBI:Mysql:#{db_name}:#{db_host}"
      db_user    ||= ENV['hivemeta_db_user']
      db_pass    ||= ENV['hivemeta_db_pass']

      @dbi_string = dbi_string
      @db_user    = db_user
      @db_pass    = db_pass
    end

    def query(sql, *args)
      dbh = results = nil

      # make a few attempts in the event that mysql has not been
      # configured with enough connections to handle many mappers
      attempts, max_attempts = 0, 3
      begin
        dbh = DBI.connect(@dbi_string, @db_user, @db_pass)
      rescue DBI::DatabaseError => e
        attempts += 1
        if attempts < max_attempts
          s = rand + 0.50
          STDERR.puts "retrying hivemeta connection after %f seconds..." % s
          sleep s
          retry
        else
          warn "cannot connect to metastore %s:\n  error %s\n  %s" %
            [@dbi_string, e.err, e.errstr]
          raise
        end
      end

      sth = dbh.prepare(sql)
      sth.execute(*args)
      if block_given?
        sth.fetch {|row| yield row}
      else
        results = []
        sth.fetch {|row| results << row.dup}
      end
      sth.finish

      dbh.disconnect

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
